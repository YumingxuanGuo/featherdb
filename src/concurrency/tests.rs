#![cfg(test)]
use tempfile::{tempdir, TempDir};

use super::*;
use crate::storage::kv::LsmStorage;
use crate::error::{Result, Error};

fn setup() -> Result<(MVCC, TempDir)> {
    let dir = tempdir()?;
    Ok((MVCC::new(Box::new(LsmStorage::open(&dir)?)), dir))
}

#[test]
fn test_begin() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    assert_eq!(1, txn.id());
    assert_eq!(Mode::ReadWrite, txn.mode());
    txn.commit()?;

    let txn = mvcc.begin()?;
    assert_eq!(2, txn.id());
    txn.rollback()?;

    let txn = mvcc.begin()?;
    assert_eq!(3, txn.id());
    txn.commit()?;

    Ok(())
}

#[test]
fn test_begin_with_mode_readonly() -> Result<()> {
    let (mvcc, _dir) = setup()?;
    let txn = mvcc.begin_with_mode(Mode::ReadOnly)?;
    assert_eq!(1, txn.id());
    assert_eq!(Mode::ReadOnly, txn.mode());
    txn.commit()?;
    Ok(())
}

#[test]
fn test_begin_with_mode_readwrite() -> Result<()> {
    let (mvcc, _dir) = setup()?;
    let txn = mvcc.begin_with_mode(Mode::ReadWrite)?;
    assert_eq!(1, txn.id());
    assert_eq!(Mode::ReadWrite, txn.mode());
    txn.commit()?;
    Ok(())
}

#[test]
fn test_begin_with_mode_snapshot() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    // Write a couple of versions for a key
    let txn = mvcc.begin_with_mode(Mode::ReadWrite)?;
    txn.set(b"key", vec![0x01])?;
    txn.commit()?;
    let txn = mvcc.begin_with_mode(Mode::ReadWrite)?;
    txn.set(b"key", vec![0x02])?;
    txn.commit()?;

    // Check that we can start a snapshot in version 1
    let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 1 })?;
    assert_eq!(3, txn.id());
    assert_eq!(Mode::Snapshot { version: 1 }, txn.mode());
    assert_eq!(Some(vec![0x01]), txn.get(b"key")?);
    txn.commit()?;

    // Check that we can start a snapshot in a past snapshot transaction
    let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 3 })?;
    assert_eq!(4, txn.id());
    assert_eq!(Mode::Snapshot { version: 3 }, txn.mode());
    assert_eq!(Some(vec![0x02]), txn.get(b"key")?);
    txn.commit()?;

    // Check that the current transaction ID is valid as a snapshot version
    let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 5 })?;
    assert_eq!(5, txn.id());
    assert_eq!(Mode::Snapshot { version: 5 }, txn.mode());
    txn.commit()?;

    // Check that any future transaction IDs are invalid
    assert_eq!(
        mvcc.begin_with_mode(Mode::Snapshot { version: 9 }).err(),
        Some(Error::Value("Snapshot not found for version 9".into()))
    );

    // Check that concurrent transactions are hidden from snapshots of snapshot transactions.
    // This is because any transaction, including a snapshot transaction, allocates a new
    // transaction ID, and we need to make sure concurrent transaction at the time the
    // transaction began are hidden from future snapshot transactions.
    let txn_active = mvcc.begin()?;
    let txn_snapshot = mvcc.begin_with_mode(Mode::Snapshot { version: 1 })?;
    assert_eq!(7, txn_active.id());
    assert_eq!(8, txn_snapshot.id());
    txn_active.set(b"key", vec![0x07])?;
    assert_eq!(Some(vec![0x01]), txn_snapshot.get(b"key")?);
    txn_active.commit()?;
    txn_snapshot.commit()?;

    let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 8 })?;
    assert_eq!(9, txn.id());
    assert_eq!(Some(vec![0x02]), txn.get(b"key")?);
    txn.commit()?;

    Ok(())
}

#[test]
fn test_resume() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    // We first write a set of values that should be visible.
    let t1 = mvcc.begin()?;
    t1.set(b"a", b"t1".to_vec())?;
    t1.set(b"b", b"t1".to_vec())?;
    t1.commit()?;

    // We then start three transactions, of which we will resume t3.
    // We commit t2 and t4's changes, which should not be visible,
    // and write a change for t3 which should be visible.
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;
    let t4 = mvcc.begin()?;

    t2.set(b"a", b"t2".to_vec())?;
    t3.set(b"b", b"t3".to_vec())?;
    t4.set(b"c", b"t4".to_vec())?;

    t2.commit()?;
    t4.commit()?;

    // We now resume t3, who should see it's own changes but none of the others'.
    let id = t3.id();
    std::mem::drop(t3);
    let tr = mvcc.resume(id)?;
    assert_eq!(3, tr.id());
    assert_eq!(Mode::ReadWrite, tr.mode());

    assert_eq!(Some(b"t1".to_vec()), tr.get(b"a")?);
    assert_eq!(Some(b"t3".to_vec()), tr.get(b"b")?);
    assert_eq!(None, tr.get(b"c")?);

    // A separate transaction should not see t3's changes, but should see the others.
    let t = mvcc.begin()?;
    assert_eq!(Some(b"t2".to_vec()), t.get(b"a")?);
    assert_eq!(Some(b"t1".to_vec()), t.get(b"b")?);
    assert_eq!(Some(b"t4".to_vec()), t.get(b"c")?);
    t.rollback()?;

    // Once tr commits, a separate transaction should see t3's changes.
    tr.commit()?;

    let t = mvcc.begin()?;
    assert_eq!(Some(b"t2".to_vec()), t.get(b"a")?);
    assert_eq!(Some(b"t3".to_vec()), t.get(b"b")?);
    assert_eq!(Some(b"t4".to_vec()), t.get(b"c")?);
    t.rollback()?;

    // It should also be possible to start a snapshot transaction and resume it.
    let ts = mvcc.begin_with_mode(Mode::Snapshot { version: 1 })?;
    assert_eq!(7, ts.id());
    assert_eq!(Some(b"t1".to_vec()), ts.get(b"a")?);

    let id = ts.id();
    std::mem::drop(ts);
    let ts = mvcc.resume(id)?;
    assert_eq!(7, ts.id());
    assert_eq!(Mode::Snapshot { version: 1 }, ts.mode());
    assert_eq!(Some(b"t1".to_vec()), ts.get(b"a")?);
    ts.commit()?;

    // Resuming an inactive transaction should error.
    assert_eq!(mvcc.resume(7).err(), Some(Error::Value("No active transaction 7".into())));

    Ok(())
}

#[test]
fn test_txn_delete_conflict() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.set(b"key", vec![0x00])?;
    txn.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;

    t2.delete(b"key")?;
    assert_eq!(Err(Error::Serialization), t1.delete(b"key"));
    assert_eq!(Err(Error::Serialization), t3.delete(b"key"));
    t2.commit()?;

    Ok(())
}

#[test]
fn test_txn_delete_idempotent() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.delete(b"key")?;
    txn.commit()?;

    Ok(())
}

#[test]
fn test_txn_get() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    assert_eq!(None, txn.get(b"a")?);
    txn.set(b"a", vec![0x01])?;
    assert_eq!(Some(vec![0x01]), txn.get(b"a")?);
    txn.set(b"a", vec![0x02])?;
    assert_eq!(Some(vec![0x02]), txn.get(b"a")?);
    txn.commit()?;

    Ok(())
}

#[test]
fn test_txn_get_deleted() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.set(b"a", vec![0x01])?;
    txn.commit()?;

    let txn = mvcc.begin()?;
    txn.delete(b"a")?;
    txn.commit()?;

    let txn = mvcc.begin()?;
    assert_eq!(None, txn.get(b"a")?);
    txn.commit()?;

    Ok(())
}

#[test]
fn test_txn_get_hides_newer() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;

    t1.set(b"a", vec![0x01])?;
    t1.commit()?;
    t3.set(b"c", vec![0x03])?;
    t3.commit()?;

    assert_eq!(None, t2.get(b"a")?);
    assert_eq!(None, t2.get(b"c")?);

    Ok(())
}

#[test]
fn test_txn_get_hides_uncommitted() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t1 = mvcc.begin()?;
    t1.set(b"a", vec![0x01])?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;
    t3.set(b"c", vec![0x03])?;

    assert_eq!(None, t2.get(b"a")?);
    assert_eq!(None, t2.get(b"c")?);

    Ok(())
}

#[test]
fn test_txn_get_read_only_historical() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.set(b"a", vec![0x01])?;
    txn.commit()?;

    let txn = mvcc.begin()?;
    txn.set(b"b", vec![0x02])?;
    txn.commit()?;

    let txn = mvcc.begin()?;
    txn.set(b"c", vec![0x03])?;
    txn.commit()?;

    let tr = mvcc.begin_with_mode(Mode::Snapshot { version: 2 })?;
    assert_eq!(Some(vec![0x01]), tr.get(b"a")?);
    assert_eq!(Some(vec![0x02]), tr.get(b"b")?);
    assert_eq!(None, tr.get(b"c")?);

    Ok(())
}

#[test]
fn test_txn_get_serial() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.set(b"a", vec![0x01])?;
    txn.commit()?;

    let txn = mvcc.begin()?;
    assert_eq!(Some(vec![0x01]), txn.get(b"a")?);

    Ok(())
}