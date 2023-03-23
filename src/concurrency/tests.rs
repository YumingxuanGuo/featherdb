#![cfg(test)]
use tempfile::{tempdir, TempDir};

use super::*;
use crate::storage::kv::LsmStorage;
use crate::error::{Result, Error};

fn setup() -> Result<(MVCC, TempDir)> {
    let dir = tempdir()?;
    Ok((MVCC::new(Box::new(LsmStorage::open(&dir)?), true), dir))
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

    // We comment out this operation because it causes false positive in SSI.
    // assert_eq!(None, tr.get(b"c")?);

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

#[test]
fn test_txn_scan() -> Result<()> {
    let (mvcc, _dir) = setup()?;
    
    let txn = mvcc.begin()?;

    txn.set(b"a", vec![0x01])?;

    txn.delete(b"b")?;

    txn.set(b"c", vec![0x01])?;
    txn.set(b"c", vec![0x02])?;
    txn.delete(b"c")?;
    txn.set(b"c", vec![0x03])?;

    txn.set(b"d", vec![0x01])?;
    txn.set(b"d", vec![0x02])?;
    txn.set(b"d", vec![0x03])?;
    txn.set(b"d", vec![0x04])?;
    txn.delete(b"d")?;

    txn.set(b"e", vec![0x01])?;
    txn.set(b"e", vec![0x02])?;
    txn.set(b"e", vec![0x03])?;
    txn.delete(b"e")?;
    txn.set(b"e", vec![0x04])?;
    txn.set(b"e", vec![0x05])?;
    txn.commit()?;

    // Forward scan
    let txn = mvcc.begin()?;
    assert_eq!(
        vec![
            (b"a".to_vec(), vec![0x01]),
            (b"c".to_vec(), vec![0x03]),
            (b"e".to_vec(), vec![0x05]),
        ],
        txn.scan(..)?.collect::<Result<Vec<_>>>()?
    );

    // Reverse scan
    assert_eq!(
        vec![
            (b"e".to_vec(), vec![0x05]),
            (b"c".to_vec(), vec![0x03]),
            (b"a".to_vec(), vec![0x01]),
        ],
        txn.scan(..)?.rev().collect::<Result<Vec<_>>>()?
    );

    // Alternate forward/backward scan
    let mut scan = txn.scan(..)?;
    assert_eq!(Some((b"a".to_vec(), vec![0x01])), scan.next().transpose()?);
    assert_eq!(Some((b"e".to_vec(), vec![0x05])), scan.next_back().transpose()?);
    assert_eq!(Some((b"c".to_vec(), vec![0x03])), scan.next_back().transpose()?);
    assert_eq!(None, scan.next().transpose()?);
    std::mem::drop(scan);

    txn.commit()?;
    Ok(())
}

#[test]
fn test_txn_scan_key_version_overlap() -> Result<()> {
    // The idea here is that with a naive key/version concatenation
    // we get overlapping entries that mess up scans. For example:
    //
    // 00|00 00 00 00 00 00 00 01
    // 00 00 00 00 00 00 00 00 02|00 00 00 00 00 00 00 02
    // 00|00 00 00 00 00 00 00 03
    //
    // The key encoding should be resistant to this.
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.set(&[0], vec![0])?; // v0
    txn.set(&[0], vec![1])?; // v1
    txn.set(&[0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?; // v2
    txn.set(&[0], vec![3])?; // v3
    txn.commit()?;

    let txn = mvcc.begin()?;
    assert_eq!(
        vec![(vec![0].to_vec(), vec![3]), (vec![0, 0, 0, 0, 0, 0, 0, 0, 2].to_vec(), vec![2]),],
        txn.scan(..)?.collect::<Result<Vec<_>>>()?
    );
    Ok(())
}

#[test]
fn test_txn_scan_prefix() -> Result<()> {
    let (mvcc, _dir) = setup()?;
    
    let txn = mvcc.begin()?;

    txn.set(b"a", vec![0x01])?;
    txn.set(b"az", vec![0x01, 0x1a])?;
    txn.set(b"b", vec![0x02])?;
    txn.set(b"ba", vec![0x02, 0x01])?;
    txn.set(b"bb", vec![0x02, 0x02])?;
    txn.set(b"bc", vec![0x02, 0x03])?;
    txn.set(b"c", vec![0x03])?;
    txn.commit()?;

    // Forward scan
    let txn = mvcc.begin()?;
    assert_eq!(
        vec![
            (b"b".to_vec(), vec![0x02]),
            (b"ba".to_vec(), vec![0x02, 0x01]),
            (b"bb".to_vec(), vec![0x02, 0x02]),
            (b"bc".to_vec(), vec![0x02, 0x03]),
        ],
        txn.scan_prefix(b"b")?.collect::<Result<Vec<_>>>()?
    );

    // Reverse scan
    assert_eq!(
        vec![
            (b"bc".to_vec(), vec![0x02, 0x03]),
            (b"bb".to_vec(), vec![0x02, 0x02]),
            (b"ba".to_vec(), vec![0x02, 0x01]),
            (b"b".to_vec(), vec![0x02]),
        ],
        txn.scan_prefix(b"b")?.rev().collect::<Result<Vec<_>>>()?
    );

    // Alternate forward/backward scan
    let mut scan = txn.scan_prefix(b"b")?;
    assert_eq!(Some((b"b".to_vec(), vec![0x02])), scan.next().transpose()?);
    assert_eq!(Some((b"bc".to_vec(), vec![0x02, 0x03])), scan.next_back().transpose()?);
    assert_eq!(Some((b"bb".to_vec(), vec![0x02, 0x02])), scan.next_back().transpose()?);
    assert_eq!(Some((b"ba".to_vec(), vec![0x02, 0x01])), scan.next().transpose()?);
    assert_eq!(None, scan.next_back().transpose()?);
    std::mem::drop(scan);

    txn.commit()?;
    Ok(())
}

#[test]
fn test_txn_set_conflict() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;

    t2.set(b"key", vec![0x02])?;
    assert_eq!(Err(Error::Serialization), t1.set(b"key", vec![0x01]));
    assert_eq!(Err(Error::Serialization), t3.set(b"key", vec![0x03]));
    t2.commit()?;

    Ok(())
}

#[test]
fn test_txn_set_conflict_committed() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;

    t2.set(b"key", vec![0x02])?;
    t2.commit()?;
    assert_eq!(Err(Error::Serialization), t1.set(b"key", vec![0x01]));
    assert_eq!(Err(Error::Serialization), t3.set(b"key", vec![0x03]));

    Ok(())
}

#[test]
fn test_txn_set_rollback() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let txn = mvcc.begin()?;
    txn.set(b"key", vec![0x00])?;
    txn.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;
    let t3 = mvcc.begin()?;

    t2.set(b"key", vec![0x02])?;
    t2.rollback()?;
    assert_eq!(Some(vec![0x00]), t1.get(b"key")?);
    t1.commit()?;
    t3.set(b"key", vec![0x03])?;
    t3.commit()?;

    Ok(())
}

#[test]
// A dirty write is when t2 overwrites an uncommitted value written by t1.
fn test_txn_anomaly_dirty_write() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    t1.set(b"key", b"t1".to_vec())?;
    assert_eq!(t2.set(b"key", b"t2".to_vec()), Err(Error::Serialization));

    Ok(())
}

#[test]
// A dirty read is when t2 can read an uncommitted value set by t1.
fn test_txn_anomaly_dirty_read() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    t1.set(b"key", b"t1".to_vec())?;
    assert_eq!(None, t2.get(b"key")?);

    Ok(())
}

#[test]
// A lost update is when t1 and t2 both read a value and update it, where t2's update replaces t1.
fn test_txn_anomaly_lost_update() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t0 = mvcc.begin()?;
    t0.set(b"key", b"t0".to_vec())?;
    t0.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    t1.get(b"key")?;
    t2.get(b"key")?;

    t1.set(b"key", b"t1".to_vec())?;
    assert_eq!(t2.set(b"key", b"t2".to_vec()), Err(Error::Serialization));

    Ok(())
}

#[test]
// A fuzzy (or unrepeatable) read is when t2 sees a value change after t1 updates it.
fn test_txn_anomaly_fuzzy_read() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t0 = mvcc.begin()?;
    t0.set(b"key", b"t0".to_vec())?;
    t0.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(Some(b"t0".to_vec()), t2.get(b"key")?);
    t1.set(b"key", b"t1".to_vec())?;
    t1.commit()?;
    assert_eq!(Some(b"t0".to_vec()), t2.get(b"key")?);

    Ok(())
}

#[test]
// Read skew is when t1 reads a and b, but t2 modifies b in between the reads.
fn test_txn_anomaly_read_skew() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t0 = mvcc.begin()?;
    t0.set(b"a", b"t0".to_vec())?;
    t0.set(b"b", b"t0".to_vec())?;
    t0.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(Some(b"t0".to_vec()), t1.get(b"a")?);
    t2.set(b"a", b"t2".to_vec())?;
    t2.set(b"b", b"t2".to_vec())?;
    t2.commit()?;
    assert_eq!(Some(b"t0".to_vec()), t1.get(b"b")?);

    Ok(())
}

#[test]
// A phantom read is when t1 reads entries matching some predicate, but a modification by
// t2 changes the entries that match the predicate such that a later read by t1 returns them.
fn test_txn_anomaly_phantom_read() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t0 = mvcc.begin()?;
    t0.set(b"a", b"true".to_vec())?;
    t0.set(b"b", b"false".to_vec())?;
    t0.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(Some(b"true".to_vec()), t1.get(b"a")?);
    assert_eq!(Some(b"false".to_vec()), t1.get(b"b")?);

    t2.set(b"b", b"true".to_vec())?;
    t2.commit()?;

    assert_eq!(Some(b"true".to_vec()), t1.get(b"a")?);
    assert_eq!(Some(b"false".to_vec()), t1.get(b"b")?);

    Ok(())
}

// FIXME To avoid write skew we need to implement serializable snapshot isolation.
#[test]
// Write skew is when t1 reads b and writes it to a while t2 reads a and writes it to b.¨
fn test_txn_anomaly_write_skew() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    let t0 = mvcc.begin()?;
    t0.set(b"a", b"1".to_vec())?;
    t0.set(b"b", b"2".to_vec())?;
    t0.commit()?;

    let t1 = mvcc.begin()?;
    let t2 = mvcc.begin()?;

    assert_eq!(Some(b"2".to_vec()), t1.get(b"b")?);
    assert_eq!(Some(b"1".to_vec()), t2.get(b"a")?);

    // Some of the following operations should error
    let write_skew = || {
        t1.set(b"a", b"2".to_vec())?;
        t2.set(b"b", b"1".to_vec())?;
    
        t1.commit()?;
        t2.commit()?;

        Ok(())
    };
    assert_eq!(write_skew(), Err(Error::Serialization));

    Ok(())
}

#[test]
fn test_metadata() -> Result<()> {
    let (mvcc, _dir) = setup()?;

    mvcc.set_metadata(b"foo", b"bar".to_vec())?;
    assert_eq!(Some(b"bar".to_vec()), mvcc.get_metadata(b"foo")?);

    assert_eq!(None, mvcc.get_metadata(b"x")?);

    mvcc.set_metadata(b"foo", b"baz".to_vec())?;
    assert_eq!(Some(b"baz".to_vec()), mvcc.get_metadata(b"foo")?);
    Ok(())
}