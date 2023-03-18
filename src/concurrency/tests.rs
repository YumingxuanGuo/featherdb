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