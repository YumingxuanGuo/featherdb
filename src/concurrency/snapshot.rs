use std::{sync::{RwLockWriteGuard, RwLockReadGuard}, collections::HashSet};

use crate::{error::{Error, Result}, storage::{Store, Range}};

use super::{key::Key, serialize, deserialize};

/// A versioned snapshot, containing visibility information about concurrent transactions.
#[derive(Clone)]
pub struct Snapshot {
    /// The version (i.e. transaction ID) that the snapshot belongs to.
    version: u64,
    /// The set of transaction IDs that were active at the start of the transactions,
    /// and thus should be invisible to the snapshot.
    pub(super) invisible: HashSet<u64>,
}

impl Snapshot {
    /// Takes a new snapshot, persisting it as `Key::TxnSnapshot(version)`.
    pub(super) fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut snapshot = Self { version, invisible: HashSet::new() };

        // A scan of all the TxnActive keys.
        let mut scan = 
            session.scan(Range::from(Key::TxnActive(0).encode()..Key::TxnActive(version).encode()));

        // Records the currently active txn IDs as invisible.
        while let Some((key, _)) = scan.next().transpose()? {
            // Ensures the scan contains only TxnActive keys (based on the encoding property).
            match Key::decode(&key)? {
                Key::TxnActive(id) => snapshot.invisible.insert(id),
                otherwise => return Err(Error::Internal(format!("Expected TxnActive, got {:?}", otherwise))),
            };
        }

        // Store info of this new snapshot.
        std::mem::drop(scan);
        session.set_or_insert(&Key::TxnSnapshot(version).encode(), serialize(&snapshot.invisible)?)?;

        Ok(snapshot)
    }

    /// Restores an existing snapshot from `Key::TxnSnapshot(version)`, or errors if not found.
    pub(super) fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref invisible) => Ok(Self { version, invisible: deserialize(invisible)? }),
            None => Err(Error::Value(format!("Snapshot not found for version {}", version))),
        }
    }

    /// Checks whether the given version is visible in this snapshot.
    pub(super) fn is_visible(&self, version: u64) -> bool {
        version <= self.version && !self.invisible.contains(&version)
    }
}
