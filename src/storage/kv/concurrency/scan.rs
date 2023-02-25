use std::iter::Peekable;

use crate::{storage::StorageScan, error::{Error, Result}};

use super::{snapshot::Snapshot, txnkey::TxnKey, mvcc::deserialize};


/// A key range scan.
pub struct Scan {
    /// The augmented KV store iterator, with key (decoded) and value. Note that we don't retain
    /// the decoded version, so there will be multiple keys (for each version). We want the last.
    scan: Peekable<StorageScan>,
    /// Keeps track of next_back() seen key, whose previous versions should be ignored.
    next_back_seen: Option<Vec<u8>>,
}

impl Scan {
    /// Creates a new scan.
    pub(super) fn new(mut scan: StorageScan, snapshot: Snapshot) -> Self {
        // Augment the underlying scan to decode the key and filter invisible versions. We don't
        // return the version, since we don't need it, but beware that all versions of the key
        // will still be returned - we usually only need the last, which is what the next() and
        // next_back() methods need to handle. We also don't decode the value, since we only need
        // to decode the last version.
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match TxnKey::decode(&k)? {
                TxnKey::Record(_, version) if !snapshot.is_visible(version) => Ok(None),
                TxnKey::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self { scan: scan.peekable(), next_back_seen: None }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // Only return the item if it is the last version of the key.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // Only return the last version of the key (so skip if seen).
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Scan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}