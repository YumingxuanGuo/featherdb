use std::borrow::Cow;

use crate::error::Result;

/// MVCC keys. The encoding preserves the grouping and ordering of keys. 
/// Uses a Cow since we want to take borrows when encoding and return owned when decoding. 
/// (TODO: Why?)
#[derive(Debug)]
pub enum TxnKey<'a> {
    /// The next available txn ID. Used when starting new txns.
    TxnNext,
    /// Active txn markers, containing the mode. Used to detect concurrent txns, and to resume.
    TxnActive(u64),
    /// Txn snapshot, containing concurrent active txns at start of txn.
    TxnSnapshot(u64),
    /// Update marker for a txn ID and key, used for rollback.
    TxnUpdate(u64, Cow<'a, [u8]>),
    /// Arbitrary unversioned metadata.
    Metadata(Cow<'a, [u8]>),
    /// A record for a key/version pair.
    Record(Cow<'a, [u8]>, u64),
}

impl<'a> TxnKey<'a> {
    /// Encodes a key into a byte vector.
    pub fn encode(self) -> Vec<u8> {
        // use encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => vec![0x02],
            Self::TxnSnapshot(version) => vec![0x03],
            Self::TxnUpdate(id, key) => vec![0x04],
            Self::Metadata(key) => vec![0x05],
            Self::Record(key, version) => vec![0xff],
        }
    }

    /// Decodes a key from a byte representation.
    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        // use encoding::*;
        Ok(TxnKey::TxnNext)
    }
}