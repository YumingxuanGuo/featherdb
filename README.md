# FeatherDB

Version: 0.2.0

## Introduction

FeatherDB is an on-disk, persistent, concurrent, unreliable, non-transactional, non-relational, centralized database.

## What's New
In the most recent release, FeatherDB evolved from purely in-memory to disk-based with persistency. 
We chose LSM-tree as the main key-value storage engine for its great performance in writing operations.

Five methods are provided as the interface:
```Rust
/// Sets a value for a key, replacing the existing value if any.
fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;

/// Gets a value for a key, if it exists.
fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

/// Deletes a key, doing nothing if it does not exist.
fn delete(&self, key: &[u8]) -> Result<()>;

/// Iterates over an ordered range of key/value pairs.
fn scan(&self, range: Range) -> Result<KvScan>;

/// Flushes any buffered data to the underlying storage medium.
fn flush(&self) -> Result<()>;
```

LSM-tree contains two different data structures: memtable for in-memory data, and sstable (sorted string table) for on-disk data.
Once the data are flushed as sstables to disk, they become immutable; so mutable operations are only performed in the memtables.

We used a lock-free skiplist as the underlying implementation for the memtable, assuring thread-safety in concurrent operations. 
As a plus, all the methods in the interface take immutable references, allowing us to access the storage with simply `Arc<LsmTree>`, instead of `Arc<RwLock<LsmTree>>`.

We also provided the iterator functionality in the `scan` method. 
This offers efficient key-range traversals and supports for, possibly in the future, SQL queries.

## Usage

Currently, there is no command-line interface available.

## What's Next

Some of the most imminent developments includes:

* Write-ahead-log (WAL): the fail-safe that guarantees all operations will be performed eventually even after unexpected crashes.

* Level compaction: the "merge" part of the LSM-tree that reduces the storage overhead for outdated and deleted entries.

* Bloom filter: an optimization technique that improves key searching performance.

* Transaction: an transactional engine that supports ACID transactions under different isolation levels.

Further upcoming ones includes:

* Relational model: an SQL interface including projections, filters, joins, aggregates, and transactions.

* Distributed system: a Raft-based distributed consensus engine for linearizable state machine replication.