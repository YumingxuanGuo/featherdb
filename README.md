# FeatherDB

Version: 0.3.0

## Introduction

FeatherDB is an disk-based, concurrent, transactional, unreliable, non-relational, centralized database management system in Rust,
written for educational purposes.

## What's New

In our latest release, FeatherDB introduces support for concurrent ACID transactions based on Multi-Version Concurrency Control (MVCC). We now offer two isolation levels: Snapshot Isolation (SI) and Serializable Snapshot Isolation (SSI).

### Snapshot Isolation (SI)

Snapshot Isolation enables multiple transactions to execute concurrently by creating a distinct snapshot of the database for each transaction. This approach avoids the need for locks and ensures that write operations do not block read operations, reducing contention between transactions and enhancing overall system performance. Furthermore, SI versions all data, allowing queries on historical data. To provide ACID transactions, commits are atomic: a transaction operates on a consistent snapshot of the key/value store from the beginning of the transaction, and any write conflicts lead to serialization errors that require retries.

Reference: [toyDB](https://github.com/erikgrinaker/toydb).

### Serializable Snapshot Isolation (SSI)

While SI eliminates most inconsistencies, including phantom reads, it is not fully serializable, as it may exhibit write skew anomalies. To address this, we have introduced a separate mechanism to detect transactions with consecutive read-write dependencies, a necessary but insufficient condition for write skews. If a transaction is detected, it is aborted, rolled back, and retried. This approach conservatively eliminates write skews and ensures serializability, although some benign transactions might be aborted. However, we accept the overhead of false positives due to their infrequency.

Reference: [Serializable Isolation for Snapshot Databases](https://courses.cs.washington.edu/courses/cse444/08au/544M/READING-LIST/fekete-sigmod2008.pdf).

## Key-Value Storage

FeatherDB has evolved from a purely in-memory solution to a disk-based, persistent storage system. 
We've chosen the LSM-tree as the primary key-value storage engine due to its excellent write performance.

We provide five methods as the interface:

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

The LSM-tree comprises two distinct data structures: a memtable for in-memory data and an SSTable (Sorted String Table) for on-disk data. Once data is flushed to disk as SSTables, it becomes immutable; mutable operations are only performed in the memtables.

We employ a lock-free skiplist as the underlying implementation for the memtable, ensuring thread-safety in concurrent operations. 
Additionally, all interface methods take immutable references, allowing us to access the storage with a simple `Arc<LsmTree>` instead of `Arc<RwLock<LsmTree>>`.

We also offer iterator functionality in the `scan` method, 
enabling efficient key-range traversals and laying the foundation for potential future SQL query support.

## Usage

At the moment, we do not provide a command-line interface for FeatherDB. However, we are actively working on this feature, and it will be available in future releases. Please follow our GitHub repository for updates and new releases.

## What's Next

We have several exciting developments planned for the near future and beyond:

* Relational Model: We're working on an SQL interface that will include projections, filters, joins, aggregates, and transactions to provide a comprehensive relational model experience.

In the longer term, we have the following enhancements in mind:

* Distributed System: We plan to implement a Raft-based distributed consensus engine for linearizable state machine replication, expanding FeatherDB's capabilities in distributed environments.

* Write-ahead-log (WAL): To ensure durability and data integrity, we will introduce a WAL mechanism that guarantees all operations will eventually be performed, even after unexpected crashes.

* Leveled Compaction: As an essential part of the LSM-tree, we will implement level compaction to minimize storage overhead for outdated and deleted entries, optimizing storage utilization.

* Bloom Filter: To enhance key searching performance, we will incorporate Bloom filters as an optimization technique, reducing unnecessary disk I/O operations.

Stay tuned for these upcoming features and improvements in FeatherDB. Be sure to follow our GitHub repository to stay up-to-date with our latest developments and releases.