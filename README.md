# GymxDB

## Introduction

GymxDB is a distributed relational database written in Rust. Its main components are

* Raft-based distributed consensus engine for linearizable state machine replication.

* ACID-compliant transaction engine with MVCC-based snapshot isolation.

* Pluggable storage engine with B+tree and log-structured backends.

* Iterator-based query engine with heuristic optimization and time-travel support.

* SQL interface including projections, filters, joins, aggregates, and transactions.

GymxDB is inspired by and partly based on [toyDB](https://github.com/erikgrinaker/toydb), but with significant improvements including

* Disk manager that communicates between memory and disk, guaranteeing persistence.

* Indexing based on B+tree and extendable hash table that speeds up SQL queries by up to 60%.

* Support for all four isolation levels: read uncommitted, read committed, repeatable read, and serializable.

* Implementation of LSM-tree that improves log storage performance.

* Snapshot technique for log compaction in Raft implementation, enabling less space consumption and faster restoration in server reboots.

