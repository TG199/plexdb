# plexdb

**plexdb** is a fast, minimal, persistant key-value store written in Rust.

---

## ✨ Features
- ✅ Persistent key-value storage using partitioned log files
- 🔁 Write-Ahead Logging (WAL) for durability
- 📦 LRU and Bloom filter-based caching
- 🧱 Compaction and snapshotting support
- 🔌 Command-line interface (CLI) and network protocol
- 📊 Prometheus metrics integration
- ⚡ Replication via master-slave or consensus protocol

---

## 🗂️ Project Structure

```text
.
├── config/           # Config files for different environments
├── data/             # WALs, partitions, snapshots, and metadata
├── docs/             # System architecture and API docs
├── src/              # Application source code
│   ├── engine/       # Core engine logic
│   ├── cache/        # LRU cache & Bloom filters
│   ├── cli/          # Command-line interface
│   ├── network/      # Server & client logic
│   ├── replication/  # Replication modules
│   ├── storage/      # File and WAL management
│   ├── metrics/      # Prometheus metrics
│   └── utils/        # Utility functions
├── tests/            # Benchmark and integration tests
├── Cargo.toml        # Project dependencies & metadata
└── README.md         # Project overview

Still under development 🔧
