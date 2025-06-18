# Kaydb

**Kaydb** is a fast, minimal, persistant key-value store written in Rust.

Built to demonstrate key systems programming principles such as file-backed storage, concurrency, trait-based design, and error handling in Rust.

---

## ✨ Features

- GET / SET / DELETE key-value operations
- File-backed persistence (write-ahead log)
- In-memory indexing for fast lookups
- Graceful shutdown & crash safety
- CLI interface with `clap`
- Easily extensible engine via trait abstraction

---

## 🔧 Usage

```bash
# Build the binary
cargo build --release

# Set a value
./target/release/kaydb set mykey myvalue

# Get a value
./target/release/kaydb get mykey

# Delete a key
./target/release/kaydb delete mykey
