//! # rust_pg_db - A PostgreSQL-style Database in Rust
//!
//! This is a minimal but robust relational database implementation featuring:
//! - **ACID Transactions**: Write-Ahead Logging (WAL) with LSN-based crash recovery
//! - **Buffer Pool Management**: LRU page caching for performance
//! - **SQL Support**: Parser and executor for CREATE/INSERT/SELECT/DELETE
//! - **Concurrent Access**: Thread-safe operations with async/await
//! - **TCP Server**: Multi-client SQL connections
//!
//! ## Architecture Overview
//!
//! The database consists of three main layers:
//!
//! 1. **Server Layer** (`server` module): Handles TCP connections and SQL protocol
//! 2. **Engine Layer** (`engine` module): Core database operations, transactions, and storage
//! 3. **SQL Layer** (`sql` module): SQL parsing, planning, and execution
//!
//! ## Key Components
//!
//! - **Engine**: Central coordinator managing transactions, recovery, and storage
//! - **WAL (Write-Ahead Log)**: Ensures durability and enables crash recovery
//! - **Buffer Pool**: LRU cache for database pages with dirty page management
//! - **Catalog**: Schema metadata storage and management
//! - **Table Storage**: Page-based data storage with serialization
//!
//! ## Usage Example
//!
//! ```bash
//! # Start the database server
//! cargo run -- --data ./dbdata --listen 127.0.0.1:54330
//!
//! # Connect and run SQL commands
//! echo "CREATE TABLE users (id INT, name TEXT);" | nc 127.0.0.1 54330
//! echo "INSERT INTO users VALUES (1, 'Alice');" | nc 127.0.0.1 54330
//! echo "SELECT * FROM users;" | nc 127.0.0.1 54330
//! ```

/// TCP server and client connection handling
pub mod server;

/// Core database engine with transactions, WAL, and storage management
pub mod engine;

/// SQL parsing, query planning, and execution
pub mod sql;