# rust_pg_db - PostgreSQL-style Database in Rust

## Overview
This is a minimal PostgreSQL-style relational database implementation written in Rust. The database features SQL parsing, write-ahead logging (WAL), buffer pooling, table storage, and a TCP server for client connections.

## Project Structure

```
rust_pg_db/
├── src/
│   ├── main.rs              # Entry point and CLI argument parsing
│   ├── lib.rs               # Library root exposing public modules
│   ├── server.rs            # TCP server for SQL connections
│   ├── engine/              # Core database engine
│   │   ├── mod.rs           # Engine coordinator and main API
│   │   ├── catalog.rs       # Table schema management
│   │   ├── storage.rs       # Page-based storage and WAL
│   │   ├── buffer.rs        # LRU buffer pool for page caching
│   │   └── executor.rs      # Query execution (currently just SELECT)
│   └── sql/
│       └── mod.rs           # SQL parsing and query planning
├── dbdata/                  # Database files (data directory)
│   ├── catalog.json         # Table schemas metadata
│   ├── tables/              # Table data files
│   │   └── *.tbl            # Binary-serialized pages per table
│   └── wal/
│       └── log.jsonl        # Write-ahead log (JSON lines)
├── Cargo.toml              # Rust package configuration
└── .gitignore              # Git ignore rules
```

## Core Components

### 1. Engine (`src/engine/mod.rs:39-203`)
The main database engine coordinates all operations:

- **Startup**: Opens catalog, WAL, and buffer pool; performs crash recovery
- **Table Management**: Creates tables and maintains schema in catalog
- **CRUD Operations**: Handles INSERT, SELECT, DELETE through storage layer
- **Transaction Safety**: Uses WAL for durability and atomic operations
- **Concurrency**: Uses RwLocks for thread-safe access to shared state

Key data structures:
- `Value`: Supports Int(i64) and Text(String) types
- `Row`: HashMap of column name to Value
- `Predicate`: WHERE clause conditions (Eq and And operations)

### 2. Catalog (`src/engine/catalog.rs:21-54`)
Manages table schemas and metadata:

- **Schema Storage**: JSON-serialized table definitions
- **Column Types**: Currently supports Int and Text
- **Persistence**: Auto-saves schema changes to `dbdata/catalog.json`

### 3. Storage Layer (`src/engine/storage.rs:34-206`)
Handles data persistence with page-based storage:

#### Table Storage (`src/engine/storage.rs:34-139`)
- **Page-based**: 4KB page size with multiple rows per page
- **File Format**: Binary-serialized using `bincode`
- **Operations**: append_row, scan, delete_where
- **Growth**: Auto-creates new pages when current page exceeds size limit

#### Write-Ahead Log (`src/engine/storage.rs:166-206`)
- **Durability**: All operations logged before execution
- **Format**: JSON Lines (`.jsonl`) with structured records
- **Recovery**: Replays all WAL entries on startup
- **Record Types**: CreateTable, Insert, Delete

### 4. Buffer Pool (`src/engine/buffer.rs:36-121`)
LRU-based page caching system:

- **Capacity**: Configurable (default 64 pages)
- **Eviction**: LRU policy with dirty page write-back
- **Thread Safety**: Mutex-protected LRU cache
- **Page Tracking**: Dirty bit for modified pages

### 5. Query Executor (`src/engine/executor.rs:6-40`)
Currently implements basic SELECT scanning:

- **Full Table Scan**: Reads all pages and filters rows
- **Predicate Evaluation**: Supports WHERE conditions
- **Projection**: Column selection (SELECT col1, col2 vs SELECT *)

### 6. SQL Parser (`src/sql/mod.rs:10-192`)
Uses `sqlparser` crate for SQL parsing and planning:

#### Supported SQL Operations:
- **CREATE TABLE**: `CREATE TABLE name (col1 INT, col2 TEXT)`
- **INSERT**: `INSERT INTO table (col1, col2) VALUES (val1, val2)`
- **SELECT**: `SELECT col1, col2 FROM table WHERE col = value`
- **DELETE**: `DELETE FROM table WHERE col = value`

#### WHERE Clause Support:
- Equality comparisons: `col = literal`
- AND operations: `col1 = val1 AND col2 = val2`
- Literal types: integers and quoted strings

### 7. TCP Server (`src/server.rs:7-49`)
Handles client connections over TCP:

- **Protocol**: Simple text-based (not PostgreSQL wire protocol)
- **Statement Parsing**: Statements end with semicolon
- **Responses**: JSON for SELECT, status messages for other operations
- **Concurrency**: Each client connection runs in separate async task

## Dependencies

### Core Runtime
- `anyhow`: Error handling with context
- `tokio`: Async runtime for server and I/O
- `parking_lot`: High-performance RwLocks and Mutexes
- `dashmap`: Concurrent HashMap (unused in current code)

### Serialization
- `serde` + `serde_json`: JSON serialization for catalog and WAL
- `bincode`: Binary serialization for table data pages
- `bytes`: Byte manipulation utilities

### SQL Processing
- `sqlparser`: SQL parsing and AST generation

### Data Structures
- `lru`: LRU cache implementation for buffer pool
- `uuid`: UUID generation (imported but unused)

### Utilities
- `tracing` + `tracing-subscriber`: Structured logging
- `clap`: Command-line argument parsing

### Development/Testing
- `tempfile`: Temporary files for tests
- `rstest`, `proptest`: Testing frameworks
- `serial_test`: Sequential test execution
- `assert_cmd`, `predicates`: Integration testing

## Usage

### Starting the Server
```bash
cargo run -- --data ./dbdata --listen 127.0.0.1:54330
```

### Connecting and Using
```bash
# Connect with netcat or telnet
nc 127.0.0.1 54330

# Example session:
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Linus');
SELECT * FROM users;
SELECT name FROM users WHERE id = 1;
DELETE FROM users WHERE id = 2;
QUIT;
```

## Data Files

### Catalog (`dbdata/catalog.json`)
```json
{
  "tables": {
    "users": {
      "name": "users", 
      "columns": [
        {"name": "id", "col_type": "Int"},
        {"name": "name", "col_type": "Text"}
      ]
    }
  }
}
```

### WAL (`dbdata/wal/log.jsonl`)
```json
{"type":"CreateTable","name":"users","columns":[...]}
{"type":"Insert","table":"users","row":{"id":{"Int":1},"name":{"Text":"Ada"}}}
{"type":"Delete","table":"users","equals":{"Eq":["id",{"Scalar":{"Int":1}}]}}
```

### Table Data (`dbdata/tables/*.tbl`)
Binary-encoded pages containing row data, managed by `bincode` serialization.

## Architecture Principles

### ACID Properties
- **Atomicity**: WAL ensures operations are logged before execution
- **Consistency**: Schema validation through catalog
- **Isolation**: RwLocks provide basic concurrency control
- **Durability**: WAL with fsync ensures data persistence

### Recovery
- WAL replay on startup recovers from crashes
- Buffer pool dirty pages are flushed during eviction
- All state reconstruction happens during engine initialization

### Concurrency Model
- Reader-writer locks allow concurrent reads
- Async/await throughout for non-blocking I/O
- Each client connection handled independently

## Limitations and Future Enhancements

### Current Limitations
- **Types**: Only Int and Text supported
- **Queries**: No JOINs, aggregations, or complex operations  
- **Indexing**: Full table scans only
- **Transactions**: No explicit transaction boundaries
- **Wire Protocol**: Custom protocol instead of PostgreSQL-compatible

### Potential Improvements
- Add more data types (DATE, FLOAT, BOOLEAN)
- Implement B-tree indexes for faster queries
- Add JOIN operations and query optimization
- Implement proper transaction isolation levels
- Add PostgreSQL wire protocol compatibility
- Implement UPDATE statements
- Add constraint checking and foreign keys