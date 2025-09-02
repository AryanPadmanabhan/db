# Developer Documentation - rust_pg_db

## 🚀 Getting Started

This database is now **fully documented** with comprehensive function-level comments. Every public function includes:
- **Purpose and behavior** 
- **Arguments and return values**
- **Usage examples**
- **Error conditions**
- **Implementation notes**

### Quick Start for New Developers

```bash
# 1. Clone and build
cargo build

# 2. Run tests
cargo test

# 3. Start the database  
cargo run -- --data ./mydata --listen 127.0.0.1:54330

# 4. Connect and test
echo "CREATE TABLE users (id INT, name TEXT);" | nc 127.0.0.1 54330
```

## 📋 Code Architecture Overview

### Module Structure
```
src/
├── main.rs           # Entry point with CLI parsing
├── lib.rs            # Module exports and crate documentation  
├── server.rs         # TCP server and client protocol
├── engine/           # Core database engine
│   ├── mod.rs        # Main Engine coordinator  
│   ├── catalog.rs    # Schema and metadata management
│   ├── storage.rs    # WAL and table storage
│   ├── buffer.rs     # LRU buffer pool
│   └── executor.rs   # Query execution
└── sql/
    └── mod.rs        # SQL parsing and planning
```

### Data Flow
```
TCP Client → Server → SQL Parser → Engine → Storage/WAL
     ↑                                ↓
   Results ← JSON Formatter ← Query Executor ← Buffer Pool
```

## 🔧 Key Functions for Development

### Starting Points
- **`main()`** - Application entry point with CLI setup
- **`serve()`** - TCP server that handles client connections  
- **`plan_and_exec()`** - SQL statement processing pipeline

### Core Database Operations
- **`Engine::open()`** - Database initialization and crash recovery
- **`Engine::create_table()`** - DDL table creation with WAL
- **`Engine::insert_rows()`** - DML row insertion with ACID guarantees  
- **`Engine::select()`** - Query execution with filtering/projection
- **`Engine::delete()`** - Row deletion with predicate matching

### Low-Level Storage
- **`TableStorage::append_row()`** - Page-based row storage
- **`BufferPool::get_or_load()`** - Page caching with LRU eviction
- **`Wal::append()`** - Write-ahead logging for durability

## 🛠️ Adding New Features

### 1. Adding a New SQL Statement
```rust
// In src/sql/mod.rs, add to the match statement:
Statement::Update { table, assignments, selection, .. } => {
    // 1. Extract table name
    let table_name = /* parse table */;
    
    // 2. Parse assignments  
    let updates = /* parse SET clauses */;
    
    // 3. Parse WHERE clause
    let predicate = parse_where(selection)?;
    
    // 4. Call engine method
    let affected = engine.update_rows(table_name, updates, predicate).await?;
    Ok(format!("UPDATE {}", affected))
}
```

### 2. Adding a New Engine Method
```rust  
// In src/engine/mod.rs:
impl Engine {
    /// Updates rows matching predicate with new values.
    /// Follows WAL-first protocol for durability.
    pub async fn update_rows(
        &self, 
        table: &str, 
        updates: HashMap<String, Value>,
        predicate: Option<Predicate>
    ) -> Result<usize> {
        // 1. Create WAL record
        let record = self.wal.write().create_record(WalRecordType::Update {
            table: table.to_string(),
            updates: updates.clone(),
            predicate: predicate.clone(),
        });
        let lsn = record.lsn();
        
        // 2. Write to WAL first
        self.wal.write().append(record)?;
        
        // 3. Apply to storage
        let affected = self.apply_update(table, updates, predicate).await?;
        
        // 4. Update LSN checkpoint
        self.catalog.write().update_last_applied_lsn(lsn)?;
        
        Ok(affected)
    }
}
```

### 3. Adding New Data Types
```rust
// In src/engine/mod.rs, extend the Value enum:
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Text(String),
    Boolean(bool),     // Add this
    Float(f64),        // Add this  
    Timestamp(i64),    // Add this (Unix timestamp)
}

// Update type inference in SQL parser (src/sql/mod.rs):
let ty = match c.data_type {
    DataType::Int(_) | DataType::Integer(_) => Value::Int(0),
    DataType::Text | DataType::Varchar(_) => Value::Text(String::new()),
    DataType::Boolean => Value::Boolean(false),           // Add this
    DataType::Float(_) | DataType::Real => Value::Float(0.0), // Add this
    _ => return Err(anyhow!("Unsupported type for column {}", cname)),
};
```

## 🔍 Understanding the WAL System

The database uses **Log Sequence Numbers (LSNs)** for crash recovery:

### WAL Record Lifecycle
1. **Operation starts** → `wal.create_record()` assigns unique LSN
2. **Write to WAL** → `wal.append()` persists to disk with fsync  
3. **Apply to storage** → Update pages in buffer pool
4. **Update checkpoint** → `catalog.update_last_applied_lsn(lsn)`

### Recovery Process
1. **Read checkpoint** → Get `last_applied_lsn` from catalog
2. **Filter WAL** → Only read records with LSN > `last_applied_lsn`
3. **Replay operations** → Apply each record to storage
4. **Update checkpoint** → Save new `last_applied_lsn`

This ensures **exactly-once semantics** - no operation is applied twice.

## 📊 Buffer Pool Management

The LRU buffer pool caches database pages in memory:

### Page Lifecycle  
1. **Read request** → `get_or_load()` checks cache first
2. **Cache miss** → Load page from disk into buffer
3. **Cache hit** → Return existing page from buffer
4. **Page modification** → Set `dirty = true`
5. **Eviction** → Write dirty pages to disk when space needed

### Key Methods
- **`get_or_load()`** - Main entry point for page access
- **`flush_all_for()`** - Write all dirty pages for a table
- **`insert_with_eviction()`** - Add page with LRU eviction

## 🧪 Testing Guidelines

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_create_and_query_table() {
        let engine = Engine::open("test_db").await.unwrap();
        
        // Test table creation
        engine.create_table("users", vec![
            ("id".to_string(), Value::Int(0)),
            ("name".to_string(), Value::Text("".to_string()))
        ]).await.unwrap();
        
        // Test insertion
        let rows = vec![HashMap::from([
            ("id".to_string(), Value::Int(1)),
            ("name".to_string(), Value::Text("Alice".to_string()))
        ])];
        engine.insert_rows("users", rows).await.unwrap();
        
        // Test query
        let results = engine.select("users", None, None).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["name"], Value::Text("Alice".to_string()));
    }
}
```

### Integration Tests
```bash
# Test server with netcat
echo "CREATE TABLE test (id INT);" | nc 127.0.0.1 54330
echo "INSERT INTO test VALUES (1);" | nc 127.0.0.1 54330  
echo "SELECT * FROM test;" | nc 127.0.0.1 54330
```

## 🚨 Common Gotchas

### 1. LSN Tracking
- **Always update LSN** after successful operations
- **Never skip WAL** for durability-critical operations
- **Use WAL-first protocol** (log before apply)

### 2. Buffer Pool Usage
- **Route all I/O through buffer pool** for consistency
- **Set dirty flags** when modifying pages
- **Flush before reads** to ensure data visibility

### 3. Concurrency
- **Use proper locking** with RwLock for shared state
- **Avoid deadlocks** by consistent lock ordering
- **Keep critical sections short** to minimize contention

### 4. Error Handling
- **Propagate errors properly** with `?` operator
- **Use structured logging** with `tracing` crate
- **Handle async cancellation** gracefully

## 📈 Performance Considerations

### Bottlenecks
1. **WAL fsync** - Every write waits for disk  
2. **Full table scans** - No indexes yet
3. **Buffer pool size** - Limited to 64 pages  
4. **Single-threaded SQL parsing** - No parallelism

### Optimization Ideas
1. **Batch WAL writes** - Group multiple operations
2. **Add B-tree indexes** - Eliminate table scans  
3. **Increase buffer pool** - More pages in memory
4. **Connection pooling** - Reuse client connections

## 🔮 Next Steps for Contributors

### Easy Wins (Good First Issues)
- [ ] Add `BOOLEAN` data type support
- [ ] Implement `UPDATE` statement  
- [ ] Add table-level `DESCRIBE` command
- [ ] Improve error messages with context

### Medium Complexity
- [ ] Implement B-tree primary key indexes
- [ ] Add `JOIN` operations between tables  
- [ ] Implement `GROUP BY` and aggregations
- [ ] Add constraint checking (NOT NULL, etc.)

### Advanced Features
- [ ] Multi-version concurrency control (MVCC)
- [ ] Query optimization with statistics
- [ ] PostgreSQL wire protocol compatibility
- [ ] Horizontal partitioning/sharding

## 📖 Additional Resources

- **Architecture**: See `claude.md` for high-level overview
- **Function docs**: Every function has detailed rustdoc comments
- **Examples**: Check `test_*.sh` scripts for usage examples  
- **Dependencies**: See `Cargo.toml` for external crates used

---

**Happy coding! 🦀** The database is well-documented and ready for feature development.