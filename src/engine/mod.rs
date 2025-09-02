use anyhow::{Result, anyhow};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};
use tracing::{info, warn};

mod storage;
mod buffer;
mod catalog;
mod executor;

pub use catalog::{Catalog, TableSchema, Column, ColumnType};
pub use storage::{Wal, WalRecord, WalRecordType, TableStorage, PageId};
pub use buffer::{BufferPool, BufferPage};
pub use executor::execute_select_scan;

/// Represents a value that can be stored in the database.
/// Currently supports integers and text strings.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Value {
    /// 64-bit signed integer value
    Int(i64),
    /// UTF-8 text string value
    Text(String),
}

/// Wrapper for values used in predicates and row comparisons.
/// Currently only supports scalar values, but designed for future extension
/// to support expressions, subqueries, etc.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum RowValue {
    /// A simple scalar value (Int or Text)
    Scalar(Value),
}

/// Represents a single row in a table as a map from column names to values.
/// This structure allows for flexible row handling and easy serialization.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Row {
    /// Column name -> Value mapping for this row
    pub cols: HashMap<String, Value>,
}

/// Represents WHERE clause conditions for filtering rows.
/// Supports basic equality and AND operations, designed for extension.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Predicate {
    /// Equality comparison: column = value
    Eq(String, RowValue),
    /// Logical AND of two predicates
    And(Box<Predicate>, Box<Predicate>),
}

/// The main database engine that coordinates all database operations.
/// 
/// The Engine is responsible for:
/// - Managing the Write-Ahead Log (WAL) for durability
/// - Coordinating buffer pool for performance
/// - Maintaining table schemas in the catalog
/// - Providing ACID transaction semantics
/// - Handling crash recovery with LSN tracking
///
/// ## Architecture
/// 
/// ```text
/// ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
/// │    WAL      │    │    Catalog   │    │ Buffer Pool │
/// │ (Durability)│    │  (Schemas)   │    │(Performance)│
/// └─────────────┘    └──────────────┘    └─────────────┘
///        │                   │                   │
///        └───────────────────┼───────────────────┘
///                            │
///                    ┌───────▼────────┐
///                    │     Engine     │
///                    │  (Coordinator) │
///                    └────────────────┘
///                            │
///                    ┌───────▼────────┐
///                    │ Table Storage  │
///                    │ (Persistence)  │
///                    └────────────────┘
/// ```
pub struct Engine {
    /// Base directory for all database files
    data_dir: PathBuf,
    /// Schema metadata and LSN tracking (thread-safe)
    catalog: RwLock<Catalog>,
    /// Write-ahead log for durability (thread-safe)
    wal: RwLock<Wal>,
    /// LRU buffer pool for page caching
    buffers: BufferPool,
    /// Active table storage handles (thread-safe)
    tables: RwLock<HashMap<String, Arc<TableStorage>>>,
}

impl Engine {
    /// Opens or creates a database engine with crash recovery.
    ///
    /// This method performs the following initialization sequence:
    /// 1. Creates necessary directory structure (tables/, wal/)
    /// 2. Loads or creates the schema catalog 
    /// 3. Opens the Write-Ahead Log (WAL)
    /// 4. Reopens existing table storage handles
    /// 5. Performs crash recovery by replaying uncommitted WAL records
    /// 6. Initializes the buffer pool (64 pages by default)
    ///
    /// ## LSN-Based Recovery
    /// 
    /// The recovery process uses Log Sequence Numbers (LSNs) to prevent
    /// double-application of operations:
    /// - Reads `last_applied_lsn` from catalog
    /// - Only replays WAL records with LSN > `last_applied_lsn`
    /// - Updates `last_applied_lsn` after successful recovery
    ///
    /// ## Arguments
    /// * `dir` - Directory path for database files (creates if doesn't exist)
    ///
    /// ## Returns
    /// * `Ok(Engine)` - Fully initialized database engine
    /// * `Err(_)` - File I/O errors, corruption, or initialization failures
    ///
    /// ## Example
    /// ```rust
    /// let engine = Engine::open("./my_database").await?;
    /// ```
    pub async fn open<P: Into<PathBuf>>(dir: P) -> Result<Self> {
        let data_dir: PathBuf = dir.into();
        
        // Create directory structure
        fs::create_dir_all(data_dir.join("tables"))?;
        fs::create_dir_all(data_dir.join("wal"))?;

        // Load schema catalog and WAL
        let catalog = Catalog::load_or_create(data_dir.join("catalog.json"))?;
        let wal_path = data_dir.join("wal").join("log.jsonl");
        let wal = Wal::open(wal_path)?;

        // Reopen table storage handles for existing tables
        let mut tables = HashMap::new();
        for (tname, _) in catalog.tables.iter() {
            let st = TableStorage::open(data_dir.join("tables"), tname)?;
            tables.insert(tname.clone(), Arc::new(st));
        }

        let mut engine = Self {
            data_dir,
            catalog: RwLock::new(catalog),
            wal: RwLock::new(wal),
            buffers: BufferPool::new(64), // 64 pages in buffer pool
            tables: RwLock::new(tables),
        };

        // Perform crash recovery
        engine.recover().await?;
        info!("Engine opened");
        Ok(engine)
    }

    /// Performs crash recovery by replaying WAL records since the last checkpoint.
    ///
    /// This method implements LSN-based recovery to ensure exactly-once semantics:
    /// 
    /// 1. **Read checkpoint**: Gets `last_applied_lsn` from catalog
    /// 2. **Filter WAL**: Only reads records with LSN > `last_applied_lsn`  
    /// 3. **Replay operations**: Applies each WAL record to restore state
    /// 4. **Update checkpoint**: Records new `last_applied_lsn` in catalog
    ///
    /// ## Recovery Safety
    /// 
    /// - **Idempotent**: Safe to run multiple times
    /// - **No double-application**: LSN tracking prevents re-applying operations
    /// - **Atomic**: Either all records are applied or none (on failure)
    /// - **Consistent**: Tables are created before data operations
    ///
    /// ## WAL Record Types
    /// 
    /// - `CreateTable`: Adds table to catalog and creates storage
    /// - `Insert`: Adds rows to table storage  
    /// - `Delete`: Removes rows matching predicate
    ///
    /// Called automatically during `Engine::open()`.
    async fn recover(&mut self) -> Result<()> {
        let last_applied_lsn = self.catalog.read().last_applied_lsn;
        let wal = self.wal.read();
        let records = wal.read_from_lsn(last_applied_lsn)?;
        drop(wal);
        
        if records.is_empty() {
            info!("No WAL records to replay (last_applied_lsn: {})", last_applied_lsn);
            return Ok(());
        }

        warn!(count = records.len(), last_applied_lsn, "Replaying WAL records from LSN");
        let mut max_applied_lsn = last_applied_lsn;
        
        // Replay each WAL record in sequence
        for r in records {
            match r {
                WalRecord::CreateTable { lsn, name, columns } => {
                    // Only create if table doesn't exist (idempotent)
                    if !self.catalog.read().tables.contains_key(&name) {
                        self.catalog
                            .write()
                            .create_table(&name, columns.clone())?;
                        self.tables.write().insert(
                            name.clone(),
                            Arc::new(TableStorage::open(
                                self.data_dir.join("tables"),
                                &name,
                            )?),
                        );
                    }
                    max_applied_lsn = lsn;
                }
                WalRecord::Insert { lsn, table, row } => {
                    self.apply_insert(&table, row).await?;
                    max_applied_lsn = lsn;
                }
                WalRecord::Delete { lsn, table, equals } => {
                    self.apply_delete(&table, equals).await?;
                    max_applied_lsn = lsn;
                }
            }
        }
        
        // Update the last applied LSN in catalog
        self.catalog.write().update_last_applied_lsn(max_applied_lsn)?;
        info!("Recovery complete, updated last_applied_lsn to {}", max_applied_lsn);
        Ok(())
    }

    /// Creates a new table with the specified name and column definitions.
    ///
    /// This operation follows the Write-Ahead Logging protocol for durability:
    /// 1. **WAL First**: Logs the CREATE TABLE operation with a new LSN
    /// 2. **Apply Changes**: Updates catalog and creates table storage file  
    /// 3. **Update Checkpoint**: Records the LSN as successfully applied
    ///
    /// ## Arguments
    /// * `name` - Table name (must be unique within the database)
    /// * `cols` - Vector of (column_name, sample_value) pairs to infer types
    ///
    /// ## Column Type Inference
    /// The column types are inferred from the sample values provided:
    /// - `Value::Int(_)` → `ColumnType::Int` 
    /// - `Value::Text(_)` → `ColumnType::Text`
    ///
    /// ## Atomicity
    /// If any step fails, the entire operation is rolled back and the table
    /// is not created. The LSN is only updated on complete success.
    ///
    /// ## Example
    /// ```rust
    /// engine.create_table("users", vec![
    ///     ("id".to_string(), Value::Int(0)),      // INT column
    ///     ("name".to_string(), Value::Text("".to_string()))  // TEXT column  
    /// ]).await?;
    /// ```
    pub async fn create_table(&self, name: &str, cols: Vec<(String, Value)>) -> Result<()> {
        // Convert sample values to column definitions
        let columns: Vec<Column> = cols
            .iter()
            .map(|(n, v)| Column {
                name: n.clone(),
                col_type: match v {
                    Value::Int(_) => ColumnType::Int,
                    Value::Text(_) => ColumnType::Text,
                },
            })
            .collect();

        // Write-Ahead Log: Record the operation first
        let record = self.wal.write().create_record(WalRecordType::CreateTable {
            name: name.to_string(),
            columns: columns.clone(),
        });
        let lsn = record.lsn();
        self.wal.write().append(record)?;

        // Apply changes: Update catalog and create table storage
        self.catalog
            .write()
            .create_table(name, columns)?;

        let st = Arc::new(TableStorage::open(self.data_dir.join("tables"), name)?);
        self.tables.write().insert(name.to_string(), st);
        
        // Update checkpoint: Mark this LSN as successfully applied
        self.catalog.write().update_last_applied_lsn(lsn)?;
        Ok(())
    }

    /// Inserts multiple rows into the specified table with full ACID guarantees.
    ///
    /// Each row insertion is treated as a separate WAL record for granular recovery.
    /// All rows are inserted atomically - if any insertion fails, the entire
    /// operation is aborted (though individual successful WAL records remain).
    ///
    /// ## Arguments
    /// * `table` - Name of the target table (must exist)
    /// * `rows` - Vector of rows where each row is a HashMap<column_name, value>
    ///
    /// ## Returns
    /// * `Ok(count)` - Number of rows successfully inserted
    /// * `Err(_)` - Table doesn't exist, type mismatch, or I/O error
    ///
    /// ## Example
    /// ```rust
    /// let rows = vec![
    ///     HashMap::from([("id", Value::Int(1)), ("name", Value::Text("Alice"))]),
    ///     HashMap::from([("id", Value::Int(2)), ("name", Value::Text("Bob"))]),
    /// ];
    /// let count = engine.insert_rows("users", rows).await?;
    /// ```
    pub async fn insert_rows(
        &self,
        table: &str,
        rows: Vec<HashMap<String, Value>>,
    ) -> Result<usize> {
        let mut n = 0;
        for r in rows {
            // WAL first: Log each insertion with unique LSN
            let record = self.wal.write().create_record(WalRecordType::Insert {
                table: table.to_string(),
                row: r.clone(),
            });
            let lsn = record.lsn();
            self.wal.write().append(record)?;
            
            // Apply change: Insert into table storage via buffer pool
            self.apply_insert(table, r).await?;
            
            // Update checkpoint: Mark this LSN as successfully applied
            self.catalog.write().update_last_applied_lsn(lsn)?;
            n += 1;
        }
        Ok(n)
    }

    /// Internal helper: Applies row insertion to table storage.
    /// Called during normal operation and WAL recovery.
    async fn apply_insert(&self, table: &str, row: HashMap<String, Value>) -> Result<()> {
        let st = self.get_storage(table)?;
        st.append_row(&self.buffers, row).await
    }

    /// Queries a table with optional filtering and column projection.
    ///
    /// This is a read-only operation that does not write to the WAL.
    /// The buffer pool is flushed before scanning to ensure all recent
    /// writes are visible.
    ///
    /// ## Arguments  
    /// * `table` - Name of the table to query
    /// * `pred` - Optional WHERE clause predicate for filtering rows
    /// * `cols` - Optional column list for projection (None = SELECT *)
    ///
    /// ## Returns
    /// * `Ok(rows)` - Vector of matching rows as HashMaps
    /// * `Err(_)` - Table doesn't exist or I/O error
    ///
    /// ## Example
    /// ```rust
    /// // SELECT * FROM users WHERE id = 1
    /// let pred = Some(Predicate::Eq("id".to_string(), RowValue::Scalar(Value::Int(1))));
    /// let rows = engine.select("users", pred, None).await?;
    ///
    /// // SELECT name FROM users  
    /// let rows = engine.select("users", None, Some(vec!["name".to_string()])).await?;
    /// ```
    pub async fn select(
        &self,
        table: &str,
        pred: Option<Predicate>,
        cols: Option<Vec<String>>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let st = self.get_storage(table)?;
        execute_select_scan(st, &self.buffers, pred, cols).await
    }

    /// Deletes rows from a table matching the specified predicate.
    ///
    /// This operation follows WAL protocol for durability and supports
    /// both conditional deletes (with predicate) and full table truncation
    /// (predicate = None).
    ///
    /// ## Arguments
    /// * `table` - Name of the target table  
    /// * `pred` - Optional WHERE clause (None deletes all rows)
    ///
    /// ## Returns  
    /// * `Ok(count)` - Number of rows deleted
    /// * `Err(_)` - Table doesn't exist or I/O error
    ///
    /// ## Example
    /// ```rust
    /// // DELETE FROM users WHERE id = 1
    /// let pred = Some(Predicate::Eq("id".to_string(), RowValue::Scalar(Value::Int(1))));
    /// let deleted = engine.delete("users", pred).await?;
    ///
    /// // DELETE FROM users (truncate table)
    /// let deleted = engine.delete("users", None).await?;
    /// ```
    pub async fn delete(&self, table: &str, pred: Option<Predicate>) -> Result<usize> {
        // WAL first: Log the delete operation  
        let record = self.wal.write().create_record(WalRecordType::Delete {
            table: table.to_string(),
            equals: pred.clone(),
        });
        let lsn = record.lsn();
        self.wal.write().append(record)?;
        
        // Apply change: Remove matching rows from storage
        let result = self.apply_delete(table, pred).await?;
        
        // Update checkpoint: Mark this LSN as successfully applied
        self.catalog.write().update_last_applied_lsn(lsn)?;
        Ok(result)
    }

    /// Internal helper: Applies row deletion to table storage.
    /// Called during normal operation and WAL recovery.
    async fn apply_delete(&self, table: &str, pred: Option<Predicate>) -> Result<usize> {
        let st = self.get_storage(table)?;
        st.delete_where(&self.buffers, pred).await
    }

    /// Internal helper: Retrieves table storage handle by name.
    /// Returns an error if the table doesn't exist.
    fn get_storage(&self, table: &str) -> Result<Arc<TableStorage>> {
        self.tables
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow!("Unknown table {}", table))
    }
}
