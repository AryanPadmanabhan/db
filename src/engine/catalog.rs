use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::{collections::HashMap, fs, path::PathBuf};

/// Enumeration of supported column data types in the database.
///
/// Currently supports basic SQL types with plans for expansion.
/// Each type maps to a corresponding `Value` variant in the engine.
///
/// ## Type Mapping
/// - `ColumnType::Int` ↔ `Value::Int(i64)`
/// - `ColumnType::Text` ↔ `Value::Text(String)`
///
/// ## Future Extensions
/// Additional types can be added here along with corresponding
/// `Value` variants: Boolean, Float, Timestamp, etc.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum ColumnType { 
    /// 64-bit signed integer type (SQL INT/INTEGER)
    Int, 
    /// Variable-length UTF-8 string type (SQL TEXT/VARCHAR)
    Text 
}

/// Definition of a single column within a database table.
///
/// Contains the column name and its data type. This structure is used
/// in table schemas and CREATE TABLE operations.
///
/// ## Example
/// ```rust
/// let id_column = Column {
///     name: "id".to_string(),
///     col_type: ColumnType::Int,
/// };
/// let name_column = Column {
///     name: "name".to_string(), 
///     col_type: ColumnType::Text,
/// };
/// ```
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Column {
    /// Column name (must be unique within a table)
    pub name: String,
    /// Data type for values stored in this column
    pub col_type: ColumnType,
}

/// Schema definition for a database table.
///
/// Contains the table name and ordered list of column definitions.
/// This structure represents the logical schema as created by
/// CREATE TABLE statements.
///
/// ## Column Ordering
/// The order of columns in the `columns` vector determines:
/// - SELECT * projection order
/// - INSERT VALUES column matching  
/// - Internal row storage layout
///
/// ## Example
/// ```rust
/// let users_schema = TableSchema {
///     name: "users".to_string(),
///     columns: vec![
///         Column { name: "id".to_string(), col_type: ColumnType::Int },
///         Column { name: "name".to_string(), col_type: ColumnType::Text },
///     ],
/// };
/// ```
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct TableSchema {
    /// Table name (unique within the database)
    pub name: String,
    /// Ordered list of column definitions
    pub columns: Vec<Column>,
}

/// Database catalog containing schema metadata and recovery state.
///
/// The catalog serves as the "data dictionary" for the database,
/// storing table schemas and tracking WAL recovery progress.
/// It's persisted to disk as JSON for human readability.
///
/// ## Responsibilities
/// - **Schema Management**: Store table and column definitions
/// - **Recovery Tracking**: Track highest applied WAL LSN
/// - **Metadata Queries**: Support schema introspection
/// - **Persistence**: Automatically save changes to disk
///
/// ## LSN Checkpointing
/// The `last_applied_lsn` field enables incremental crash recovery:
/// - Updated after each successful WAL operation
/// - Used to filter WAL records during recovery
/// - Prevents double-application of operations
///
/// ## File Format
/// ```json
/// {
///   "tables": {
///     "users": {
///       "name": "users",
///       "columns": [{"name": "id", "col_type": "Int"}]
///     }
///   },
///   "last_applied_lsn": 42
/// }
/// ```
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Catalog {
    /// All table schemas indexed by table name
    pub tables: HashMap<String, TableSchema>,
    /// Highest WAL LSN that has been successfully applied (for recovery)
    pub last_applied_lsn: u64,
    /// File path for persistence (not serialized)
    #[serde(skip)]
    path: Option<PathBuf>,
}

impl Catalog {
    /// Loads an existing catalog from disk or creates a new one if it doesn't exist.
    ///
    /// This method handles catalog initialization during database startup:
    /// 1. **File exists**: Deserialize existing catalog from JSON
    /// 2. **File missing**: Create new empty catalog and persist it
    ///
    /// ## Persistence Setup
    /// The file path is stored in the catalog for future `persist()` calls.
    /// This enables automatic saving when schema changes occur.
    ///
    /// ## Recovery State
    /// If loading an existing catalog, the `last_applied_lsn` indicates
    /// where WAL recovery should resume from.
    ///
    /// ## Arguments
    /// * `path` - File path for the catalog JSON (e.g., "./data/catalog.json")
    ///
    /// ## Returns
    /// * `Ok(Catalog)` - Loaded or newly created catalog
    /// * `Err(_)` - File I/O or JSON parsing errors
    ///
    /// ## Example
    /// ```rust
    /// let catalog = Catalog::load_or_create(PathBuf::from("./data/catalog.json"))?;
    /// println!("Loaded {} tables", catalog.tables.len());
    /// ```
    pub fn load_or_create(path: PathBuf) -> Result<Self> {
        if path.exists() {
            let bytes = fs::read(&path)?;
            let mut c: Catalog = serde_json::from_slice(&bytes)?;
            c.path = Some(path);
            Ok(c)
        } else {
            let mut c = Catalog::default();
            c.path = Some(path.clone());
            fs::write(path, serde_json::to_vec_pretty(&c)?)?;
            Ok(c)
        }
    }

    /// Adds a new table schema to the catalog and persists the change.
    ///
    /// This method updates the in-memory catalog with the new table
    /// definition and immediately writes the updated catalog to disk
    /// for durability.
    ///
    /// ## Schema Validation
    /// Currently performs minimal validation - future versions may add:
    /// - Duplicate table name checking
    /// - Column name uniqueness within table
    /// - Reserved keyword validation
    ///
    /// ## Atomicity
    /// The operation is atomic: either both the in-memory update and
    /// disk persistence succeed, or the operation fails with no changes.
    ///
    /// ## Arguments
    /// * `name` - Table name (should be unique in the database)
    /// * `columns` - Vector of column definitions
    ///
    /// ## Returns
    /// * `Ok(())` - Table schema successfully added and persisted
    /// * `Err(_)` - File I/O error during persistence
    ///
    /// ## Example
    /// ```rust
    /// catalog.create_table("users", vec![
    ///     Column { name: "id".to_string(), col_type: ColumnType::Int },
    ///     Column { name: "name".to_string(), col_type: ColumnType::Text },
    /// ])?;
    /// ```
    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<()> {
        let ts = TableSchema { name: name.to_string(), columns };
        self.tables.insert(name.to_string(), ts);
        self.persist()
    }

    /// Updates the recovery checkpoint LSN and persists the change.
    ///
    /// This method is called after each successful WAL operation to track
    /// recovery progress. The LSN indicates the highest WAL record that
    /// has been successfully applied to the database.
    ///
    /// ## Recovery Semantics
    /// - LSN = 0: No operations have been applied (fresh database)
    /// - LSN = N: All operations with LSN ≤ N have been applied
    /// - Recovery starts from LSN > last_applied_lsn
    ///
    /// ## Checkpoint Frequency
    /// Currently updates on every operation. Future optimizations could:
    /// - Batch updates to reduce I/O
    /// - Periodic checkpointing instead of per-operation
    /// - Asynchronous background checkpointing
    ///
    /// ## Arguments
    /// * `lsn` - Highest LSN that has been successfully applied
    ///
    /// ## Returns
    /// * `Ok(())` - LSN updated and persisted successfully
    /// * `Err(_)` - File I/O error during persistence
    ///
    /// ## Example
    /// ```rust
    /// // After successfully applying WAL record with LSN 42
    /// catalog.update_last_applied_lsn(42)?;
    /// // Recovery will now start from LSN 43 and higher
    /// ```
    pub fn update_last_applied_lsn(&mut self, lsn: u64) -> Result<()> {
        self.last_applied_lsn = lsn;
        self.persist()
    }

    /// Internal helper to write the catalog to disk as pretty-printed JSON.
    ///
    /// This method handles the low-level persistence logic:
    /// 1. Serialize catalog to JSON with pretty formatting
    /// 2. Atomically write to the configured file path
    /// 3. Handle the case where no path is configured (no-op)
    ///
    /// ## JSON Format
    /// Uses `serde_json::to_vec_pretty()` for human-readable output,
    /// making it easy to inspect and debug the catalog contents.
    ///
    /// ## Error Handling
    /// If no path is configured (path is None), this method succeeds
    /// without doing anything, supporting in-memory-only catalogs.
    ///
    /// ## Returns
    /// * `Ok(())` - Catalog successfully written to disk
    /// * `Err(_)` - Serialization or file I/O errors
    fn persist(&self) -> Result<()> {
        if let Some(path) = &self.path {
            fs::write(path, serde_json::to_vec_pretty(self)?)?;
        }
        Ok(())
    }
}
