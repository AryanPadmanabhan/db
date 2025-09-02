use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};
use tokio::fs as tfs;

use crate::engine::{Predicate, RowValue, Value};

/// Standard database page size (4KB) used for storage allocation and buffer management.
/// This matches common database systems and filesystem block sizes for optimal I/O.
pub const PAGE_SIZE: usize = 4096;

/// Unique identifier for a database page within a table file.
/// 
/// Pages are numbered sequentially starting from 0. Each table maintains
/// its own PageId sequence. Used as keys in the buffer pool cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PageId(pub u64);

/// Represents a database page as stored on disk with serialized row data.
///
/// Pages contain variable numbers of rows up to the PAGE_SIZE limit.
/// When a page exceeds the size limit, a new page is created.
/// 
/// ## Serialization
/// Pages are serialized using `bincode` for efficient binary storage.
/// The entire page (including all rows) must fit within PAGE_SIZE bytes.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PageOnDisk {
    /// All rows stored in this page as column-name -> value mappings
    pub rows: Vec<HashMap<String, Value>>,
}

impl PageOnDisk {
    /// Creates an empty page with no rows.
    /// Used when initializing new table files or splitting full pages.
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }
    
    /// Moves all rows out of this page, leaving it empty.
    /// This is an efficient way to transfer rows without cloning.
    /// Used during buffer pool operations and page splits.
    pub fn take(&mut self) -> Self {
        PageOnDisk {
            rows: std::mem::take(&mut self.rows),
        }
    }
}

/// Manages persistent storage for a single database table.
///
/// Each table is stored as a single file containing serialized pages.
/// The file format is a vector of `PageOnDisk` structures serialized
/// with `bincode`. Pages are loaded on-demand through the buffer pool.
///
/// ## File Structure
/// ```text
/// table_name.tbl:
/// [Page0][Page1][Page2]...[PageN]
///   |      |      |        |
///   |      |      |        └─ Serialized PageOnDisk
///   |      |      └─ Serialized PageOnDisk  
///   |      └─ Serialized PageOnDisk
///   └─ Serialized PageOnDisk
/// ```
///
/// ## Concurrency
/// TableStorage is thread-safe when used with the buffer pool.
/// Multiple readers can access different pages concurrently.
#[derive(Clone)]
pub struct TableStorage {
    /// Directory containing all table files
    pub dir: PathBuf,
    /// Table name (used for file naming)
    pub name: String,
    /// Full path to the table file (dir/name.tbl)
    path: PathBuf,
}

impl TableStorage {
    /// Opens an existing table storage or creates a new one if it doesn't exist.
    ///
    /// This method initializes the table storage by:
    /// 1. Creating the storage directory if it doesn't exist
    /// 2. Creating an empty table file if this is a new table
    /// 3. Setting up the file path for future operations
    ///
    /// ## Arguments
    /// * `dir` - Directory where table files are stored
    /// * `name` - Table name (will create `name.tbl` file)
    ///
    /// ## File Format
    /// New tables are initialized with an empty vector of pages
    /// serialized using `bincode`.
    ///
    /// ## Returns
    /// * `Ok(TableStorage)` - Ready-to-use table storage handle
    /// * `Err(_)` - File I/O errors or serialization failures
    pub fn open(dir: PathBuf, name: &str) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.tbl", name));
        
        // Create empty table file if it doesn't exist
        if !path.exists() {
            let empty_pages: Vec<PageOnDisk> = Vec::new();
            fs::write(&path, bincode::serialize(&empty_pages)?)?;
        }
        
        Ok(Self {
            dir,
            name: name.to_string(),
            path,
        })
    }

    /// Reads a specific page from the table file.
    ///
    /// This method loads the entire table file, deserializes all pages,
    /// and returns the requested page. If the page doesn't exist,
    /// returns an empty page.
    ///
    /// ## Performance Note
    /// This method reads the entire file each time, which is inefficient
    /// for large tables. The buffer pool should be used to cache pages.
    ///
    /// ## Arguments
    /// * `pid` - Page identifier to read
    ///
    /// ## Returns
    /// * `Ok(PageOnDisk)` - The requested page (or empty if doesn't exist)
    /// * `Err(_)` - File I/O or deserialization errors
    pub async fn read_page(&self, pid: PageId) -> Result<PageOnDisk> {
        let data = tfs::read(&self.path).await?;
        let pages: Vec<PageOnDisk> = bincode::deserialize(&data)?;
        if (pid.0 as usize) < pages.len() {
            Ok(pages[pid.0 as usize].clone())
        } else {
            Ok(PageOnDisk::empty())
        }
    }

    /// Writes a page to a specific position in the table file.
    /// Expands the file with empty pages if necessary.
    pub async fn write_page(&self, pid: PageId, page: PageOnDisk) -> Result<()> {
        let data = tfs::read(&self.path).await.unwrap_or_default();
        let mut pages: Vec<PageOnDisk> =
            if data.is_empty() { Vec::new() } else { bincode::deserialize(&data)? };
        let idx = pid.0 as usize;
        if idx >= pages.len() {
            pages.resize(idx + 1, PageOnDisk::empty());
        }
        pages[idx] = page;
        let bytes = bincode::serialize(&pages)?;
        tfs::write(&self.path, bytes).await?;
        Ok(())
    }

    /// Appends a new row to the table, using buffer pool for efficient page management.
    /// Creates new pages automatically when current page exceeds PAGE_SIZE.
    pub async fn append_row(
        &self,
        buffers: &crate::engine::BufferPool,
        row: HashMap<String, Value>,
    ) -> Result<()> {
        // Find the last page ID by reading the file size info
        let last_pid = self.get_last_page_id().await?;
        
        // Get or load the last page through buffer pool
        let (_buf_key, buf_page) = buffers.get_or_load(self, last_pid).await?;
        
        // Try to add row to current page
        let needs_new_page = {
            let mut page_data = buf_page.data.write();
            page_data.rows.push(row.clone());
            
            // Check if page is now too large
            if bincode::serialize(&*page_data)?.len() > PAGE_SIZE {
                // Remove the row we just added
                page_data.rows.pop();
                *buf_page.dirty.write() = true; // Mark current page as dirty
                true // Need to create new page
            } else {
                *buf_page.dirty.write() = true; // Mark page as dirty
                false
            }
        }; // Guards are dropped here
        
        if needs_new_page {
            // Create new page with the row
            let new_pid = PageId(last_pid.0 + 1);
            let mut new_page = PageOnDisk::empty();
            new_page.rows.push(row);
            
            // Write the new page directly to extend the file
            self.write_page(new_pid, new_page).await?;
        }
        
        Ok(())
    }

    /// Finds the last page ID in the table file. Creates first page if table is empty.
    async fn get_last_page_id(&self) -> Result<PageId> {
        let data = tfs::read(&self.path).await.unwrap_or_default();
        if data.is_empty() {
            // No file exists yet, create first page
            let empty_pages: Vec<PageOnDisk> = vec![PageOnDisk::empty()];
            let bytes = bincode::serialize(&empty_pages)?;
            tfs::write(&self.path, bytes).await?;
            Ok(PageId(0))
        } else {
            let pages: Vec<PageOnDisk> = bincode::deserialize(&data)?;
            Ok(PageId(if pages.is_empty() { 0 } else { (pages.len() - 1) as u64 }))
        }
    }

    /// Returns all pages in the table for full table scans.
    /// Used by query executor for SELECT operations.
    pub async fn scan(&self) -> Result<Vec<(PageId, PageOnDisk)>> {
        let data = tfs::read(&self.path).await.unwrap_or_default();
        let pages: Vec<PageOnDisk> =
            if data.is_empty() { Vec::new() } else { bincode::deserialize(&data)? };
        Ok(pages
            .into_iter()
            .enumerate()
            .map(|(i, p)| (PageId(i as u64), p))
            .collect())
    }

    /// Deletes rows matching the given predicate. Flushes buffer pool first
    /// to ensure all recent writes are visible during deletion.
    pub async fn delete_where(
        &self,
        buffers: &crate::engine::BufferPool,
        pred: Option<Predicate>,
    ) -> Result<usize> {
        // For now, still do bulk operations but flush buffer pool first
        buffers.flush_all_for(self).await?;
        
        let data = tfs::read(&self.path).await.unwrap_or_default();
        let mut pages: Vec<PageOnDisk> =
            if data.is_empty() { Vec::new() } else { bincode::deserialize(&data)? };
        let mut removed = 0usize;
        for p in pages.iter_mut() {
            let before = p.rows.len();
            p.rows.retain(|r| !matches_pred(r, pred.as_ref()));
            removed += before - p.rows.len();
        }
        let bytes = bincode::serialize(&pages)?;
        tfs::write(&self.path, bytes).await?;
        Ok(removed)
    }
}

fn matches_pred(row: &HashMap<String, Value>, pred: Option<&Predicate>) -> bool {
    match pred {
        None => true,
        Some(Predicate::Eq(col, RowValue::Scalar(val))) => row.get(col) == Some(val),
        Some(Predicate::And(l, r)) => matches_pred(row, Some(l)) && matches_pred(row, Some(r)),
    }
}

/// Write-Ahead Log (WAL) record representing a single database operation.
///
/// Each WAL record contains an operation type and a unique Log Sequence Number (LSN)
/// for ordering and recovery purposes. Records are serialized to JSON Lines format
/// for human-readable logging and reliable parsing.
///
/// ## LSN-Based Recovery
/// The LSN provides total ordering of all database operations:
/// - LSNs are assigned sequentially (1, 2, 3, ...)
/// - Operations are applied in LSN order during recovery
/// - Checkpointing tracks the highest successfully applied LSN
/// - Recovery only replays records with LSN > checkpoint LSN
///
/// ## Serialization Format
/// Records use `#[serde(tag = "type")]` for JSON discrimination:
/// ```json
/// {"type": "Insert", "lsn": 42, "table": "users", "row": {"id": {"Int": 1}}}
/// ```
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum WalRecord {
    /// DDL operation to create a new table with schema definition.
    /// Contains table name and column specifications for catalog updates.
    CreateTable {
        /// Unique sequence number for this operation
        lsn: u64,
        /// Name of the table to create
        name: String,
        /// Column definitions including names and types
        columns: Vec<crate::engine::Column>,
    },
    /// DML operation to insert a single row into a table.
    /// Contains the target table and row data as column-value pairs.
    Insert {
        /// Unique sequence number for this operation
        lsn: u64,
        /// Name of the target table
        table: String,
        /// Row data as column name -> value mapping
        row: HashMap<String, Value>,
    },
    /// DML operation to delete rows matching a predicate.
    /// If predicate is None, deletes all rows (table truncation).
    Delete {
        /// Unique sequence number for this operation
        lsn: u64,
        /// Name of the target table
        table: String,
        /// Optional WHERE clause predicate (None = delete all)
        equals: Option<crate::engine::Predicate>,
    },
}

impl WalRecord {
    /// Extracts the Log Sequence Number (LSN) from any WAL record type.
    ///
    /// This method provides a uniform interface to access LSNs across all
    /// record variants, which is essential for recovery ordering and
    /// checkpoint management.
    ///
    /// ## Returns
    /// The LSN as a 64-bit unsigned integer
    ///
    /// ## Example
    /// ```rust
    /// let record = WalRecord::Insert { lsn: 42, table: "users".to_string(), row: HashMap::new() };
    /// assert_eq!(record.lsn(), 42);
    /// ```
    pub fn lsn(&self) -> u64 {
        match self {
            WalRecord::CreateTable { lsn, .. } => *lsn,
            WalRecord::Insert { lsn, .. } => *lsn,
            WalRecord::Delete { lsn, .. } => *lsn,
        }
    }
}

/// Template for creating WAL records without pre-assigned LSNs.
///
/// This enum represents the "intent" to create a WAL record before
/// the LSN is assigned. It contains the same data as `WalRecord`
/// but without LSNs, allowing the WAL to assign sequence numbers
/// automatically during record creation.
///
/// ## Usage Pattern
/// ```rust
/// // 1. Create record template
/// let template = WalRecordType::Insert {
///     table: "users".to_string(),
///     row: user_data,
/// };
/// 
/// // 2. WAL assigns LSN and creates final record
/// let record = wal.create_record(template);
/// 
/// // 3. Append to log with durability guarantee
/// wal.append(record)?;
/// ```
#[derive(Clone, Debug)]
pub enum WalRecordType {
    /// Template for CREATE TABLE operations
    CreateTable {
        /// Name of the table to create
        name: String,
        /// Column definitions including names and types
        columns: Vec<crate::engine::Column>,
    },
    /// Template for INSERT operations
    Insert {
        /// Name of the target table
        table: String,
        /// Row data as column name -> value mapping
        row: HashMap<String, Value>,
    },
    /// Template for DELETE operations
    Delete {
        /// Name of the target table
        table: String,
        /// Optional WHERE clause predicate (None = delete all)
        equals: Option<crate::engine::Predicate>,
    },
}

/// Write-Ahead Log (WAL) manager providing durability guarantees for database operations.
///
/// The WAL ensures that all database changes are logged to persistent storage
/// before being applied to data files. This enables crash recovery and ACID
/// transaction semantics.
///
/// ## Architecture
/// 
/// ```text
/// Operation → WAL.create_record() → WAL.append() → fsync → Apply to Storage
///    ↑              ↑                    ↑           ↑            ↑
///  User Request   Assign LSN      Write to Log   Durability   Update Data
/// ```
///
/// ## File Format
/// The WAL file is stored in JSON Lines format (one JSON object per line):
/// ```text
/// wal/log.jsonl:
/// {"type": "CreateTable", "lsn": 1, "name": "users", "columns": [...]}
/// {"type": "Insert", "lsn": 2, "table": "users", "row": {"id": {"Int": 1}}}
/// {"type": "Delete", "lsn": 3, "table": "users", "equals": null}
/// ```
///
/// ## Recovery Process
/// 1. Read checkpoint LSN from catalog
/// 2. Scan WAL file for records with LSN > checkpoint
/// 3. Replay operations in LSN order
/// 4. Update checkpoint to highest applied LSN
///
/// ## Thread Safety
/// The WAL is protected by RwLock in the Engine for concurrent access.
pub struct Wal {
    /// Path to the WAL file (typically "wal/log.jsonl")
    path: PathBuf,
    /// Open file handle for appending records
    file: File,
    /// Next LSN to assign (monotonically increasing)
    next_lsn: u64,
}

impl Wal {
    /// Opens an existing WAL file or creates a new one if it doesn't exist.
    ///
    /// This method initializes the WAL by:
    /// 1. Creating parent directories if needed
    /// 2. Opening file in append mode (preserves existing records)
    /// 3. Scanning existing records to determine next LSN
    /// 4. Setting up for future append operations
    ///
    /// ## LSN Initialization
    /// If the WAL file exists, scans all records to find the highest LSN
    /// and sets `next_lsn = max_lsn + 1`. If no file exists, starts at LSN 1.
    ///
    /// ## Arguments
    /// * `path` - File path for the WAL (e.g., "./data/wal/log.jsonl")
    ///
    /// ## Returns
    /// * `Ok(Wal)` - Ready-to-use WAL instance
    /// * `Err(_)` - File I/O errors or JSON parsing failures
    ///
    /// ## Example
    /// ```rust
    /// let wal = Wal::open(PathBuf::from("./wal/log.jsonl"))?;
    /// // WAL is now ready for create_record() and append() operations
    /// ```
    pub fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        
        // Calculate next LSN by reading existing records
        let next_lsn = if path.exists() {
            let existing_records = Self::read_records_from_path(&path)?;
            existing_records.iter().map(|r| r.lsn()).max().unwrap_or(0) + 1
        } else {
            1
        };
        
        Ok(Self { path, file, next_lsn })
    }

    /// Creates a WAL record with the next sequential LSN.
    ///
    /// This method takes a `WalRecordType` template and assigns the next
    /// available LSN to create a complete `WalRecord`. The LSN counter
    /// is automatically incremented for subsequent operations.
    ///
    /// ## LSN Assignment
    /// LSNs are assigned sequentially starting from 1:
    /// - First record gets LSN 1
    /// - Second record gets LSN 2
    /// - And so on...
    ///
    /// This ordering is critical for crash recovery to ensure operations
    /// are replayed in the exact same order they were originally executed.
    ///
    /// ## Arguments
    /// * `record_type` - Template containing operation data (without LSN)
    ///
    /// ## Returns
    /// Complete `WalRecord` with assigned LSN, ready for `append()`
    ///
    /// ## Example
    /// ```rust
    /// let template = WalRecordType::Insert {
    ///     table: "users".to_string(),
    ///     row: user_data,
    /// };
    /// let record = wal.create_record(template); // Gets next LSN
    /// assert_eq!(record.lsn(), expected_lsn);
    /// ```
    pub fn create_record(&mut self, record_type: WalRecordType) -> WalRecord {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        match record_type {
            WalRecordType::CreateTable { name, columns } => {
                WalRecord::CreateTable { lsn, name, columns }
            }
            WalRecordType::Insert { table, row } => {
                WalRecord::Insert { lsn, table, row }
            }
            WalRecordType::Delete { table, equals } => {
                WalRecord::Delete { lsn, table, equals }
            }
        }
    }

    /// Appends a WAL record to the log file with full durability guarantees.
    ///
    /// This method implements the "write-ahead" part of Write-Ahead Logging:
    /// 1. **Serialize**: Convert record to JSON format
    /// 2. **Write**: Append to WAL file as a single line
    /// 3. **Fsync**: Force operating system to flush to physical storage
    /// 4. **Return**: Only after data is guaranteed durable
    ///
    /// ## Durability Guarantee
    /// The `sync_all()` call ensures that even if the process crashes
    /// immediately after this method returns, the record will survive
    /// and be available during recovery. This is essential for ACID compliance.
    ///
    /// ## Performance Impact
    /// Each append includes an fsync, which can be expensive (1-10ms per call).
    /// Future optimizations could batch multiple records before syncing.
    ///
    /// ## Arguments
    /// * `rec` - Complete WAL record with assigned LSN
    ///
    /// ## Returns
    /// * `Ok(())` - Record is durably stored and recoverable
    /// * `Err(_)` - Serialization or I/O error (operation should be retried)
    ///
    /// ## Example
    /// ```rust
    /// let record = wal.create_record(operation_template);
    /// wal.append(record)?; // Blocks until data is on disk
    /// // Now safe to apply operation to data files
    /// ```
    pub fn append(&mut self, rec: WalRecord) -> Result<()> {
        let line = serde_json::to_string(&rec)?;
        writeln!(self.file, "{}", line)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Internal helper to read and parse all WAL records from a file.
    ///
    /// This method handles the low-level details of WAL file parsing:
    /// - Opens file in read-only mode
    /// - Reads line-by-line (JSON Lines format)
    /// - Skips empty lines (allows for formatting flexibility)
    /// - Deserializes each line as a `WalRecord`
    /// - Collects all records into a vector
    ///
    /// ## File Format Assumptions
    /// - Each line contains exactly one JSON object
    /// - Empty lines are ignored (not treated as errors)
    /// - Malformed JSON lines cause parsing errors
    ///
    /// ## Arguments
    /// * `path` - Path to the WAL file to read
    ///
    /// ## Returns
    /// * `Ok(records)` - All valid records in file order
    /// * `Err(_)` - File I/O or JSON parsing errors
    fn read_records_from_path(path: &PathBuf) -> Result<Vec<WalRecord>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let f = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(f);
        let mut out = Vec::new();
        for line in reader.lines() {
            let l = line?;
            if l.trim().is_empty() {
                continue;
            }
            let rec: WalRecord = serde_json::from_str(&l)?;
            out.push(rec);
        }
        Ok(out)
    }

    /// Reads all WAL records from the log file.
    ///
    /// This method provides complete access to the WAL history,
    /// typically used during engine initialization and full recovery
    /// scenarios. Records are returned in the order they appear
    /// in the file (which matches LSN order).
    ///
    /// ## Use Cases
    /// - Engine startup: Read all records to determine current state
    /// - Full recovery: Replay entire WAL from beginning
    /// - Debugging: Examine complete operation history
    /// - Backup/export: Extract all operations for archival
    ///
    /// ## Performance Note
    /// This method reads the entire file, which may be expensive
    /// for large WAL files. Consider `read_from_lsn()` for partial reads.
    ///
    /// ## Returns
    /// * `Ok(records)` - All WAL records in LSN order
    /// * `Err(_)` - File I/O or parsing errors
    ///
    /// ## Example
    /// ```rust
    /// let all_records = wal.read_all()?;
    /// println!("WAL contains {} operations", all_records.len());
    /// ```
    pub fn read_all(&self) -> Result<Vec<WalRecord>> {
        Self::read_records_from_path(&self.path)
    }

    /// Reads WAL records with LSN greater than the specified value.
    ///
    /// This method is the core of incremental recovery, reading only
    /// the records that need to be replayed since the last checkpoint.
    /// It filters the complete WAL to return just the "uncommitted" operations.
    ///
    /// ## Recovery Usage
    /// ```rust
    /// let checkpoint_lsn = catalog.last_applied_lsn;
    /// let uncommitted = wal.read_from_lsn(checkpoint_lsn)?;
    /// for record in uncommitted {
    ///     apply_operation(record).await?;
    /// }
    /// ```
    ///
    /// ## Filter Logic
    /// Only returns records where `record.lsn() > from_lsn`.
    /// Records with LSN equal to `from_lsn` are considered already applied.
    ///
    /// ## Arguments
    /// * `from_lsn` - LSN threshold (exclusive). Records with LSN ≤ this value are filtered out
    ///
    /// ## Returns
    /// * `Ok(records)` - Filtered records in LSN order, ready for replay
    /// * `Err(_)` - File I/O or parsing errors
    ///
    /// ## Example
    /// ```rust
    /// // Read operations since checkpoint LSN 100
    /// let new_ops = wal.read_from_lsn(100)?;
    /// // Only returns records with LSN 101, 102, 103, etc.
    /// ```
    pub fn read_from_lsn(&self, from_lsn: u64) -> Result<Vec<WalRecord>> {
        let all_records = self.read_all()?;
        Ok(all_records.into_iter().filter(|r| r.lsn() > from_lsn).collect())
    }
}
