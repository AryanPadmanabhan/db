use anyhow::Result;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    sync::Arc,
};

use super::storage::{PageId, PageOnDisk, TableStorage};

/// Unique identifier for a page in the buffer pool cache.
///
/// The buffer pool manages pages from multiple tables simultaneously,
/// so each cached page needs a compound key containing both the table
/// name and the page ID within that table.
///
/// ## Hash-Based Lookups
/// `BufferKey` implements `Hash` and `Eq` to enable efficient
/// hash-based lookups in the LRU cache. The hash combines both
/// the table name and page ID for uniqueness across tables.
///
/// ## Example
/// ```rust
/// let key = BufferKey {
///     table: "users".to_string(),
///     page_id: PageId(42),
/// };
/// // This key uniquely identifies page 42 of the "users" table
/// ```
#[derive(Clone, Debug)]
pub struct BufferKey {
    /// Name of the table this page belongs to
    pub table: String,
    /// Page identifier within the table
    pub page_id: PageId,
}
impl PartialEq for BufferKey {
    /// Compares two buffer keys for equality.
    /// Keys are equal if both table name and page ID match.
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table && self.page_id == other.page_id
    }
}
impl Eq for BufferKey {}
impl std::hash::Hash for BufferKey {
    /// Computes hash value combining table name and page ID.
    /// This enables efficient HashMap/LRU cache lookups.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table.hash(state);
        self.page_id.0.hash(state);
    }
}

/// A page cached in the buffer pool with metadata for cache management.
///
/// Each `BufferPage` contains the actual page data plus a dirty flag
/// to track whether the page has been modified since loading from disk.
/// Both fields use `RwLock` for thread-safe concurrent access.
///
/// ## Dirty Flag Management
/// - `dirty = false`: Page matches disk contents, safe to evict
/// - `dirty = true`: Page has been modified, must be written before eviction
///
/// ## Concurrency
/// Multiple threads can read from the same page simultaneously via
/// `data.read()`, but writes require exclusive access via `data.write()`.
///
/// ## Example
/// ```rust
/// // Read page data (shared access)
/// let page_data = buffer_page.data.read();
/// println!("Page has {} rows", page_data.rows.len());
/// 
/// // Modify page (exclusive access)
/// {
///     let mut page_data = buffer_page.data.write();
///     page_data.rows.push(new_row);
///     *buffer_page.dirty.write() = true; // Mark as dirty
/// }
/// ```
pub struct BufferPage {
    /// The actual page data from disk, protected by RwLock for concurrency
    pub data: RwLock<PageOnDisk>,
    /// Dirty flag indicating whether page has been modified (needs write-back)
    pub dirty: RwLock<bool>,
}

/// LRU (Least Recently Used) buffer pool for caching database pages in memory.
///
/// The buffer pool sits between the storage layer and the database engine,
/// providing fast access to frequently used pages while automatically
/// managing memory usage through LRU eviction.
///
/// ## Architecture
/// 
/// ```text
/// Engine Request → BufferPool.get_or_load() → Check LRU Cache
///      ↑                                           ↓
///   Return Page ←←← Insert New Page ←←← Cache Miss: Load from Disk
///                        ↓
///                 LRU Full? → Evict LRU Page → Write if Dirty
/// ```
///
/// ## Key Features
/// - **LRU Eviction**: Automatically removes least recently used pages
/// - **Dirty Tracking**: Only writes modified pages back to disk
/// - **Thread Safety**: Concurrent access protected by Mutex
/// - **Hash-based Lookup**: O(1) average case page retrieval
/// - **Reference Counting**: Uses `Arc<BufferPage>` for shared ownership
///
/// ## Memory Management
/// The buffer pool has a fixed capacity (e.g., 64 pages). When full,
/// adding a new page causes the least recently used page to be evicted.
/// Dirty pages are automatically written to disk during eviction.
///
/// ## Performance Impact
/// - **Cache Hit**: ~microseconds (in-memory access)
/// - **Cache Miss**: ~milliseconds (disk I/O required)
/// - **Eviction**: ~milliseconds if dirty page needs write-back
pub struct BufferPool {
    /// Maximum number of pages to keep in memory
    cap: usize,
    /// LRU cache mapping hash(BufferKey) -> (key, page)
    /// Uses Arc<BufferPage> for cheap cloning and shared ownership
    cache: Mutex<LruCache<u64, (BufferKey, Arc<BufferPage>)>>,
}

impl BufferPool {
    /// Creates a new buffer pool with the specified capacity.
    ///
    /// The capacity determines the maximum number of pages that can be
    /// cached in memory simultaneously. Once this limit is reached,
    /// the LRU eviction policy takes effect.
    ///
    /// ## Capacity Guidelines
    /// - **Small databases**: 16-64 pages (64KB-256KB)
    /// - **Medium databases**: 128-512 pages (512KB-2MB)  
    /// - **Large databases**: 1024+ pages (4MB+)
    ///
    /// ## Arguments
    /// * `cap_pages` - Maximum number of pages to cache (must be > 0)
    ///
    /// ## Example
    /// ```rust
    /// let buffer_pool = BufferPool::new(64); // Cache up to 64 pages (256KB)
    /// ```
    pub fn new(cap_pages: usize) -> Self {
        Self {
            cap: cap_pages,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(cap_pages).unwrap())),
        }
    }

    /// Computes a hash value for a buffer key for cache lookup.
    ///
    /// This internal helper function provides consistent hashing
    /// of `BufferKey` objects for use in the LRU cache. The hash
    /// combines both table name and page ID.
    ///
    /// ## Arguments
    /// * `key` - Buffer key to hash
    ///
    /// ## Returns
    /// 64-bit hash value for cache indexing
    fn key_hash(key: &BufferKey) -> u64 {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        h.finish()
    }

    /// Retrieves a page from the buffer pool, loading from disk if necessary.
    ///
    /// This is the primary interface for accessing pages through the buffer pool.
    /// It implements a cache-first strategy:
    /// 1. **Cache Hit**: Return existing page from memory (fast path)
    /// 2. **Cache Miss**: Load page from disk and cache it (slow path)
    ///
    /// ## Cache Hit Path
    /// If the page is already in memory, this method returns immediately
    /// with the cached page. The LRU cache is updated to mark this page
    /// as recently used.
    ///
    /// ## Cache Miss Path
    /// If the page is not cached:
    /// 1. Load page from table storage (disk I/O)
    /// 2. Wrap in `BufferPage` with `dirty = false`
    /// 3. Insert into cache (may trigger LRU eviction)
    /// 4. Return the newly cached page
    ///
    /// ## Arguments
    /// * `table` - Table storage to load from (if cache miss)
    /// * `page_id` - Page identifier within the table
    ///
    /// ## Returns
    /// * `Ok((key, page))` - Buffer key and cached page (Arc for sharing)
    /// * `Err(_)` - Disk I/O error during page loading
    ///
    /// ## Example
    /// ```rust
    /// let (key, page) = buffer_pool.get_or_load(&table_storage, PageId(0)).await?;
    /// let page_data = page.data.read(); // Access cached page data
    /// ```
    pub async fn get_or_load(
        &self,
        table: &TableStorage,
        page_id: PageId,
    ) -> Result<(BufferKey, Arc<BufferPage>)> {
        let key = BufferKey {
            table: table.name.clone(),
            page_id,
        };
        let h = Self::key_hash(&key);

        if let Some((k, v)) = self.cache.lock().get(&h) {
            if *k == key {
                return Ok((k.clone(), v.clone()));
            }
        }

        let mut page = table.read_page(page_id).await?;
        let buf = Arc::new(BufferPage {
            data: RwLock::new(page.take()),
            dirty: RwLock::new(false),
        });
        self.insert_with_eviction(table, key.clone(), buf).await?;

        let mut lock = self.cache.lock();
        let (_, v) = lock.get(&h).expect("just inserted");
        Ok((key, v.clone()))
    }

    /// Inserts a page into the cache with LRU eviction if necessary.
    ///
    /// This internal method handles the complex logic of adding pages to
    /// a full cache. It implements write-back caching: dirty pages are
    /// only written to disk when evicted, not on every modification.
    ///
    /// ## Eviction Algorithm
    /// 1. **Check capacity**: If cache is full, proceed with eviction
    /// 2. **Select victim**: Remove least recently used page
    /// 3. **Write-back**: If victim page is dirty, write to disk
    /// 4. **Insert new**: Add the new page to cache
    ///
    /// ## Concurrency Considerations
    /// The method carefully manages lock ordering to prevent deadlocks:
    /// - Acquire cache lock to identify eviction candidate
    /// - Release cache lock before disk I/O (avoid blocking other threads)
    /// - Reacquire cache lock to insert new page
    ///
    /// ## Arguments
    /// * `table` - Table storage for writing evicted pages
    /// * `key` - Buffer key for the new page
    /// * `page` - Page to insert into cache
    ///
    /// ## Returns
    /// * `Ok(())` - Page successfully inserted
    /// * `Err(_)` - Disk I/O error during eviction write-back
    async fn insert_with_eviction(
        &self,
        table: &TableStorage,
        key: BufferKey,
        page: Arc<BufferPage>,
    ) -> Result<()> {
        let h = Self::key_hash(&key);
        
        // Check if we need to evict and collect eviction data
        let eviction_info = {
            let mut cache = self.cache.lock();
            if cache.len() >= self.cap {
                if let Some((_old_h, (old_key, old_page))) = cache.pop_lru() {
                    if *old_page.dirty.read() {
                        let data = old_page.data.read().clone();
                        Some((old_key.page_id, data))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }; // Cache lock is dropped here
        
        // Perform eviction write outside the lock
        if let Some((page_id, data)) = eviction_info {
            table.write_page(page_id, data).await?;
        }
        
        // Insert the new page
        let mut cache = self.cache.lock();
        cache.put(h, (key, page));
        Ok(())
    }

    /// Flushes all dirty pages for a specific table to disk.
    ///
    /// This method writes all modified pages for the given table back to
    /// disk storage, ensuring that recent changes are persisted. After
    /// flushing, the dirty flags are cleared since pages match disk state.
    ///
    /// ## Use Cases
    /// - **Before SELECT**: Ensure all recent writes are visible
    /// - **Before DELETE**: Ensure consistent view of data
    /// - **Transaction commit**: Persist all changes
    /// - **Checkpoint operations**: Reduce recovery time
    ///
    /// ## Implementation Details
    /// 1. **Collect dirty pages**: Scan cache for dirty pages of this table
    /// 2. **Clear dirty flags**: Mark pages as clean (prevent redundant writes)
    /// 3. **Batch write**: Write all dirty pages to disk in sequence
    ///
    /// ## Concurrency
    /// The cache lock is held only during the collection phase, then
    /// released before disk I/O to allow concurrent access.
    ///
    /// ## Arguments
    /// * `table` - Table storage to flush pages to
    ///
    /// ## Returns
    /// * `Ok(())` - All dirty pages successfully written
    /// * `Err(_)` - Disk I/O error during flush
    ///
    /// ## Example
    /// ```rust
    /// // Ensure all recent changes to "users" table are on disk
    /// buffer_pool.flush_all_for(&users_table_storage).await?;
    /// ```
    pub async fn flush_all_for(&self, table: &TableStorage) -> Result<()> {
        let mut to_flush = Vec::new();
        {
            let cache = self.cache.lock();
            for (_h, (k, p)) in cache.iter() {
                if k.table == table.name && *p.dirty.read() {
                    to_flush.push((k.page_id, p.data.read().clone()));
                    *p.dirty.write() = false;
                }
            }
        }
        for (pid, data) in to_flush {
            table.write_page(pid, data).await?;
        }
        Ok(())
    }
}
