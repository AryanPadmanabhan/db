use anyhow::Result;
use std::collections::HashMap;

use crate::engine::{BufferPool, Predicate, TableStorage, Value};

/// Executes a SELECT query with full table scan, filtering, and projection.
///
/// This function implements the query execution pipeline for SELECT statements:
/// 1. **Data Visibility**: Flush buffer pool to ensure all recent writes are visible
/// 2. **Table Scan**: Read all pages and rows from the table
/// 3. **Filtering**: Apply WHERE clause predicates to select matching rows
/// 4. **Projection**: Select specific columns (if specified) or return all columns
///
/// ## Query Processing Pipeline
/// ```text
/// Table Pages → Row Scan → Predicate Filter → Column Projection → Results
///      ↑             ↑            ↑                 ↑               ↑
///  All pages     All rows    WHERE clause      SELECT list    Final rows
/// ```
///
/// ## Performance Characteristics
/// - **Full Table Scan**: O(n) where n = total number of rows
/// - **No Indexes**: Currently scans every row (future: add B-tree indexes)
/// - **Memory Efficient**: Processes one page at a time
/// - **Buffer Pool**: Leverages cached pages for better performance
///
/// ## Supported WHERE Clauses
/// - **Equality**: `column = literal`
/// - **Logical AND**: `expr1 AND expr2`
/// - **No predicate**: Returns all rows
///
/// ## Column Projection
/// - **None**: Return all columns (SELECT *)
/// - **Some(cols)**: Return only specified columns (SELECT col1, col2)
///
/// ## Arguments
/// * `table` - Table storage to scan (Arc for shared ownership)
/// * `buffers` - Buffer pool for page caching and write visibility
/// * `pred` - Optional WHERE clause predicate for row filtering
/// * `cols` - Optional column list for projection (None = SELECT *)
///
/// ## Returns
/// * `Ok(rows)` - Vector of matching rows as column→value HashMaps
/// * `Err(_)` - I/O errors during table scan
///
/// ## Example
/// ```rust
/// // SELECT name FROM users WHERE id = 1
/// let pred = Some(Predicate::Eq("id".to_string(), RowValue::Scalar(Value::Int(1))));
/// let cols = Some(vec!["name".to_string()]);
/// let results = execute_select_scan(table, buffers, pred, cols).await?;
/// ```
pub async fn execute_select_scan(
    table: std::sync::Arc<TableStorage>,
    buffers: &BufferPool,
    pred: Option<Predicate>,
    cols: Option<Vec<String>>,
) -> Result<Vec<HashMap<String, Value>>> {
    // Flush buffer pool to ensure all writes are visible
    buffers.flush_all_for(&table).await?;
    
    let mut out = Vec::new();
    let scan = table.scan().await?;
    for (_pid, page) in scan {
        for row in page.rows {
            if matches_pred(&row, pred.as_ref()) {
                if let Some(proj) = &cols {
                    let mut m = HashMap::new();
                    for c in proj {
                        if let Some(v) = row.get(c) {
                            m.insert(c.clone(), v.clone());
                        }
                    }
                    out.push(m);
                } else {
                    out.push(row);
                }
            }
        }
    }
    Ok(out)
}

/// Evaluates a WHERE clause predicate against a single row.
///
/// This function recursively evaluates predicates to determine if a row
/// should be included in the query results. It supports the basic predicate
/// types currently available in the database.
///
/// ## Predicate Evaluation Rules
/// - **No predicate**: Always returns true (no filtering)
/// - **Equality**: Compares column value with literal (exact match)
/// - **Logical AND**: Both sub-predicates must be true
///
/// ## Type Safety
/// The function performs exact value matching using `PartialEq`. Type
/// mismatches (e.g., comparing Int column to Text literal) will return false.
///
/// ## Future Extensions
/// Additional predicate types can be added:
/// - **Inequality**: `>`, `<`, `>=`, `<=`, `!=`
/// - **Logical OR**: `expr1 OR expr2`
/// - **Pattern matching**: `LIKE`, `ILIKE`
/// - **Range queries**: `BETWEEN`, `IN`
///
/// ## Arguments
/// * `row` - Row data as column→value HashMap
/// * `pred` - Optional predicate to evaluate (None = no filtering)
///
/// ## Returns
/// * `true` - Row matches the predicate (should be included)
/// * `false` - Row doesn't match (should be filtered out)
///
/// ## Example
/// ```rust
/// let row = HashMap::from([("id".to_string(), Value::Int(42))]);
/// let pred = Predicate::Eq("id".to_string(), RowValue::Scalar(Value::Int(42)));
/// assert!(matches_pred(&row, Some(&pred))); // true - exact match
/// ```
fn matches_pred(row: &HashMap<String, Value>, pred: Option<&Predicate>) -> bool {
    match pred {
        None => true,
        Some(Predicate::Eq(col, super::RowValue::Scalar(v))) => row.get(col) == Some(v),
        Some(Predicate::And(l, r)) => matches_pred(row, Some(l)) && matches_pred(row, Some(r)),
    }
}
