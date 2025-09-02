use anyhow::{anyhow, Result};
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::{Engine, Predicate, RowValue, Value};

/// Parses and executes a SQL statement against the database engine.
///
/// This is the main entry point for SQL processing. It handles the complete
/// pipeline from SQL text to execution results:
///
/// ## Processing Pipeline
/// 1. **Lexing & Parsing**: Uses `sqlparser` crate to generate AST
/// 2. **Validation**: Ensures single statement and supported operations
/// 3. **Planning**: Converts AST to engine operations
/// 4. **Execution**: Calls appropriate engine methods
/// 5. **Formatting**: Returns results as formatted strings
///
/// ## Supported SQL Statements
///
/// ### DDL (Data Definition Language)
/// - `CREATE TABLE name (col1 INT, col2 TEXT, ...)`
///
/// ### DML (Data Manipulation Language)  
/// - `INSERT INTO table (col1, col2) VALUES (val1, val2), ...`
/// - `SELECT col1, col2 FROM table [WHERE conditions]`
/// - `DELETE FROM table [WHERE conditions]`
///
/// ### WHERE Clause Support
/// - Equality: `column = literal`
/// - Logical AND: `col1 = val1 AND col2 = val2`
/// - Literals: integers (`42`) and strings (`'text'`)
///
/// ## Arguments
/// * `engine` - Database engine instance to execute against
/// * `sql` - SQL statement text (must end with semicolon)
///
/// ## Returns
/// * `Ok(String)` - Formatted result:
///   - DDL: `"CREATE TABLE"`, etc.
///   - INSERT: `"INSERT n"` (row count)
///   - SELECT: JSON array of matching rows
///   - DELETE: `"DELETE n"` (row count)
/// * `Err(_)` - Parse error, unsupported statement, or execution failure
///
/// ## Example Usage
/// ```rust
/// let result = plan_and_exec(engine, "SELECT * FROM users WHERE id = 1;").await?;
/// println!("{}", result); // JSON array: [{"id": {"Int": 1}, "name": {"Text": "Alice"}}]
/// ```
pub async fn plan_and_exec(engine: Arc<Engine>, sql: &str) -> Result<String> {
    // Parse SQL into Abstract Syntax Tree (AST)
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql)?;
    if ast.len() != 1 {
        return Err(anyhow!("Only one statement at a time is supported"));
    }
    let stmt = ast.pop().unwrap();

    match stmt {
        // CREATE TABLE foo (id INT, name TEXT);
        Statement::CreateTable { name, columns, .. } => {
            let tname = name.0.last().unwrap().value.clone();
            let mut cols: Vec<(String, Value)> = Vec::new();
            for c in columns {
                let cname = c.name.value;
                let ty = match c.data_type {
                    DataType::Int(_) | DataType::Integer(_) => Value::Int(0),
                    DataType::Text | DataType::Varchar(_) | DataType::Char(_) => {
                        Value::Text(String::new())
                    }
                    _ => return Err(anyhow!("Unsupported type for column {}", cname)),
                };
                cols.push((cname, ty));
            }
            engine.create_table(&tname, cols).await?;
            Ok("CREATE TABLE".to_string())
        }

        // INSERT INTO foo (id, name) VALUES (1,'Ada'),(2,'Linus');
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            let tname = table_name.0.last().unwrap().value.clone();

            let query = source.ok_or_else(|| anyhow!("INSERT requires a source"))?;
            let body = query.body.as_ref();

            let mut rows = Vec::<HashMap<String, Value>>::new();
            match body {
                SetExpr::Values(values) => {
                    for row in &values.rows {
                        let mut map = HashMap::new();
                        for (i, expr) in row.iter().enumerate() {
                            let col = if i < columns.len() {
                                columns[i].value.clone()
                            } else {
                                return Err(anyhow!("Column count mismatch"));
                            };
                            let v = match expr {
                                Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                                    Value::Int(n.parse::<i64>()?)
                                }
                                Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                                    Value::Text(s.clone())
                                }
                                _ => return Err(anyhow!("Unsupported literal in VALUES")),
                            };
                            map.insert(col, v);
                        }
                        rows.push(map);
                    }
                }
                _ => return Err(anyhow!("INSERT supports VALUES only")),
            }
            let n = engine.insert_rows(&tname, rows).await?;
            Ok(format!("INSERT {}", n))
        }

        // SELECT ... FROM foo WHERE col = literal [AND ...]
        Statement::Query(q) => {
            let body = q.body.as_ref();
            match body {
                SetExpr::Select(select_box) => {
                    let Select {
                        from,
                        projection,
                        selection,
                        ..
                    } = select_box.as_ref();

                    if from.len() != 1 {
                        return Err(anyhow!("Only single table SELECT supported"));
                    }
                    let table_name = match &from[0].relation {
                        TableFactor::Table { name, .. } => {
                            name.0.last().unwrap().value.clone()
                        }
                        _ => return Err(anyhow!("Unsupported FROM clause")),
                    };

                    // WHERE
                    let pred = parse_where(selection.clone())?;

                    // projection
                    let mut cols = Vec::new();
                    for item in projection {
                        match item {
                            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                                cols.push(id.value.clone())
                            }
                            SelectItem::Wildcard(_) => {
                                cols.clear();
                                break;
                            }
                            _ => {
                                return Err(anyhow!(
                                    "Only SELECT col1, col2 or * supported"
                                ))
                            }
                        }
                    }

                    let rows = engine
                        .select(
                            &table_name,
                            pred,
                            if cols.is_empty() { None } else { Some(cols) },
                        )
                        .await?;
                    Ok(serde_json::to_string_pretty(&rows)?)
                }
                _ => Err(anyhow!("Unsupported query body")),
            }
        }

        // DELETE FROM foo WHERE ...
        Statement::Delete { from, selection, .. } => {
            // sqlparser 0.43: `from` is Vec<TableWithJoins>
            if from.len() != 1 {
                return Err(anyhow!("Only single table DELETE supported"));
            }
            let tname = match &from[0].relation {
                TableFactor::Table { name, .. } => name.0.last().unwrap().value.clone(),
                _ => return Err(anyhow!("Unsupported DELETE FROM target")),
            };
            let pred = parse_where(selection)?;
            let n = engine.delete(&tname, pred).await?;
            Ok(format!("DELETE {}", n))
        }

        _ => Err(anyhow!("Unsupported statement")),
    }
}

/// Parses an optional WHERE clause expression into a database predicate.
///
/// This helper function serves as the entry point for WHERE clause processing,
/// handling the common case where WHERE clauses are optional in SQL statements.
/// It delegates to `convert_expr()` for the actual AST-to-predicate conversion.
///
/// ## Processing Logic
/// - **No WHERE clause**: Returns `Ok(None)` (no filtering)
/// - **Has WHERE clause**: Attempts to convert the expression to a predicate
///
/// ## Arguments
/// * `selection` - Optional WHERE clause AST from sqlparser
///
/// ## Returns
/// * `Ok(None)` - No WHERE clause (no filtering applied)
/// * `Ok(Some(predicate))` - Valid WHERE clause converted to predicate
/// * `Err(_)` - Unsupported WHERE clause syntax
///
/// ## Example
/// ```rust
/// // SQL: SELECT * FROM users (no WHERE clause)
/// let result = parse_where(None)?; // Returns Ok(None)
///
/// // SQL: SELECT * FROM users WHERE id = 1
/// let where_expr = /* parsed WHERE clause */;
/// let result = parse_where(Some(where_expr))?; // Returns Ok(Some(predicate))
/// ```
fn parse_where(selection: Option<Expr>) -> Result<Option<Predicate>> {
    match selection {
        None => Ok(None),
        Some(expr) => convert_expr(expr),
    }
}

/// Converts a SQL expression AST into a database predicate.
///
/// This function recursively processes SQL expressions from the sqlparser AST
/// and converts them into the database's internal `Predicate` representation.
/// It handles the core WHERE clause operations supported by the database.
///
/// ## Supported Expressions
///
/// ### Equality Comparisons
/// - **Integer literals**: `column = 42`
/// - **String literals**: `column = 'text'`
/// - **Column on left**: Only `column = literal` format supported
///
/// ### Logical AND
/// - **Binary AND**: `expr1 AND expr2`
/// - **Nested expressions**: Recursively processes both sides
/// - **Null handling**: Gracefully handles None results from sub-expressions
///
/// ## Limitations
/// Current implementation is intentionally minimal. Future extensions:
/// - **More operators**: `>`, `<`, `>=`, `<=`, `!=`, `LIKE`
/// - **Logical OR**: `expr1 OR expr2`
/// - **Literal on left**: `42 = column`
/// - **Column comparisons**: `col1 = col2`
/// - **Functions**: `UPPER(column) = 'VALUE'`
///
/// ## Error Handling
/// Returns descriptive errors for unsupported constructs to guide
/// users toward supported SQL syntax.
///
/// ## Arguments
/// * `expr` - SQL expression AST from sqlparser
///
/// ## Returns
/// * `Ok(Some(predicate))` - Successfully converted expression
/// * `Ok(None)` - Empty/null expression (edge case)
/// * `Err(_)` - Unsupported expression type or format
///
/// ## Example
/// ```rust
/// // SQL: WHERE id = 42 AND name = 'Alice'
/// let ast = /* parsed binary AND expression */;
/// let predicate = convert_expr(ast)?;
/// // Returns: Predicate::And(
/// //   Box::new(Predicate::Eq("id", RowValue::Scalar(Value::Int(42)))),
/// //   Box::new(Predicate::Eq("name", RowValue::Scalar(Value::Text("Alice"))))
/// // )
/// ```
fn convert_expr(expr: Expr) -> Result<Option<Predicate>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let (col, val) = match (*left, *right) {
                    (Expr::Identifier(id), Expr::Value(sqlparser::ast::Value::Number(n, _))) => {
                        (id.value, Value::Int(n.parse::<i64>()?))
                    }
                    (Expr::Identifier(id), Expr::Value(sqlparser::ast::Value::SingleQuotedString(s))) => {
                        (id.value, Value::Text(s))
                    }
                    _ => return Err(anyhow!("Only col = literal supported")),
                };
                Ok(Some(Predicate::Eq(col, RowValue::Scalar(val))))
            }
            BinaryOperator::And => {
                let l = convert_expr(*left)?;
                let r = convert_expr(*right)?;
                Ok(match (l, r) {
                    (Some(lp), Some(rp)) => Some(Predicate::And(Box::new(lp), Box::new(rp))),
                    (Some(p), None) | (None, Some(p)) => Some(p),
                    _ => None,
                })
            }
            _ => Err(anyhow!("Only = and AND supported in WHERE")),
        },
        _ => Err(anyhow!("Unsupported WHERE expression")),
    }
}
