use anyhow::Result;
use rust_pg_db::{engine::Engine, server::serve};
use std::sync::Arc;
use clap::{Arg, Command};
use tracing_subscriber::{fmt, EnvFilter};

/// Main entry point for the rust_pg_db database server.
///
/// This function:
/// 1. Parses command-line arguments for data directory and listen address
/// 2. Initializes structured logging with tracing
/// 3. Opens/creates the database engine with LSN-based WAL recovery
/// 4. Starts the TCP server to accept SQL connections
///
/// # Arguments
/// - `--data DIR`: Required data directory for storing catalog, table files, and WAL
/// - `--listen ADDR`: TCP address to bind to (default: 127.0.0.1:54330)
///
/// # Example Usage
/// ```bash
/// cargo run -- --data ./dbdata --listen 127.0.0.1:5432
/// ```
#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let matches = Command::new("rust_pg_db")
        .about("Tiny Postgres-style DB with WAL, SQL, concurrency, and LRU buffer")
        .arg(Arg::new("data")
            .long("data")
            .value_name("DIR")
            .required(true)
            .help("Data directory for catalog, tables, wal"))
        .arg(Arg::new("listen")
            .long("listen")
            .value_name("ADDR")
            .default_value("127.0.0.1:54330")
            .help("Listen address for TCP SQL server"))
        .get_matches();

    let data_dir = matches.get_one::<String>("data").unwrap().to_string();
    let listen = matches.get_one::<String>("listen").unwrap().to_string();

    // Initialize structured logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();

    // Open the database engine with crash recovery
    let engine = Arc::new(Engine::open(data_dir).await?);

    // Start the TCP server
    serve(engine, &listen).await
}
