use crate::{engine::Engine, sql::plan_and_exec};
use anyhow::Result;
use std::{sync::Arc};
use tokio::{net::TcpListener, io::{AsyncBufReadExt, AsyncWriteExt, BufReader}};
use tracing::{info, error};

/// Starts the database TCP server and handles client connections.
///
/// This function implements a simple SQL protocol over TCP:
/// 
/// ## Protocol Features
/// - **Multi-line statements**: SQL can span multiple lines  
/// - **Statement termination**: Statements must end with semicolon (;)
/// - **Concurrent clients**: Each connection runs in a separate async task
/// - **Graceful shutdown**: Clients can send "QUIT;" to disconnect
/// - **Error handling**: SQL errors are returned as "ERROR: message"
///
/// ## Connection Lifecycle
/// 1. Accept TCP connection and spawn async task per client
/// 2. Send greeting: "rust_pg_db ready. End statements with ;"
/// 3. Read lines and buffer until semicolon is encountered  
/// 4. Parse and execute complete SQL statement
/// 5. Return results (JSON for SELECT, status for others)
/// 6. Repeat until client disconnects or sends QUIT
///
/// ## Protocol Example
/// ```text
/// Client: CREATE TABLE users (id INT, name TEXT);
/// Server: CREATE TABLE
/// 
/// Client: INSERT INTO users VALUES (1, 'Alice');  
/// Server: INSERT 1
///
/// Client: SELECT * FROM users;
/// Server: [{"id": {"Int": 1}, "name": {"Text": "Alice"}}]
///
/// Client: QUIT;
/// Server: bye
/// ```
///
/// ## Arguments
/// * `engine` - Shared database engine instance (Arc for multi-threading)
/// * `addr` - TCP bind address (e.g., "127.0.0.1:54330")
///
/// ## Returns
/// * `Ok(())` - Server shut down cleanly (never in normal operation)
/// * `Err(_)` - Network binding error or other server failure
pub async fn serve(engine: Arc<Engine>, addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "listening");

    loop {
        // Accept new client connection
        let (socket, peer) = listener.accept().await?;
        let engine = engine.clone();

        // Spawn separate task for each client (concurrent handling)
        tokio::spawn(async move {
            let (r, mut w) = socket.into_split();
            let mut reader = BufReader::new(r).lines();

            // Send greeting message
            if let Err(e) = w.write_all(b"rust_pg_db ready. End statements with ;\n").await {
                error!(error=%e, "write greeting error");
                return;
            }

            // Handle client session: accumulate lines until semicolon
            let mut buf = String::new();
            while let Ok(Some(line)) = reader.next_line().await {
                let line = line.trim_end().to_string();
                buf.push_str(&line);
                
                // Check for statement terminator
                if buf.ends_with(';') {
                    let stmt = buf.clone();
                    buf.clear();
                    
                    // Handle special QUIT command
                    if stmt.trim().eq_ignore_ascii_case("QUIT;") {
                        let _ = w.write_all(b"bye\n").await;
                        break;
                    }
                    
                    // Parse and execute SQL statement
                    match plan_and_exec(engine.clone(), &stmt).await {
                        Ok(resp) => {
                            // Send successful response
                            let _ = w.write_all(resp.as_bytes()).await;
                            let _ = w.write_all(b"\n").await;
                        }
                        Err(e) => {
                            // Send error response
                            let _ = w.write_all(format!("ERROR: {}\n", e).as_bytes()).await;
                        }
                    }
                }
            }
            info!(?peer, "client disconnected");
        });
    }
}
