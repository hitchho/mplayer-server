use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::timeout;

use super::messages::{parse_init_message, InitMessage};
use super::peer::PeerShared;
use super::types::{ConnectionState, SearchResult};

/// search results keyed by search token, shared with the client
pub type SearchResults = Arc<RwLock<HashMap<u32, Vec<SearchResult>>>>;

/// peers must send their init message promptly after connecting
const INIT_TIMEOUT: Duration = Duration::from_secs(30);

/// listens for incoming peer connections
pub struct PeerListener {
    port: u16,
    state: Arc<RwLock<ConnectionState>>,
    shared: Arc<PeerShared>,
}

impl PeerListener {
    pub fn new(port: u16, state: Arc<RwLock<ConnectionState>>, shared: Arc<PeerShared>) -> Self {
        Self {
            port,
            state,
            shared,
        }
    }

    /// start listening for peer connections
    pub async fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let addr = format!("0.0.0.0:{}", self.port);
            let listener = match TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(_e) => return,
            };

            loop {
                if *self.state.read().await == ConnectionState::Disconnected {
                    break;
                }

                if let Ok((stream, _addr)) = listener.accept().await {
                    let shared = Arc::clone(&self.shared);
                    tokio::spawn(async move {
                        let _ = handle_incoming(stream, shared).await;
                    });
                }
            }
        })
    }
}

/// read the init message of an incoming connection and hand the stream to
/// the right handler
async fn handle_incoming(mut stream: TcpStream, shared: Arc<PeerShared>) -> std::io::Result<()> {
    stream.set_nodelay(true)?;

    let init = timeout(INIT_TIMEOUT, async {
        let size = stream.read_u32_le().await?;
        if !(1..=1024 * 1024).contains(&size) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad init size",
            ));
        }
        let code = stream.read_u8().await?;
        let mut body = vec![0u8; (size - 1) as usize];
        stream.read_exact(&mut body).await?;
        parse_init_message(code, &body)
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "init timeout"))??;

    match init {
        InitMessage::PierceFirewall(token) => shared.handle_incoming_pierce(stream, token),
        InitMessage::PeerInit {
            username,
            conn_type,
            ..
        } => match conn_type.as_str() {
            "P" => {
                shared.spawn_p_connection(stream, Some(username), vec![], false);
            }
            "F" => shared.handle_file_connection(stream).await,
            _ => {} // "D" distributed network etc: ignore
        },
    }
    Ok(())
}
