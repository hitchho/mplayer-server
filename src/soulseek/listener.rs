use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use super::protocol::{pack_shared_file_list_response, MessageReader, PeerCode};
use super::types::ConnectionState;

/// listens for incoming peer connections
pub struct PeerListener {
    port: u16,
    state: Arc<RwLock<ConnectionState>>,
}

impl PeerListener {
    pub fn new(port: u16, state: Arc<RwLock<ConnectionState>>) -> Self {
        Self { port, state }
    }

    /// start listening for peer connections
    pub async fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let addr = format!("0.0.0.0:{}", self.port);
            let listener = match TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(_e) => {
                    // couldn't bind, just return
                    return;
                }
            };

            loop {
                // check if we're still connected
                {
                    let state = self.state.read().await;
                    if *state == ConnectionState::Disconnected {
                        break;
                    }
                }

                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        // handle peer in separate task
                        tokio::spawn(async move {
                            let _ = handle_peer_connection(stream).await;
                        });
                    }
                    Err(_) => {
                        // accept error, continue
                    }
                }
            }
        })
    }
}

/// handle an incoming peer connection
async fn handle_peer_connection(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;

    // read peer init message
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_le_bytes(size_buf);

    if !(1..=1024 * 1024).contains(&size) {
        return Ok(());
    }

    // read init code (1 byte for peer init)
    let init_code = stream.read_u8().await?;

    if init_code == 1 {
        // PeerInit message
        let remaining = (size - 1) as usize;
        let mut data = vec![0u8; remaining];
        stream.read_exact(&mut data).await?;

        let mut reader = MessageReader::new(&data);
        let _username = reader.read_string()?;
        let conn_type = reader.read_string()?;
        let _token = reader.read_u32()?;

        // handle based on connection type
        match conn_type.as_str() {
            "P" => {
                // peer message connection - handle requests
                handle_peer_messages(&mut stream).await?;
            }
            "F" => {
                // file transfer connection - we don't upload
            }
            "D" => {
                // distributed network - ignore
            }
            _ => {}
        }
    }

    Ok(())
}

/// handle messages on a peer connection
async fn handle_peer_messages(stream: &mut TcpStream) -> std::io::Result<()> {
    loop {
        // read message size
        let mut size_buf = [0u8; 4];
        match stream.read_exact(&mut size_buf).await {
            Ok(_) => {}
            Err(_) => break,
        }

        let size = u32::from_le_bytes(size_buf);
        if !(4..=16 * 1024 * 1024).contains(&size) {
            break;
        }

        // read code
        let mut code_buf = [0u8; 4];
        stream.read_exact(&mut code_buf).await?;
        let code = u32::from_le_bytes(code_buf);

        // read content
        let content_size = (size - 4) as usize;
        let mut _content = vec![0u8; content_size];
        if content_size > 0 {
            stream.read_exact(&mut _content).await?;
        }

        // handle known peer codes
        match code {
            4 => {
                // SharedFileListRequest - respond with our dummy share
                let response = pack_shared_file_list_response();
                stream.write_all(&response).await?;
                stream.flush().await?;
            }
            15 => {
                // UserInfoRequest - send minimal info
                let response = pack_user_info_response();
                stream.write_all(&response).await?;
                stream.flush().await?;
            }
            _ => {
                // unknown code, ignore
            }
        }
    }

    Ok(())
}

/// pack user info response (peer code 16)
fn pack_user_info_response() -> Vec<u8> {
    use super::protocol::MessageWriter;

    let mut w = MessageWriter::new();
    w.write_string("mplayer-client user"); // description
    w.write_bool(false); // no picture
    w.write_u32(0); // total uploads
    w.write_u32(0); // queue size
    w.write_bool(false); // slots free

    w.into_message(PeerCode::UserInfoResponse as u32)
}
