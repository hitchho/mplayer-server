use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

use super::protocol::{MessageReader, MessageWriter, PeerCode};
use super::types::{Download, SoulseekError};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const CHUNK_SIZE: usize = 16384;

/// events from peer download tasks
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// connected to peer
    Connected(u32),
    /// download started
    Started(u32),
    /// progress (id, bytes downloaded, total)
    Progress(u32, u64, u64),
    /// completed
    Completed(u32),
    /// failed
    Failed(u32, String),
}

/// download a file from a peer
pub async fn download_file(
    download: Download,
    peer_ip: std::net::Ipv4Addr,
    peer_port: u16,
    our_username: &str,
    event_tx: mpsc::UnboundedSender<PeerEvent>,
) -> Result<(), SoulseekError> {
    let id = download.id;

    // connect to peer
    let addr = format!("{}:{}", peer_ip, peer_port);
    let stream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            let _ = event_tx.send(PeerEvent::Failed(id, format!("connect failed: {}", e)));
            return Err(SoulseekError::Io(e));
        }
        Err(_) => {
            let _ = event_tx.send(PeerEvent::Failed(id, "connect timeout".to_string()));
            return Err(SoulseekError::Timeout);
        }
    };

    stream.set_nodelay(true)?;
    let _ = event_tx.send(PeerEvent::Connected(id));

    // perform handshake and download
    match do_transfer(stream, &download, our_username, &event_tx).await {
        Ok(_) => {
            let _ = event_tx.send(PeerEvent::Completed(id));
            Ok(())
        }
        Err(e) => {
            let _ = event_tx.send(PeerEvent::Failed(id, e.to_string()));
            Err(e)
        }
    }
}

async fn do_transfer(
    mut stream: TcpStream,
    download: &Download,
    our_username: &str,
    event_tx: &mpsc::UnboundedSender<PeerEvent>,
) -> Result<(), SoulseekError> {
    let id = download.id;

    // send peer init (code 1)
    let init_msg = pack_peer_init(our_username, "P", download.token);
    stream.write_all(&init_msg).await?;
    stream.flush().await?;

    // send transfer request (peer code 40)
    // direction 0 = download, 1 = upload
    let transfer_msg = pack_transfer_request(0, download.token, &download.remote_path);
    stream.write_all(&transfer_msg).await?;
    stream.flush().await?;

    // read transfer response
    let (code, data) = read_peer_message(&mut stream).await?;

    if code != PeerCode::TransferResponse as u32 {
        return Err(SoulseekError::InvalidMessage);
    }

    let mut reader = MessageReader::new(&data);
    let _token = reader.read_u32()?;
    let allowed = reader.read_bool()?;

    if !allowed {
        let reason = if reader.remaining() >= 4 {
            reader.read_string()?
        } else {
            "transfer denied".to_string()
        };
        return Err(SoulseekError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            reason,
        )));
    }

    // read file size from response
    let file_size = if reader.remaining() >= 8 {
        reader.read_u64()?
    } else {
        download.size
    };

    let _ = event_tx.send(PeerEvent::Started(id));

    // send file offset (start from 0)
    let offset: u64 = 0;
    stream.write_all(&offset.to_le_bytes()).await?;
    stream.flush().await?;

    // create parent dirs if needed
    if let Some(parent) = download.local_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // open file for writing
    let file = File::create(&download.local_path).await?;
    let mut writer = BufWriter::new(file);

    // receive file data
    let mut downloaded: u64 = 0;
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut last_progress = 0u64;

    while downloaded < file_size {
        let to_read = ((file_size - downloaded) as usize).min(CHUNK_SIZE);

        let n = match timeout(READ_TIMEOUT, stream.read(&mut buf[..to_read])).await {
            Ok(Ok(0)) => break, // eof
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(SoulseekError::Io(e)),
            Err(_) => return Err(SoulseekError::Timeout),
        };

        writer.write_all(&buf[..n]).await?;
        downloaded += n as u64;

        // send progress every 64KB
        if downloaded - last_progress >= 65536 {
            let _ = event_tx.send(PeerEvent::Progress(id, downloaded, file_size));
            last_progress = downloaded;
        }
    }

    writer.flush().await?;

    // final progress
    let _ = event_tx.send(PeerEvent::Progress(id, downloaded, file_size));

    if downloaded < file_size {
        return Err(SoulseekError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("incomplete: {} of {} bytes", downloaded, file_size),
        )));
    }

    Ok(())
}

async fn read_peer_message(stream: &mut TcpStream) -> Result<(u32, Vec<u8>), SoulseekError> {
    // read size
    let mut size_buf = [0u8; 4];
    match timeout(READ_TIMEOUT, stream.read_exact(&mut size_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(SoulseekError::Io(e)),
        Err(_) => return Err(SoulseekError::Timeout),
    }

    let size = u32::from_le_bytes(size_buf);
    if !(4..=16 * 1024 * 1024).contains(&size) {
        return Err(SoulseekError::InvalidMessage);
    }

    // read code
    let mut code_buf = [0u8; 4];
    stream.read_exact(&mut code_buf).await?;
    let code = u32::from_le_bytes(code_buf);

    // read content
    let content_size = (size - 4) as usize;
    let mut content = vec![0u8; content_size];
    if content_size > 0 {
        stream.read_exact(&mut content).await?;
    }

    Ok((code, content))
}

fn pack_peer_init(username: &str, conn_type: &str, token: u32) -> Vec<u8> {
    let mut w = MessageWriter::new();
    w.write_string(username);
    w.write_string(conn_type);
    w.write_u32(token);

    // peer init uses 1-byte code prefix
    let content = w.buf;
    let msg_len = content.len() + 1;
    let mut msg = Vec::with_capacity(4 + msg_len);
    msg.extend_from_slice(&(msg_len as u32).to_le_bytes());
    msg.push(1u8); // PeerInit code
    msg.extend(content);
    msg
}

fn pack_transfer_request(direction: u32, token: u32, filename: &str) -> Vec<u8> {
    let mut w = MessageWriter::new();
    w.write_u32(direction);
    w.write_u32(token);
    w.write_string(filename);
    w.into_message(PeerCode::TransferRequest as u32)
}
