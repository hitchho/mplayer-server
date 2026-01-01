use std::io;

use super::protocol::MessageReader;
use super::types::{FileAttribute, SearchResult, SharedFile};

#[derive(Debug)]
pub enum ServerMessage {
    LoginResponse(LoginResponse),
    FileSearchResponse(SearchResult),
    GetPeerAddress(PeerAddress),
    ConnectToPeer(ConnectToPeerInfo),
    Unknown(u32),
}

#[derive(Debug)]
pub struct LoginResponse {
    pub success: bool,
    pub message: String,
    pub ip: Option<std::net::Ipv4Addr>,
    pub password_hash: Option<String>,
    pub is_supporter: bool,
}

impl LoginResponse {
    /// get human-readable error description
    pub fn error_description(&self) -> Option<String> {
        if self.success {
            return None;
        }

        let desc = match self.message.as_str() {
            "INVALIDUSERNAME" => {
                "invalid username (must be 1-30 printable ascii chars, no leading/trailing spaces)"
            }
            "EMPTYPASSWORD" => "password cannot be empty",
            "INVALIDPASS" => "wrong password for existing account",
            "INVALIDVERSION" => "client version too old",
            "SVRFULL" => "server is full, try again later",
            "SVRPRIVATE" => "server not accepting new registrations",
            other => return Some(format!("login failed: {}", other)),
        };

        Some(desc.to_string())
    }

    /// check if this was a new account registration
    pub fn is_new_account(&self) -> bool {
        // server sends "OK" or similar welcome for new accounts
        // vs returning user info for existing accounts
        self.success && self.message.contains("Welcome")
    }
}

#[derive(Debug)]
pub struct PeerAddress {
    pub username: String,
    pub ip: std::net::Ipv4Addr,
    pub port: u16,
}

#[derive(Debug)]
pub struct ConnectToPeerInfo {
    pub username: String,
    pub conn_type: String,
    pub ip: std::net::Ipv4Addr,
    pub port: u16,
    pub token: u32,
    pub privileged: bool,
}

pub fn parse_server_message(code: u32, data: &[u8]) -> io::Result<ServerMessage> {
    match code {
        1 => parse_login_response(data),
        3 => parse_get_peer_address(data),
        18 => parse_connect_to_peer(data),
        _ => Ok(ServerMessage::Unknown(code)),
    }
}

fn parse_login_response(data: &[u8]) -> io::Result<ServerMessage> {
    let mut r = MessageReader::new(data);
    let success = r.read_bool()?;

    if success {
        let banner = r.read_string()?;
        let ip = r.read_ip()?;
        let password_hash = if r.remaining() >= 4 {
            Some(r.read_string()?)
        } else {
            None
        };
        let is_supporter = if r.remaining() >= 1 {
            r.read_bool()?
        } else {
            false
        };

        Ok(ServerMessage::LoginResponse(LoginResponse {
            success: true,
            message: banner,
            ip: Some(ip),
            password_hash,
            is_supporter,
        }))
    } else {
        let reason = r.read_string()?;
        Ok(ServerMessage::LoginResponse(LoginResponse {
            success: false,
            message: reason,
            ip: None,
            password_hash: None,
            is_supporter: false,
        }))
    }
}

fn parse_get_peer_address(data: &[u8]) -> io::Result<ServerMessage> {
    let mut r = MessageReader::new(data);
    let username = r.read_string()?;
    let ip = r.read_ip()?;
    let port = r.read_u32()? as u16;

    Ok(ServerMessage::GetPeerAddress(PeerAddress {
        username,
        ip,
        port,
    }))
}

fn parse_connect_to_peer(data: &[u8]) -> io::Result<ServerMessage> {
    let mut r = MessageReader::new(data);
    let username = r.read_string()?;
    let conn_type = r.read_string()?;
    let ip = r.read_ip()?;
    let port = r.read_u32()? as u16;
    let token = r.read_u32()?;
    let privileged = if r.remaining() >= 1 {
        r.read_bool()?
    } else {
        false
    };

    Ok(ServerMessage::ConnectToPeer(ConnectToPeerInfo {
        username,
        conn_type,
        ip,
        port,
        token,
        privileged,
    }))
}

pub fn parse_file_search_response(data: &[u8]) -> io::Result<SearchResult> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    // decompress zlib data
    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;

    let mut r = MessageReader::new(&decompressed);

    let username = r.read_string()?;
    let token = r.read_u32()?;
    let num_files = r.read_u32()? as usize;

    let mut files = Vec::with_capacity(num_files.min(1000));

    for _ in 0..num_files.min(1000) {
        if r.remaining() < 10 {
            break;
        }

        let _code = r.read_u8()?;
        let filename = r.read_string()?;

        // handle soulseek ns bug for files > 2gb
        let size_bytes_available = r.remaining() >= 8;
        let size = if size_bytes_available {
            let raw = r.read_u64()?;
            // check for ns bug (byte 7 == 0xff indicates bug)
            if (raw >> 56) == 0xff {
                raw & 0xffffffff
            } else {
                raw
            }
        } else {
            0
        };

        // skip obsolete extension field
        let _ext_len = r.read_u32()?;

        let num_attrs = r.read_u32()? as usize;
        let mut file = SharedFile {
            filename,
            size,
            ..Default::default()
        };

        for _ in 0..num_attrs.min(20) {
            if r.remaining() < 8 {
                break;
            }
            let attr_type = r.read_u32()?;
            let attr_value = r.read_u32()?;

            match FileAttribute::try_from(attr_type) {
                Ok(FileAttribute::Bitrate) => file.bitrate = attr_value,
                Ok(FileAttribute::Length) => file.length_secs = attr_value as u64,
                Ok(FileAttribute::Vbr) => file.vbr = attr_value != 0,
                Ok(FileAttribute::SampleRate) => file.sample_rate = attr_value,
                _ => {}
            }
        }

        files.push(file);
    }

    // read transfer info
    let free_slots = if r.remaining() >= 1 {
        r.read_bool()?
    } else {
        false
    };
    let upload_speed = if r.remaining() >= 4 { r.read_u32()? } else { 0 };
    let queue_size = if r.remaining() >= 4 { r.read_u32()? } else { 0 };

    Ok(SearchResult {
        username,
        token,
        files,
        free_slots,
        upload_speed,
        queue_size,
    })
}

#[derive(Debug)]
pub enum PeerMessage {
    FileSearchResponse(SearchResult),
    Unknown(u32),
}

pub fn parse_peer_message(code: u32, data: &[u8]) -> io::Result<PeerMessage> {
    match code {
        9 => {
            let result = parse_file_search_response(data)?;
            Ok(PeerMessage::FileSearchResponse(result))
        }
        _ => Ok(PeerMessage::Unknown(code)),
    }
}
