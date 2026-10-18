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

/// peer code 40
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRequest {
    /// 0 = download request (to the uploader), 1 = upload (uploader to us)
    pub direction: u32,
    pub token: u32,
    pub filename: String,
    /// only present when direction == 1
    pub size: Option<u64>,
}

pub fn parse_transfer_request(data: &[u8]) -> io::Result<TransferRequest> {
    let mut r = MessageReader::new(data);
    let direction = r.read_u32()?;
    let token = r.read_u32()?;
    let filename = r.read_string()?;
    let size = if direction == 1 && r.remaining() >= 8 {
        Some(r.read_u64()?)
    } else {
        None
    };
    Ok(TransferRequest {
        direction,
        token,
        filename,
        size,
    })
}

/// peer code 44: (filename, place)
pub fn parse_place_in_queue(data: &[u8]) -> io::Result<(String, u32)> {
    let mut r = MessageReader::new(data);
    Ok((r.read_string()?, r.read_u32()?))
}

/// peer code 46: filename
pub fn parse_upload_failed(data: &[u8]) -> io::Result<String> {
    MessageReader::new(data).read_string()
}

/// peer code 50: (filename, reason)
pub fn parse_upload_denied(data: &[u8]) -> io::Result<(String, String)> {
    let mut r = MessageReader::new(data);
    Ok((r.read_string()?, r.read_string()?))
}

/// first message on an incoming peer connection (1-byte code framing)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitMessage {
    PierceFirewall(u32),
    PeerInit {
        username: String,
        conn_type: String,
        token: u32,
    },
}

pub fn parse_init_message(code: u8, body: &[u8]) -> io::Result<InitMessage> {
    let mut r = MessageReader::new(body);
    match code {
        0 => Ok(InitMessage::PierceFirewall(r.read_u32()?)),
        1 => Ok(InitMessage::PeerInit {
            username: r.read_string()?,
            conn_type: r.read_string()?,
            token: r.read_u32()?,
        }),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown init code")),
    }
}

#[cfg(test)]
mod tests {
    use super::super::protocol::*;
    use super::*;

    #[test]
    fn queue_upload_bytes() {
        // len=4+4+3, code 43, "abc"
        assert_eq!(
            pack_queue_upload("abc"),
            vec![11, 0, 0, 0, 43, 0, 0, 0, 3, 0, 0, 0, b'a', b'b', b'c']
        );
    }

    #[test]
    fn transfer_request_upload_parse() {
        let mut w = MessageWriter::new();
        w.write_u32(1);
        w.write_u32(0xdeadbeef);
        w.write_string("a\\b.mp3");
        w.write_u64(5_000_000_000);
        let msg = w.into_message(40);
        // strip len + code
        let req = parse_transfer_request(&msg[8..]).unwrap();
        assert_eq!(
            req,
            TransferRequest {
                direction: 1,
                token: 0xdeadbeef,
                filename: "a\\b.mp3".into(),
                size: Some(5_000_000_000)
            }
        );
    }

    #[test]
    fn transfer_request_download_has_no_size() {
        let mut w = MessageWriter::new();
        w.write_u32(0);
        w.write_u32(7);
        w.write_string("x");
        let msg = w.into_message(40);
        let req = parse_transfer_request(&msg[8..]).unwrap();
        assert_eq!(req.size, None);
        assert!(parse_transfer_request(&[1, 0, 0, 0]).is_err());
    }

    #[test]
    fn transfer_response_bytes() {
        assert_eq!(
            pack_transfer_response(0x01020304, true, None),
            vec![9, 0, 0, 0, 41, 0, 0, 0, 4, 3, 2, 1, 1]
        );
        assert_eq!(
            pack_transfer_response(1, false, Some("no")),
            vec![15, 0, 0, 0, 41, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, b'n', b'o']
        );
    }

    #[test]
    fn pierce_firewall_bytes() {
        assert_eq!(pack_pierce_firewall(0x0a0b0c0d), vec![5, 0, 0, 0, 0, 0x0d, 0x0c, 0x0b, 0x0a]);
        assert_eq!(
            parse_init_message(0, &[0x0d, 0x0c, 0x0b, 0x0a]).unwrap(),
            InitMessage::PierceFirewall(0x0a0b0c0d)
        );
    }

    #[test]
    fn peer_init_roundtrip() {
        let msg = pack_peer_init("me", "P", 0);
        assert_eq!(
            msg,
            vec![
                16, 0, 0, 0, 1, 2, 0, 0, 0, b'm', b'e', 1, 0, 0, 0, b'P', 0, 0, 0, 0
            ]
        );
        assert_eq!(
            parse_init_message(msg[4], &msg[5..]).unwrap(),
            InitMessage::PeerInit {
                username: "me".into(),
                conn_type: "P".into(),
                token: 0
            }
        );
        assert!(parse_init_message(9, &[]).is_err());
    }

    #[test]
    fn connect_to_peer_request_bytes() {
        assert_eq!(
            pack_connect_to_peer(5, "bob", "P"),
            vec![
                20, 0, 0, 0, 18, 0, 0, 0, 5, 0, 0, 0, 3, 0, 0, 0, b'b', b'o', b'b', 1, 0, 0, 0,
                b'P'
            ]
        );
    }

    #[test]
    fn file_transfer_raw_values() {
        assert_eq!(pack_file_transfer_init(0x11223344), [0x44, 0x33, 0x22, 0x11]);
        assert_eq!(pack_file_offset(0), [0u8; 8]);
        assert_eq!(pack_file_offset(1), [1, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn queue_state_messages_parse() {
        let mut w = MessageWriter::new();
        w.write_string("f");
        w.write_string("Banned");
        let m = w.into_message(50);
        assert_eq!(parse_upload_denied(&m[8..]).unwrap(), ("f".into(), "Banned".into()));
        let mut w = MessageWriter::new();
        w.write_string("f");
        w.write_u32(3);
        let m = w.into_message(44);
        assert_eq!(parse_place_in_queue(&m[8..]).unwrap(), ("f".into(), 3));
    }
}
