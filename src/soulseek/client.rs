use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::timeout;

use super::download::DownloadManager;
use super::listener::PeerListener;
use super::messages::{
    parse_file_search_response, parse_server_message, LoginResponse, ServerMessage,
};
use super::peer::{self, PeerEvent};
use super::protocol::{
    pack_file_search, pack_login, pack_set_status, pack_set_wait_port, pack_shared_folders_files,
    MessageWriter, ServerCode, MAX_MESSAGE_SIZE,
};
use super::types::{
    ConnectionState, Download, DownloadState, RankedFile, SearchResult, SoulseekConfig,
    SoulseekError,
};

const CLIENT_VERSION: u32 = 160;
const MINOR_VERSION: u32 = 3;
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn pack_get_peer_address(username: &str) -> Vec<u8> {
    let mut w = MessageWriter::new();
    w.write_string(username);
    w.into_message(ServerCode::GetPeerAddress as u32)
}

/// pending peer address request
#[allow(dead_code)]
struct PendingDownload {
    download: Download,
    username: String,
}

#[allow(dead_code)]
pub struct SoulseekClient {
    config: SoulseekConfig,
    state: Arc<RwLock<ConnectionState>>,
    stream: Arc<Mutex<Option<TcpStream>>>,
    search_token: AtomicU32,
    transfer_token: AtomicU32,
    pending_searches: Arc<RwLock<HashMap<u32, mpsc::Sender<SearchResult>>>>,
    search_results: Arc<RwLock<HashMap<u32, Vec<SearchResult>>>>,
    /// downloads waiting for peer address
    pending_downloads: Arc<RwLock<HashMap<String, Vec<PendingDownload>>>>,
    /// download manager
    download_manager: Arc<DownloadManager>,
    /// channel for peer events
    peer_event_tx: mpsc::UnboundedSender<PeerEvent>,
    peer_event_rx: Arc<Mutex<mpsc::UnboundedReceiver<PeerEvent>>>,
}

#[allow(dead_code)]
impl SoulseekClient {
    pub fn new(config: SoulseekConfig, download_dir: PathBuf) -> Self {
        let (peer_event_tx, peer_event_rx) = mpsc::unbounded_channel();
        let download_manager =
            Arc::new(DownloadManager::new(download_dir, config.username.clone()));

        Self {
            config,
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            stream: Arc::new(Mutex::new(None)),
            search_token: AtomicU32::new(1),
            transfer_token: AtomicU32::new(1000),
            pending_searches: Arc::new(RwLock::new(HashMap::new())),
            search_results: Arc::new(RwLock::new(HashMap::new())),
            pending_downloads: Arc::new(RwLock::new(HashMap::new())),
            download_manager,
            peer_event_tx,
            peer_event_rx: Arc::new(Mutex::new(peer_event_rx)),
        }
    }

    pub fn username(&self) -> &str {
        &self.config.username
    }

    pub fn download_manager(&self) -> Arc<DownloadManager> {
        Arc::clone(&self.download_manager)
    }

    pub async fn state(&self) -> ConnectionState {
        *self.state.read().await
    }

    pub async fn connect(&self) -> Result<(), SoulseekError> {
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connecting;
        }

        let addr = format!("{}:{}", self.config.server_host, self.config.server_port);
        let stream = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                let mut state = self.state.write().await;
                *state = ConnectionState::Error;
                return Err(SoulseekError::Io(e));
            }
            Err(_) => {
                let mut state = self.state.write().await;
                *state = ConnectionState::Error;
                return Err(SoulseekError::Timeout);
            }
        };

        stream.set_nodelay(true)?;

        {
            let mut s = self.stream.lock().await;
            *s = Some(stream);
        }

        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connected;
        }

        Ok(())
    }

    pub async fn login(&self) -> Result<LoginResponse, SoulseekError> {
        {
            let state = self.state.read().await;
            if *state != ConnectionState::Connected {
                return Err(SoulseekError::NotConnected);
            }
        }

        {
            let mut state = self.state.write().await;
            *state = ConnectionState::LoggingIn;
        }

        let login_msg = pack_login(
            &self.config.username,
            &self.config.password,
            CLIENT_VERSION,
            MINOR_VERSION,
        );

        self.send_raw(&login_msg).await?;

        // read login response
        let (code, data) = self.read_message().await?;
        let response = parse_server_message(code, &data)?;

        match response {
            ServerMessage::LoginResponse(lr) => {
                if lr.success {
                    // send wait port and status
                    let port_msg = pack_set_wait_port(self.config.listen_port);
                    self.send_raw(&port_msg).await?;

                    let status_msg = pack_set_status(2); // online
                    self.send_raw(&status_msg).await?;

                    // tell server we share 1 folder with 1 file (dummy.mp3)
                    // this is required to participate in the network
                    let shares_msg = pack_shared_folders_files(1, 1);
                    self.send_raw(&shares_msg).await?;

                    // start peer listener for incoming connections
                    let listener =
                        PeerListener::new(self.config.listen_port, Arc::clone(&self.state));
                    listener.start().await;

                    {
                        let mut state = self.state.write().await;
                        *state = ConnectionState::Ready;
                    }

                    Ok(lr)
                } else {
                    {
                        let mut state = self.state.write().await;
                        *state = ConnectionState::Error;
                    }
                    // use human-readable error if available
                    let err_msg = lr.error_description().unwrap_or(lr.message);
                    Err(SoulseekError::LoginFailed(err_msg))
                }
            }
            _ => {
                {
                    let mut state = self.state.write().await;
                    *state = ConnectionState::Error;
                }
                Err(SoulseekError::InvalidMessage)
            }
        }
    }

    pub async fn search(&self, query: &str) -> Result<u32, SoulseekError> {
        {
            let state = self.state.read().await;
            if *state != ConnectionState::Ready {
                return Err(SoulseekError::NotConnected);
            }
        }

        let token = self.search_token.fetch_add(1, Ordering::SeqCst);

        // clean up query - remove leading dashes per protocol
        let clean_query: String = query
            .split_whitespace()
            .filter(|w| !w.starts_with('-'))
            .collect::<Vec<_>>()
            .join(" ");

        let msg = pack_file_search(token, &clean_query);
        self.send_raw(&msg).await?;

        // initialize results storage
        {
            let mut results = self.search_results.write().await;
            results.insert(token, Vec::new());
        }

        Ok(token)
    }

    pub async fn get_search_results(&self, token: u32) -> Vec<SearchResult> {
        let results = self.search_results.read().await;
        results.get(&token).cloned().unwrap_or_default()
    }

    pub async fn clear_search(&self, token: u32) {
        let mut results = self.search_results.write().await;
        results.remove(&token);
    }

    pub async fn disconnect(&self) {
        let mut stream = self.stream.lock().await;
        if let Some(mut s) = stream.take() {
            let _ = s.shutdown().await;
        }
        let mut state = self.state.write().await;
        *state = ConnectionState::Disconnected;
    }

    /// queue a file for download
    pub async fn queue_download(&self, file: &RankedFile) -> Result<u32, SoulseekError> {
        {
            let state = self.state.read().await;
            if *state != ConnectionState::Ready {
                return Err(SoulseekError::NotConnected);
            }
        }

        // create download entry
        let id = self.download_manager.queue_download(file).await;

        // get download and set token
        let token = self.transfer_token.fetch_add(1, Ordering::SeqCst);
        {
            let mut downloads = self.download_manager.downloads.write().await;
            if let Some(dl) = downloads.get_mut(&id) {
                dl.token = token;
            }
        }

        // get the download info
        let download = self.download_manager.get_download(id).await;
        if let Some(dl) = download {
            // request peer address
            self.request_peer_address(&file.username, dl).await?;
        }

        Ok(id)
    }

    /// request peer address from server
    async fn request_peer_address(
        &self,
        username: &str,
        download: Download,
    ) -> Result<(), SoulseekError> {
        // add to pending
        {
            let mut pending = self.pending_downloads.write().await;
            pending
                .entry(username.to_string())
                .or_insert_with(Vec::new)
                .push(PendingDownload {
                    download,
                    username: username.to_string(),
                });
        }

        // send GetPeerAddress request
        let msg = pack_get_peer_address(username);
        self.send_raw(&msg).await
    }

    /// handle peer address response and start download
    async fn handle_peer_address(&self, username: &str, ip: Ipv4Addr, port: u16) {
        let pending = {
            let mut pending = self.pending_downloads.write().await;
            pending.remove(username).unwrap_or_default()
        };

        let our_username = self.config.username.clone();
        let event_tx = self.peer_event_tx.clone();
        let dm = Arc::clone(&self.download_manager);

        for pd in pending {
            let download = pd.download;
            let id = download.id;
            let username_clone = our_username.clone();
            let tx = event_tx.clone();
            let dm_clone = Arc::clone(&dm);

            // update state to connecting
            {
                let mut downloads = dm_clone.downloads.write().await;
                if let Some(dl) = downloads.get_mut(&id) {
                    dl.state = DownloadState::Connecting;
                }
            }

            // spawn download task
            tokio::spawn(async move {
                let result = peer::download_file(download, ip, port, &username_clone, tx).await;

                // update state based on result
                let mut downloads = dm_clone.downloads.write().await;
                if let Some(dl) = downloads.get_mut(&id) {
                    match result {
                        Ok(_) => {
                            dl.state = DownloadState::Complete;
                            dl.downloaded = dl.size;
                        }
                        Err(_) => {
                            dl.state = DownloadState::Failed;
                        }
                    }
                }
            });
        }
    }

    /// cancel a download
    pub async fn cancel_download(&self, id: u32) {
        self.download_manager.cancel_download(id).await;
    }

    /// clear finished downloads
    pub async fn clear_finished_downloads(&self) {
        self.download_manager.clear_finished().await;
    }

    /// get all downloads
    pub async fn get_downloads(&self) -> Vec<Download> {
        self.download_manager.get_downloads().await
    }

    /// process peer events and update download states
    pub async fn process_peer_events(&self) {
        let mut rx = self.peer_event_rx.lock().await;
        while let Ok(event) = rx.try_recv() {
            match event {
                PeerEvent::Connected(id) => {
                    let mut downloads = self.download_manager.downloads.write().await;
                    if let Some(dl) = downloads.get_mut(&id) {
                        dl.state = DownloadState::Connecting;
                    }
                }
                PeerEvent::Started(id) => {
                    let mut downloads = self.download_manager.downloads.write().await;
                    if let Some(dl) = downloads.get_mut(&id) {
                        dl.state = DownloadState::Downloading;
                    }
                }
                PeerEvent::Progress(id, downloaded, _total) => {
                    let mut downloads = self.download_manager.downloads.write().await;
                    if let Some(dl) = downloads.get_mut(&id) {
                        dl.downloaded = downloaded;
                        dl.state = DownloadState::Downloading;
                    }
                }
                PeerEvent::Completed(id) => {
                    let mut downloads = self.download_manager.downloads.write().await;
                    if let Some(dl) = downloads.get_mut(&id) {
                        dl.state = DownloadState::Complete;
                        dl.downloaded = dl.size;
                    }
                }
                PeerEvent::Failed(id, _reason) => {
                    let mut downloads = self.download_manager.downloads.write().await;
                    if let Some(dl) = downloads.get_mut(&id) {
                        dl.state = DownloadState::Failed;
                    }
                }
            }
        }
    }

    async fn send_raw(&self, data: &[u8]) -> Result<(), SoulseekError> {
        let mut stream = self.stream.lock().await;
        match stream.as_mut() {
            Some(s) => {
                s.write_all(data).await?;
                s.flush().await?;
                Ok(())
            }
            None => Err(SoulseekError::NotConnected),
        }
    }

    async fn read_message(&self) -> Result<(u32, Vec<u8>), SoulseekError> {
        let mut stream = self.stream.lock().await;
        let s = stream.as_mut().ok_or(SoulseekError::NotConnected)?;

        // read size (4 bytes)
        let mut size_buf = [0u8; 4];
        match timeout(READ_TIMEOUT, s.read_exact(&mut size_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(SoulseekError::ConnectionClosed);
            }
            Ok(Err(e)) => return Err(SoulseekError::Io(e)),
            Err(_) => return Err(SoulseekError::Timeout),
        }

        let size = u32::from_le_bytes(size_buf);
        if !(4..=MAX_MESSAGE_SIZE).contains(&size) {
            return Err(SoulseekError::InvalidMessage);
        }

        // read code (4 bytes)
        let mut code_buf = [0u8; 4];
        s.read_exact(&mut code_buf).await?;
        let code = u32::from_le_bytes(code_buf);

        // read content
        let content_size = (size - 4) as usize;
        let mut content = vec![0u8; content_size];
        if content_size > 0 {
            s.read_exact(&mut content).await?;
        }

        Ok((code, content))
    }

    /// poll for incoming messages and process search responses
    pub async fn poll(&self) -> Result<Option<ServerMessage>, SoulseekError> {
        let (code, data) = self.read_message().await?;

        // check for peer file search response (code 9 from peer)
        if code == 9 {
            if let Ok(result) = parse_file_search_response(&data) {
                let token = result.token;
                let mut results = self.search_results.write().await;
                if let Some(v) = results.get_mut(&token) {
                    v.push(result.clone());
                }
                return Ok(Some(ServerMessage::FileSearchResponse(result)));
            }
        }

        let msg = parse_server_message(code, &data)?;

        // handle GetPeerAddress response
        if let ServerMessage::GetPeerAddress(ref pa) = msg {
            self.handle_peer_address(&pa.username, pa.ip, pa.port).await;
        }

        // handle ConnectToPeer (indirect connection request)
        if let ServerMessage::ConnectToPeer(ref ctp) = msg {
            // peer wants us to connect to them (NAT traversal)
            if ctp.conn_type == "F" {
                // file transfer - check if we have pending downloads from this user
                self.handle_peer_address(&ctp.username, ctp.ip, ctp.port)
                    .await;
            }
        }

        Ok(Some(msg))
    }

    /// run the message loop in background
    pub fn spawn_receiver(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                // process any pending peer events
                self.process_peer_events().await;

                match self.poll().await {
                    Ok(Some(_msg)) => {
                        // message processed
                    }
                    Ok(None) => {}
                    Err(SoulseekError::ConnectionClosed) => {
                        let mut state = self.state.write().await;
                        *state = ConnectionState::Disconnected;
                        break;
                    }
                    Err(SoulseekError::Timeout) => {
                        // timeout is ok, just continue
                    }
                    Err(_e) => {
                        // log error but continue
                    }
                }
            }
        })
    }
}

impl Drop for SoulseekClient {
    fn drop(&mut self) {
        // cleanup handled by tokio
    }
}
