use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use super::types::{Download, DownloadState, RankedFile, SearchResult};

/// events sent from download manager to UI
#[derive(Debug, Clone)]
pub enum DownloadEvent {
    /// download started
    Started(u32),
    /// progress update (id, downloaded, total)
    Progress(u32, u64, u64),
    /// download completed
    Completed(u32),
    /// download failed
    Failed(u32, String),
    /// download cancelled
    Cancelled(u32),
}

/// manages file downloads from peers
pub struct DownloadManager {
    /// active and queued downloads
    pub downloads: Arc<RwLock<HashMap<u32, Download>>>,
    /// next download id
    next_id: AtomicU32,
    /// download directory
    download_dir: PathBuf,
    /// event sender for UI updates
    event_tx: Option<mpsc::UnboundedSender<DownloadEvent>>,
    /// our username for peer connections
    username: String,
    /// max concurrent downloads
    max_concurrent: usize,
}

impl DownloadManager {
    pub fn new(download_dir: PathBuf, username: String) -> Self {
        Self {
            downloads: Arc::new(RwLock::new(HashMap::new())),
            next_id: AtomicU32::new(1),
            download_dir,
            event_tx: None,
            username,
            max_concurrent: 3,
        }
    }

    pub fn set_event_sender(&mut self, tx: mpsc::UnboundedSender<DownloadEvent>) {
        self.event_tx = Some(tx);
    }

    fn send_event(&self, event: DownloadEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    /// queue a file for download, returns download id
    pub async fn queue_download(&self, file: &RankedFile) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // create local path
        let filename = file.file.basename();
        let local_path = self.download_dir.join(filename);

        let download = Download::new(
            id,
            file.username.clone(),
            file.file.filename.clone(),
            local_path,
            file.file.size,
            file.quality_score,
        );

        {
            let mut downloads = self.downloads.write().await;
            downloads.insert(id, download);
        }

        id
    }

    /// cancel a download
    pub async fn cancel_download(&self, id: u32) {
        let mut downloads = self.downloads.write().await;
        if let Some(dl) = downloads.get_mut(&id) {
            if dl.is_active() {
                dl.state = DownloadState::Cancelled;
                self.send_event(DownloadEvent::Cancelled(id));
            }
        }
    }

    /// remove completed/failed downloads from list
    pub async fn clear_finished(&self) {
        let mut downloads = self.downloads.write().await;
        downloads.retain(|_, dl| dl.is_active());
    }

    /// get all downloads for display
    pub async fn get_downloads(&self) -> Vec<Download> {
        let downloads = self.downloads.read().await;
        let mut list: Vec<_> = downloads.values().cloned().collect();
        // sort by state (active first), then by quality
        list.sort_by(|a, b| {
            let state_ord = |s: DownloadState| match s {
                DownloadState::Downloading => 0,
                DownloadState::Connecting => 1,
                DownloadState::Pending => 2,
                DownloadState::Complete => 3,
                DownloadState::Failed => 4,
                DownloadState::Cancelled => 5,
            };
            state_ord(a.state)
                .cmp(&state_ord(b.state))
                .then(b.quality_score.cmp(&a.quality_score))
        });
        list
    }

    /// get a specific download
    pub async fn get_download(&self, id: u32) -> Option<Download> {
        let downloads = self.downloads.read().await;
        downloads.get(&id).cloned()
    }

    /// update download progress
    pub async fn update_progress(&self, id: u32, downloaded: u64) {
        let mut downloads = self.downloads.write().await;
        if let Some(dl) = downloads.get_mut(&id) {
            dl.downloaded = downloaded;
            if dl.state != DownloadState::Downloading {
                dl.state = DownloadState::Downloading;
            }
            self.send_event(DownloadEvent::Progress(id, downloaded, dl.size));
        }
    }

    /// mark download as complete
    pub async fn mark_complete(&self, id: u32) {
        let mut downloads = self.downloads.write().await;
        if let Some(dl) = downloads.get_mut(&id) {
            dl.state = DownloadState::Complete;
            dl.downloaded = dl.size;
            self.send_event(DownloadEvent::Completed(id));
        }
    }

    /// mark download as failed
    pub async fn mark_failed(&self, id: u32, reason: &str) {
        let mut downloads = self.downloads.write().await;
        if let Some(dl) = downloads.get_mut(&id) {
            dl.state = DownloadState::Failed;
            self.send_event(DownloadEvent::Failed(id, reason.to_string()));
        }
    }

    /// count active downloads
    pub async fn active_count(&self) -> usize {
        let downloads = self.downloads.read().await;
        downloads.values().filter(|d| d.is_active()).count()
    }

    /// get next pending download
    pub async fn next_pending(&self) -> Option<Download> {
        let downloads = self.downloads.read().await;
        downloads
            .values()
            .filter(|d| d.state == DownloadState::Pending)
            .max_by_key(|d| d.quality_score)
            .cloned()
    }
}

/// rank and sort search results by quality
pub fn rank_search_results(results: &[SearchResult]) -> Vec<RankedFile> {
    let mut ranked: Vec<RankedFile> = Vec::new();

    for result in results {
        for file in &result.files {
            // skip non-audio files
            let ext = file.extension().unwrap_or("");
            if !matches!(
                ext.to_lowercase().as_str(),
                "mp3" | "flac" | "ogg" | "opus" | "m4a" | "aac" | "wav" | "wma"
            ) {
                continue;
            }

            let quality_score = file.quality_score();

            // bonus for free slots and fast upload
            let mut adjusted_score = quality_score;
            if result.free_slots {
                adjusted_score += 50;
            }
            if result.upload_speed > 100_000 {
                adjusted_score += 25;
            }
            if result.queue_size == 0 {
                adjusted_score += 25;
            }

            ranked.push(RankedFile {
                file: file.clone(),
                username: result.username.clone(),
                free_slots: result.free_slots,
                upload_speed: result.upload_speed,
                queue_size: result.queue_size,
                quality_score: adjusted_score,
            });
        }
    }

    // sort by quality score descending
    ranked.sort_by(|a, b| b.quality_score.cmp(&a.quality_score));

    ranked
}
