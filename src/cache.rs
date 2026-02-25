use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{env, io::ErrorKind};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::download_gate::DownloadGate;
use crate::download_pool::DownloadPool;
use crate::http::UrlDownloader;

#[derive(Debug, Clone)]
pub struct Cache {
    root: PathBuf,
    lru_tracker: Arc<LruTracker>,
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub data_path: PathBuf,
    pub meta_path: PathBuf,
    pub lock_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CacheMeta {
    pub url: String,
    pub size: Option<u64>,
    pub mtime: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockMeta {
    pub url: String,
    pub size: Option<u64>,
    pub mtime: Option<DateTime<Utc>>,
    pub block_size: u64,
    pub no_range: bool,
}

#[derive(Debug, Clone)]
pub struct BlockCache {
    root: PathBuf,
    block_size: u64,
}

#[derive(Debug, Clone)]
pub struct BlockEntry {
    pub dir: PathBuf,
    pub meta_path: PathBuf,
}

pub struct LruTracker {
    inner: Mutex<LruInner>,
    max_bytes: u64,
}

struct LruInner {
    order: lru::LruCache<PathBuf, u64>,
    current_bytes: u64,
}

impl std::fmt::Debug for LruTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruTracker")
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

impl LruTracker {
    pub fn new(max_bytes: u64) -> Self {
        LruTracker {
            inner: Mutex::new(LruInner {
                order: lru::LruCache::unbounded(),
                current_bytes: 0,
            }),
            max_bytes,
        }
    }

    /// Register a cached file. Evicts least-recently-used entries from disk
    /// until total tracked bytes is within the configured limit.
    pub fn track(&self, path: &Path, size: u64) {
        let to_evict = {
            let mut inner = self.inner.lock().expect("lru lock");
            if let Some(old_size) = inner.order.get_mut(path) {
                let prev = *old_size;
                *old_size = size;
                inner.current_bytes = inner.current_bytes - prev + size;
                return;
            }
            inner.order.push(path.to_path_buf(), size);
            inner.current_bytes += size;
            let mut evicted = Vec::new();
            while inner.current_bytes > self.max_bytes {
                if let Some((p, s)) = inner.order.pop_lru() {
                    inner.current_bytes -= s;
                    evicted.push((p, s));
                } else {
                    break;
                }
            }
            evicted
        };
        for (evicted_path, evicted_size) in to_evict {
            if let Err(err) = fs::remove_file(&evicted_path) {
                debug!("lru evict remove failed for {:?}: {err}", evicted_path);
            }
            info!("lru evicted {:?} ({evicted_size} bytes)", evicted_path);
        }
    }

    /// Promote a cached file to most-recently-used.
    pub fn touch(&self, path: &Path) {
        let mut inner = self.inner.lock().expect("lru lock");
        let _ = inner.order.get(path);
    }

    #[cfg(test)]
    fn current_bytes(&self) -> u64 {
        self.inner.lock().expect("lru lock").current_bytes
    }
}

impl BlockCache {
    pub fn new(root: impl Into<PathBuf>, block_size: u64) -> Self {
        let block_size = block_size.max(4096);
        BlockCache {
            root: root.into(),
            block_size,
        }
    }

    pub fn entry_for(&self, url: &str) -> io::Result<BlockEntry> {
        fs::create_dir_all(&self.root)?;
        let hash = hash_url(url);
        let dir = self.root.join(format!("{hash}.{}.blocks", self.block_size));
        fs::create_dir_all(&dir)?;
        let meta_path = dir.join("meta.json");
        Ok(BlockEntry { dir, meta_path })
    }

    pub fn block_path(&self, entry: &BlockEntry, index: u64) -> PathBuf {
        entry.dir.join(format!("{index}.block"))
    }

    pub fn load_meta(entry: &BlockEntry) -> io::Result<BlockMeta> {
        let raw = fs::read_to_string(&entry.meta_path)?;
        let meta = serde_json::from_str(&raw)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        Ok(meta)
    }

    pub fn write_meta(entry: &BlockEntry, meta: &BlockMeta) -> io::Result<()> {
        let raw = serde_json::to_string(meta)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        fs::write(&entry.meta_path, raw)
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }
}

impl Cache {
    pub fn new(root: impl Into<PathBuf>, lru_tracker: Arc<LruTracker>) -> Self {
        Cache {
            root: root.into(),
            lru_tracker,
        }
    }

    pub fn track(&self, path: &Path, size: u64) {
        self.lru_tracker.track(path, size);
    }

    pub fn touch(&self, path: &Path) {
        self.lru_tracker.touch(path);
    }

    pub fn entry_for(&self, url: &str) -> io::Result<CacheEntry> {
        fs::create_dir_all(&self.root)?;
        let hash = hash_url(url);
        let data_path = self.root.join(&hash);
        let meta_path = self.root.join(format!("{hash}.meta"));
        let lock_path = self.root.join(format!("{hash}.lock"));
        Ok(CacheEntry {
            data_path,
            meta_path,
            lock_path,
        })
    }

    pub fn load_meta(entry: &CacheEntry) -> io::Result<CacheMeta> {
        let raw = fs::read_to_string(&entry.meta_path)?;
        let meta = serde_json::from_str(&raw)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        Ok(meta)
    }

    pub fn write_meta(entry: &CacheEntry, meta: &CacheMeta) -> io::Result<()> {
        let raw = serde_json::to_string(meta)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        fs::write(&entry.meta_path, raw)
    }

    pub fn is_fresh(entry: &CacheEntry, expected: &CacheMeta) -> io::Result<bool> {
        if !entry.data_path.exists() || !entry.meta_path.exists() {
            return Ok(false);
        }
        let current = Self::load_meta(entry)?;
        if current.url != expected.url {
            return Ok(false);
        }
        let metadata = fs::metadata(&entry.data_path)?;
        if let Some(expected_size) = expected.size
            && metadata.len() != expected_size
        {
            warn!(
                "cache size mismatch for {:?}: expected {}, found {}",
                entry.data_path,
                expected_size,
                metadata.len()
            );
            return Ok(false);
        }
        if let Some(current_size) = current.size
            && metadata.len() != current_size
        {
            warn!(
                "cache size mismatch for {:?}: meta {}, found {}",
                entry.data_path,
                current_size,
                metadata.len()
            );
            return Ok(false);
        }
        Ok(true)
    }

    pub fn lock(entry: &CacheEntry) -> io::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&entry.lock_path)?;
        file.lock_exclusive()?;
        Ok(file)
    }

    pub fn try_lock(entry: &CacheEntry) -> io::Result<Option<File>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&entry.lock_path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(file)),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn ensure_cached<D: UrlDownloader>(
        &self,
        entry: &CacheEntry,
        expected: &CacheMeta,
        downloader: &D,
    ) -> io::Result<PathBuf> {
        let _lock = Self::lock(entry)?;
        if Self::is_fresh(entry, expected)? {
            debug!("cache hit for {:?}", entry.data_path);
            self.touch(&entry.data_path);
            return Ok(entry.data_path.clone());
        }

        info!("cache miss, downloading to {:?}", entry.data_path);
        let temp_path = Self::temp_path(entry);
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }
        if let Err(err) = downloader.download_full(&expected.url, &temp_path) {
            warn!("download failed: {err}");
            return Err(err);
        }
        if let Err(err) = Self::commit_temp(&temp_path, &entry.data_path) {
            warn!("failed to commit cache file: {err}");
            return Err(err);
        }
        let mut meta = expected.clone();
        if meta.size.is_none()
            && let Ok(metadata) = fs::metadata(&entry.data_path)
        {
            meta.size = Some(metadata.len());
        }
        Self::write_meta(entry, &meta)?;
        self.track(&entry.data_path, meta.size.unwrap_or(0));
        Ok(entry.data_path.clone())
    }

    pub fn ensure_cached_streaming<D: UrlDownloader + 'static>(
        &self,
        entry: &CacheEntry,
        expected: CacheMeta,
        downloader: Arc<D>,
        download_gate: Arc<DownloadGate>,
        download_pool: Arc<DownloadPool>,
    ) -> io::Result<PathBuf> {
        if Self::is_fresh(entry, &expected)? {
            debug!("cache hit for {:?}", entry.data_path);
            self.touch(&entry.data_path);
            return Ok(entry.data_path.clone());
        }

        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&entry.data_path)?;

        let lock = match Self::try_lock(entry)? {
            Some(lock) => lock,
            None => {
                debug!("download already in progress for {:?}", entry.data_path);
                return Ok(entry.data_path.clone());
            }
        };

        let data_path = entry.data_path.clone();
        let meta_path = entry.meta_path.clone();
        let lru_tracker = Arc::clone(&self.lru_tracker);
        download_pool
            .enqueue(move || {
                let _permit = download_gate.acquire();
                let _lock = lock;
                let result = downloader.download_full(&expected.url, &data_path);
                if let Err(err) = result {
                    warn!("streaming download failed: {err}");
                    let _ = fs::remove_file(&data_path);
                    return;
                }
                let mut meta = expected;
                let size = match meta.size {
                    Some(s) => s,
                    None => {
                        let s = fs::metadata(&data_path).map(|m| m.len()).unwrap_or(0);
                        meta.size = Some(s);
                        s
                    }
                };
                lru_tracker.track(&data_path, size);
                if let Ok(raw) = serde_json::to_string(&meta) {
                    let _ = fs::write(&meta_path, raw);
                }
            })
            .map_err(|_| io::Error::other("streaming download pool unavailable"))?;

        Ok(entry.data_path.clone())
    }

    pub fn store_bytes(
        &self,
        entry: &CacheEntry,
        data: &[u8],
        expected: &CacheMeta,
    ) -> io::Result<PathBuf> {
        let _lock = Self::lock(entry)?;
        let temp_path = Self::temp_path(entry);
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }
        fs::write(&temp_path, data)?;
        Self::commit_temp(&temp_path, &entry.data_path)?;
        let mut meta = expected.clone();
        if meta.size.is_none() {
            meta.size = Some(data.len() as u64);
        }
        Self::write_meta(entry, &meta)?;
        self.track(&entry.data_path, data.len() as u64);
        Ok(entry.data_path.clone())
    }

    pub fn temp_path(entry: &CacheEntry) -> PathBuf {
        entry.data_path.with_file_name(format!(
            "{}.tmp",
            entry
                .data_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("cache.tmp")
        ))
    }

    pub fn commit_temp(temp_path: &Path, data_path: &Path) -> io::Result<()> {
        fs::rename(temp_path, data_path)
    }

    pub fn stats(&self) -> io::Result<CacheStats> {
        let mut stats = CacheStats::default();
        if !self.root.exists() {
            return Ok(stats);
        }
        for data_path in list_data_files(&self.root)? {
            let metadata = fs::metadata(&data_path)?;
            stats.bytes += metadata.len();
            stats.files += 1;
        }
        Ok(stats)
    }

    pub fn clean(&self) -> io::Result<()> {
        if self.root.exists() {
            fs::remove_dir_all(&self.root)?;
        }
        Ok(())
    }

    pub fn evict(&self, max_bytes: Option<u64>, max_age_days: Option<u64>) -> io::Result<()> {
        if !self.root.exists() {
            return Ok(());
        }

        let mut data_files = list_data_files(&self.root)?;
        let now = SystemTime::now();

        if let Some(days) = max_age_days {
            let threshold = now
                .checked_sub(Duration::from_secs(days * 24 * 60 * 60))
                .unwrap_or(SystemTime::UNIX_EPOCH);
            for path in data_files.iter() {
                if let Ok(metadata) = fs::metadata(path)
                    && let Ok(modified) = metadata.modified()
                    && modified < threshold
                {
                    remove_entry_files(path)?;
                }
            }
            data_files = list_data_files(&self.root)?;
        }

        if let Some(max_bytes) = max_bytes {
            let mut entries = Vec::new();
            let mut total = 0u64;
            for path in data_files {
                if let Ok(metadata) = fs::metadata(&path) {
                    total += metadata.len();
                    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    entries.push((path, metadata.len(), modified));
                }
            }
            if total > max_bytes {
                entries.sort_by_key(|(_, _, modified)| *modified);
                for (path, size, _) in entries {
                    if total <= max_bytes {
                        break;
                    }
                    remove_entry_files(&path)?;
                    total = total.saturating_sub(size);
                }
            }
        }

        Ok(())
    }
}

pub fn default_cache_dir() -> io::Result<PathBuf> {
    let home = env::var("HOME").map_err(|_| io::Error::new(ErrorKind::NotFound, "HOME not set"))?;
    Ok(PathBuf::from(home).join(".cache").join("fetchfs"))
}

fn hash_url(url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();
    hex::encode(digest)
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub files: u64,
    pub bytes: u64,
}

fn list_data_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(list_data_files(&path)?);
            continue;
        }
        if let Some(ext) = path.extension().and_then(|s| s.to_str())
            && (ext == "meta" || ext == "lock" || ext == "tmp")
        {
            continue;
        }
        files.push(path);
    }
    Ok(files)
}

fn remove_entry_files(data_path: &Path) -> io::Result<()> {
    let meta_path = data_path.with_file_name(format!(
        "{}.meta",
        data_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("cache")
    ));
    let lock_path = data_path.with_file_name(format!(
        "{}.lock",
        data_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("cache")
    ));
    let _ = fs::remove_file(data_path);
    let _ = fs::remove_file(meta_path);
    let _ = fs::remove_file(lock_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct MockDownloader {
        calls: Arc<Mutex<u32>>,
        content: Vec<u8>,
    }

    impl UrlDownloader for MockDownloader {
        fn download_full(&self, _url: &str, dest: &Path) -> io::Result<()> {
            let mut calls = self.calls.lock().expect("lock");
            *calls += 1;
            fs::write(dest, &self.content)
        }
    }

    #[test]
    fn cache_entry_paths_are_stable() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(tmp.path(), Arc::new(LruTracker::new(u64::MAX)));
        let entry = cache
            .entry_for("https://example.com/file.txt")
            .expect("entry");
        let entry2 = cache
            .entry_for("https://example.com/file.txt")
            .expect("entry2");

        assert_eq!(entry.data_path, entry2.data_path);
        assert!(entry.data_path.starts_with(tmp.path()));
        assert_eq!(
            entry.meta_path.extension().and_then(|s| s.to_str()),
            Some("meta")
        );
        assert_eq!(
            entry.lock_path.extension().and_then(|s| s.to_str()),
            Some("lock")
        );
    }

    #[test]
    fn ensure_cached_downloads_and_reuses() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(tmp.path(), Arc::new(LruTracker::new(u64::MAX)));
        let entry = cache
            .entry_for("https://example.com/file.txt")
            .expect("entry");
        let meta = CacheMeta {
            url: "https://example.com/file.txt".to_string(),
            size: Some(4),
            mtime: None,
        };
        let calls = Arc::new(Mutex::new(0));
        let downloader = MockDownloader {
            calls: Arc::clone(&calls),
            content: b"data".to_vec(),
        };

        let first = cache
            .ensure_cached(&entry, &meta, &downloader)
            .expect("download");
        let second = cache
            .ensure_cached(&entry, &meta, &downloader)
            .expect("reuse");

        assert_eq!(first, entry.data_path);
        assert_eq!(second, entry.data_path);
        assert_eq!(*calls.lock().expect("lock"), 1);
        assert_eq!(fs::read(&entry.data_path).expect("read"), b"data");
    }

    #[test]
    fn lru_tracker_evicts_oldest_when_over_limit() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tracker = LruTracker::new(250);

        let mut paths = Vec::new();
        for i in 0..3 {
            let path = tmp.path().join(format!("file_{i}"));
            fs::write(&path, vec![0u8; 100]).expect("write");
            paths.push(path);
        }

        for path in &paths {
            tracker.track(path, 100);
        }

        assert!(!paths[0].exists(), "oldest file should be evicted");
        assert!(paths[1].exists());
        assert!(paths[2].exists());
        assert_eq!(tracker.current_bytes(), 200);
    }

    #[test]
    fn lru_tracker_touch_prevents_eviction() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tracker = LruTracker::new(250);

        let path_a = tmp.path().join("a");
        let path_b = tmp.path().join("b");
        let path_c = tmp.path().join("c");
        for p in [&path_a, &path_b, &path_c] {
            fs::write(p, vec![0u8; 100]).expect("write");
        }

        tracker.track(&path_a, 100);
        tracker.track(&path_b, 100);
        tracker.touch(&path_a);
        tracker.track(&path_c, 100);

        assert!(path_a.exists(), "touched file should survive");
        assert!(!path_b.exists(), "untouched LRU file should be evicted");
        assert!(path_c.exists());
        assert_eq!(tracker.current_bytes(), 200);
    }

    #[test]
    fn lru_tracker_track_updates_existing_entry() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tracker = LruTracker::new(500);

        let path = tmp.path().join("file");
        fs::write(&path, vec![0u8; 100]).expect("write");

        tracker.track(&path, 100);
        assert_eq!(tracker.current_bytes(), 100);

        tracker.track(&path, 200);
        assert_eq!(tracker.current_bytes(), 200);

        assert!(path.exists());
    }
}
