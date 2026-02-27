use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use fs2::FileExt;
use tracing::debug;

use crate::cache::{BlockCache, BlockMeta, Cache, CacheEntry, CacheMeta};
use crate::download_gate::DownloadGate;
use crate::download_pool::DownloadPool;
use crate::http::{HttpClient, RangeStatus};
use crate::manifest::ManifestEntry;

pub struct FetchFs {
    pub cache: Cache,
    pub block_cache: Option<BlockCache>,
    pub downloader: Arc<HttpClient>,
    pub download_gate: Arc<DownloadGate>,
    pub download_pool: Arc<DownloadPool>,
    pub streaming: bool,
    pub mount_time: DateTime<Utc>,
}

impl FetchFs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache: Cache,
        block_cache: Option<BlockCache>,
        downloader: Arc<HttpClient>,
        download_gate: Arc<DownloadGate>,
        download_pool: Arc<DownloadPool>,
        streaming: bool,
    ) -> Self {
        Self {
            cache,
            block_cache,
            downloader,
            download_gate,
            download_pool,
            streaming,
            mount_time: Utc::now(),
        }
    }

    pub(crate) fn cache_entry_for(&self, entry: &ManifestEntry) -> std::io::Result<CacheEntry> {
        self.cache.entry_for(entry.require_url()?)
    }

    fn cache_meta_for(&self, entry: &ManifestEntry) -> CacheMeta {
        CacheMeta {
            url: entry.url().unwrap_or("").to_string(),
            size: entry.size,
            mtime: entry.mtime,
        }
    }

    pub fn ensure_meta(&self, entry: &ManifestEntry) -> Option<CacheMeta> {
        if let Some(data) = entry.inline_data() {
            return Some(CacheMeta {
                url: String::new(),
                size: Some(data.len() as u64),
                mtime: entry.mtime,
            });
        }

        let url = entry.url()?;
        let cache_entry = self.cache_entry_for(entry).ok()?;
        if cache_entry.meta_path.exists() {
            return Cache::load_meta(&cache_entry).ok();
        }
        let mut meta = self.cache_meta_for(entry);
        if meta.size.is_some() && meta.mtime.is_some() {
            let _ = Cache::write_meta(&cache_entry, &meta);
            return Some(meta);
        }
        debug!("fetching metadata for {}", url);
        let _permit = self.download_gate.acquire();
        if let Ok(head) = self.downloader.head_with_fallback(url) {
            if meta.size.is_none() {
                meta.size = head.size;
            }
            if meta.mtime.is_none() {
                meta.mtime = head.mtime;
            }
            if Cache::write_meta(&cache_entry, &meta).is_ok() {
                debug!("cached metadata for {}", url);
                return Some(meta);
            }
        }
        debug!("metadata unavailable for {}", url);
        None
    }

    pub fn ensure_cached(&self, entry: &ManifestEntry) -> std::io::Result<(CacheEntry, PathBuf)> {
        let url = entry.require_url()?;
        let cache_entry = self.cache_entry_for(entry)?;
        let mut meta = self.cache_meta_for(entry);
        if meta.size.is_none() || meta.mtime.is_none() {
            let _permit = self.download_gate.acquire();
            if let Ok(head) = self.downloader.head_with_fallback(url) {
                if meta.size.is_none() {
                    meta.size = head.size;
                }
                if meta.mtime.is_none() {
                    meta.mtime = head.mtime;
                }
            }
        }
        let data_path = if self.streaming {
            self.cache.ensure_cached_streaming(
                &cache_entry,
                meta,
                Arc::clone(&self.downloader),
                Arc::clone(&self.download_gate),
                Arc::clone(&self.download_pool),
            )?
        } else {
            let _permit = self.download_gate.acquire();
            self.cache
                .ensure_cached(&cache_entry, &meta, self.downloader.as_ref())?
        };
        Ok((cache_entry, data_path))
    }

    pub fn read_range(
        &self,
        entry: &ManifestEntry,
        offset: u64,
        size: u32,
    ) -> std::io::Result<Vec<u8>> {
        if size == 0 {
            return Ok(Vec::new());
        }
        if let Some(data) = entry.inline_data() {
            return Ok(read_from_slice(data, offset, size));
        }
        if let Some(block_cache) = &self.block_cache {
            return self.read_range_block_cache(entry, offset, size, block_cache);
        }
        if !self.streaming && offset == 0 {
            let known_size = entry
                .known_size()
                .or_else(|| self.ensure_meta(entry).and_then(|meta| meta.size));
            if let Some(total) = known_size
                && (size as u64) >= total
            {
                let (_entry, data_path) = self.ensure_cached(entry)?;
                return read_from_path(&data_path, offset, size);
            }
        }
        let cache_entry = self.cache_entry_for(entry)?;
        if cache_entry.data_path.exists() {
            self.cache.touch(&cache_entry.data_path);
            return read_from_path(&cache_entry.data_path, offset, size);
        }

        let url = entry.require_url()?;
        let _permit = self.download_gate.acquire();
        let end = offset.saturating_add(size as u64).saturating_sub(1);
        let result = self.downloader.get_range(url, offset, end)?;
        let mut meta = self.cache_meta_for(entry);
        if meta.size.is_none() {
            meta.size = result.meta.size;
        }
        if meta.mtime.is_none() {
            meta.mtime = result.meta.mtime;
        }
        match result.status {
            RangeStatus::Partial => {
                if self.streaming {
                    let _ = self.cache.ensure_cached_streaming(
                        &cache_entry,
                        meta,
                        Arc::clone(&self.downloader),
                        Arc::clone(&self.download_gate),
                        Arc::clone(&self.download_pool),
                    );
                } else {
                    let _ = Cache::write_meta(&cache_entry, &meta);
                }
                Ok(result.data)
            }
            RangeStatus::Full => {
                let data_path = self.cache.store_bytes(&cache_entry, &result.data, &meta)?;
                read_from_path(&data_path, offset, size)
            }
            RangeStatus::Empty => Ok(Vec::new()),
        }
    }

    pub fn mtime_for(&self, entry: &ManifestEntry) -> SystemTime {
        entry
            .mtime
            .map(system_time_from_datetime)
            .unwrap_or_else(|| system_time_from_datetime(self.mount_time))
    }

    pub fn mount_system_time(&self) -> SystemTime {
        system_time_from_datetime(self.mount_time)
    }

    fn read_range_block_cache(
        &self,
        entry: &ManifestEntry,
        offset: u64,
        size: u32,
        block_cache: &BlockCache,
    ) -> std::io::Result<Vec<u8>> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let url = entry.require_url()?;
        let fresh_meta = || BlockMeta {
            url: url.to_string(),
            size: entry.size,
            mtime: entry.mtime,
            block_size: block_cache.block_size(),
            no_range: false,
        };
        let block_entry = block_cache.entry_for(url)?;
        let mut meta = if block_entry.meta_path.exists() {
            let loaded = BlockCache::load_meta(&block_entry)?;
            if loaded.url != url || loaded.block_size != block_cache.block_size() {
                fresh_meta()
            } else {
                loaded
            }
        } else {
            fresh_meta()
        };

        if meta.no_range {
            let (_entry, data_path) = self.ensure_cached(entry)?;
            return read_from_path(&data_path, offset, size);
        }

        let block_size = meta.block_size.max(1);
        let start_block = offset / block_size;
        let end_block = (offset + size as u64 - 1) / block_size;

        for block in start_block..=end_block {
            let block_path = block_cache.block_path(&block_entry, block);
            if block_path.exists() {
                self.cache.touch(&block_path);
                continue;
            }
            {
                // Acquire the download permit before taking the file lock to avoid lock/permit inversion.
                let _permit = self.download_gate.acquire();
                let lock_path = block_path.with_extension("lock");
                // Keep lock files so concurrent readers share the same inode lock.
                let lock_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&lock_path)?;
                lock_file.lock_exclusive()?;
                if block_path.exists() {
                    self.cache.touch(&block_path);
                    drop(lock_file);
                    continue;
                }
                let start = block * block_size;
                let end = start + block_size - 1;
                let result = self.downloader.get_range(url, start, end)?;
                match result.status {
                    RangeStatus::Partial => {
                        if meta.size.is_none() {
                            meta.size = result.meta.size;
                        }
                        if meta.mtime.is_none() {
                            meta.mtime = result.meta.mtime;
                        }
                        let block_len = result.data.len() as u64;
                        let tmp_path = block_path.with_extension("tmp");
                        std::fs::write(&tmp_path, result.data)?;
                        std::fs::rename(&tmp_path, &block_path)?;
                        self.cache.track(&block_path, block_len);
                        drop(lock_file);
                    }
                    RangeStatus::Full => {
                        meta.no_range = true;
                        let _ = BlockCache::write_meta(&block_entry, &meta);
                        let cache_entry = self.cache_entry_for(entry)?;
                        let mut cache_meta = self.cache_meta_for(entry);
                        if cache_meta.size.is_none() {
                            cache_meta.size = result.meta.size.or(Some(result.data.len() as u64));
                        }
                        if cache_meta.mtime.is_none() {
                            cache_meta.mtime = result.meta.mtime;
                        }
                        let data_path =
                            self.cache
                                .store_bytes(&cache_entry, &result.data, &cache_meta)?;
                        drop(lock_file);
                        return read_from_path(&data_path, offset, size);
                    }
                    RangeStatus::Empty => {
                        drop(lock_file);
                        return Ok(Vec::new());
                    }
                }
            }
        }
        let _ = BlockCache::write_meta(&block_entry, &meta);

        let mut buffer = Vec::with_capacity(size as usize);
        for block in start_block..=end_block {
            let block_path = block_cache.block_path(&block_entry, block);
            let data = std::fs::read(&block_path)?;
            let block_start = block * block_size;
            let slice_start = offset.saturating_sub(block_start) as usize;
            let slice_end = ((offset + size as u64).saturating_sub(block_start)) as usize;
            if slice_start >= data.len() {
                continue;
            }
            let end = slice_end.min(data.len());
            buffer.extend_from_slice(&data[slice_start..end]);
        }
        Ok(buffer)
    }
}

pub(crate) fn system_time_from_datetime(dt: DateTime<Utc>) -> SystemTime {
    let secs = dt.timestamp();
    if secs <= 0 {
        return SystemTime::UNIX_EPOCH;
    }
    let nanos = dt.timestamp_subsec_nanos() as u64;
    SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(secs as u64)
        + std::time::Duration::from_nanos(nanos)
}

fn read_from_slice(data: &[u8], offset: u64, size: u32) -> Vec<u8> {
    let off = offset as usize;
    if off >= data.len() {
        return Vec::new();
    }
    let end = (off + size as usize).min(data.len());
    data[off..end].to_vec()
}

fn read_from_path(path: &Path, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = vec![0u8; size as usize];
    let read = file.read(&mut buffer)?;
    buffer.truncate(read);
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Manifest;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn read_range_zero_size_returns_empty() {
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![ManifestEntry::test_remote(
                "data/file.txt",
                "http://127.0.0.1:1",
                Some(10),
            )],
        };
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(
            cache_dir.path(),
            Arc::new(crate::cache::LruTracker::new(u64::MAX)),
        );
        let block_cache = Some(BlockCache::new(cache_dir.path().join("blocks"), 4096));
        let downloader = HttpClient::new(0, 0, 100, 100).expect("client");
        let fs = FetchFs::new(
            cache,
            block_cache,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(1)),
            Arc::new(crate::download_pool::DownloadPool::new(1)),
            false,
        );

        let entry = manifest.entries.first().expect("entry");
        let data = fs.read_range(entry, 0, 0).expect("read");
        assert!(data.is_empty());
    }

    #[test]
    fn read_range_inline_returns_data() {
        let inline = ManifestEntry::test_inline("hello.txt", b"Hello, world!");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(
            cache_dir.path(),
            Arc::new(crate::cache::LruTracker::new(u64::MAX)),
        );
        let downloader = HttpClient::new(0, 0, 100, 100).expect("client");
        let fs = FetchFs::new(
            cache,
            None,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(1)),
            Arc::new(crate::download_pool::DownloadPool::new(1)),
            false,
        );

        assert_eq!(fs.read_range(&inline, 0, 100).unwrap(), b"Hello, world!");
        assert_eq!(fs.read_range(&inline, 7, 5).unwrap(), b"world");
        assert_eq!(fs.read_range(&inline, 100, 10).unwrap(), b"");
        assert_eq!(fs.read_range(&inline, 0, 0).unwrap(), b"");
    }

    #[test]
    fn ensure_meta_inline_returns_size() {
        let inline = ManifestEntry::test_inline("f.txt", b"abcdef");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(
            cache_dir.path(),
            Arc::new(crate::cache::LruTracker::new(u64::MAX)),
        );
        let downloader = HttpClient::new(0, 0, 100, 100).expect("client");
        let fs = FetchFs::new(
            cache,
            None,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(1)),
            Arc::new(crate::download_pool::DownloadPool::new(1)),
            false,
        );

        let meta = fs.ensure_meta(&inline).expect("meta");
        assert_eq!(meta.size, Some(6));
    }

    #[test]
    fn block_cache_serializes_downloads_before_lock() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        listener.set_nonblocking(true).expect("nonblocking");

        let calls = Arc::new(AtomicUsize::new(0));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = mpsc::channel();
        let calls_server = Arc::clone(&calls);

        let server_thread = thread::spawn(move || {
            let body = b"0123456789abcdef";
            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                let (mut stream, _) = match listener.accept() {
                    Ok(conn) => conn,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };
                let mut buffer = [0u8; 2048];
                let _ = stream.read(&mut buffer);
                let call = calls_server.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    let _ = ready_tx.send(());
                    let _ = release_rx.recv();
                }
                let header = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-15/16\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(body);
            }
        });

        let url = format!("http://{}", addr);
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![ManifestEntry::test_remote("data/file.txt", &url, None)],
        };
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(
            cache_dir.path(),
            Arc::new(crate::cache::LruTracker::new(u64::MAX)),
        );
        let block_cache = Some(BlockCache::new(cache_dir.path().join("blocks"), 4096));
        let downloader = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let fs = FetchFs::new(
            cache,
            block_cache,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(1)),
            Arc::new(DownloadPool::new(1)),
            false,
        );
        let fs = Arc::new(fs);
        let fs_first = Arc::clone(&fs);
        let fs_second = Arc::clone(&fs);
        let entry = manifest.entries[0].clone();
        let entry_first = entry.clone();
        let entry_second = entry.clone();

        let first =
            thread::spawn(move || fs_first.read_range(&entry_first, 0, 4).expect("read first"));

        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("ready");

        let second = thread::spawn(move || {
            fs_second
                .read_range(&entry_second, 0, 4)
                .expect("read second")
        });

        thread::sleep(Duration::from_millis(50));
        let _ = release_tx.send(());

        let first_data = first.join().expect("first join");
        let second_data = second.join().expect("second join");
        assert_eq!(first_data, b"0123");
        assert_eq!(second_data, b"0123");

        let _ = stop_tx.send(());
        let _ = server_thread.join();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn start_multi_file_server(
        file_sizes: &[usize],
    ) -> (
        std::net::SocketAddr,
        Arc<AtomicUsize>,
        mpsc::Sender<()>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        listener.set_nonblocking(true).expect("nonblocking");

        let total_calls = Arc::new(AtomicUsize::new(0));
        let (stop_tx, stop_rx) = mpsc::channel();
        let total_calls_server = Arc::clone(&total_calls);

        let bodies: Vec<Vec<u8>> = file_sizes
            .iter()
            .enumerate()
            .map(|(i, &sz)| vec![b'a' + i as u8; sz])
            .collect();
        let n_files = bodies.len();

        let handle = thread::spawn(move || {
            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                let (mut stream, _) = match listener.accept() {
                    Ok(conn) => conn,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };
                let mut buffer = [0u8; 4096];
                let n = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..n]);
                total_calls_server.fetch_add(1, Ordering::SeqCst);

                let file_idx = request
                    .lines()
                    .next()
                    .and_then(|line| {
                        let path = line.split_whitespace().nth(1)?;
                        let name = path.trim_start_matches('/');
                        name.strip_prefix("file_")
                            .and_then(|s| s.parse::<usize>().ok())
                    })
                    .unwrap_or(0)
                    .min(n_files.saturating_sub(1));

                let body = &bodies[file_idx];
                let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(body);
            }
        });

        (addr, total_calls, stop_tx, handle)
    }

    #[test]
    fn lru_evicts_least_recently_used_file() {
        let (addr, total_calls, stop_tx, server_handle) = start_multi_file_server(&[100, 100, 100]);

        let mtime = Some(chrono::Utc::now());
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![
                ManifestEntry::test_remote_with_mtime(
                    "file_0",
                    &format!("http://{}/file_0", addr),
                    Some(100),
                    mtime,
                ),
                ManifestEntry::test_remote_with_mtime(
                    "file_1",
                    &format!("http://{}/file_1", addr),
                    Some(100),
                    mtime,
                ),
                ManifestEntry::test_remote_with_mtime(
                    "file_2",
                    &format!("http://{}/file_2", addr),
                    Some(100),
                    mtime,
                ),
            ],
        };
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let tracker = Arc::new(crate::cache::LruTracker::new(250));
        let cache = Cache::new(cache_dir.path(), Arc::clone(&tracker));
        let downloader = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let fs = FetchFs::new(
            cache,
            None,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(4)),
            Arc::new(DownloadPool::new(4)),
            false,
        );

        for i in 0..3 {
            fs.ensure_cached(&manifest.entries[i]).expect("cache");
        }

        let entry0 = fs.cache_entry_for(&manifest.entries[0]).expect("entry");
        let entry1 = fs.cache_entry_for(&manifest.entries[1]).expect("entry");
        let entry2 = fs.cache_entry_for(&manifest.entries[2]).expect("entry");
        assert!(
            !entry0.data_path.exists(),
            "file_0 should be evicted (oldest)"
        );
        assert!(entry1.data_path.exists(), "file_1 should remain");
        assert!(entry2.data_path.exists(), "file_2 should remain");
        assert_eq!(total_calls.load(Ordering::SeqCst), 3);

        fs.ensure_cached(&manifest.entries[0]).expect("re-cache");
        assert_eq!(
            total_calls.load(Ordering::SeqCst),
            4,
            "evicted file should be re-downloaded"
        );

        let _ = stop_tx.send(());
        let _ = server_handle.join();
    }

    #[test]
    fn lru_touch_on_read_keeps_file_alive() {
        let (addr, _total_calls, stop_tx, server_handle) =
            start_multi_file_server(&[100, 100, 100]);

        let mtime = Some(chrono::Utc::now());
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![
                ManifestEntry::test_remote_with_mtime(
                    "file_0",
                    &format!("http://{}/file_0", addr),
                    Some(100),
                    mtime,
                ),
                ManifestEntry::test_remote_with_mtime(
                    "file_1",
                    &format!("http://{}/file_1", addr),
                    Some(100),
                    mtime,
                ),
                ManifestEntry::test_remote_with_mtime(
                    "file_2",
                    &format!("http://{}/file_2", addr),
                    Some(100),
                    mtime,
                ),
            ],
        };
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let tracker = Arc::new(crate::cache::LruTracker::new(250));
        let cache = Cache::new(cache_dir.path(), Arc::clone(&tracker));
        let downloader = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let fs = FetchFs::new(
            cache,
            None,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(4)),
            Arc::new(DownloadPool::new(4)),
            false,
        );

        fs.ensure_cached(&manifest.entries[0]).expect("cache A");
        fs.ensure_cached(&manifest.entries[1]).expect("cache B");

        fs.read_range(&manifest.entries[0], 0, 10)
            .expect("touch A via read");

        fs.ensure_cached(&manifest.entries[2]).expect("cache C");

        let entry0 = fs.cache_entry_for(&manifest.entries[0]).expect("entry");
        let entry1 = fs.cache_entry_for(&manifest.entries[1]).expect("entry");
        let entry2 = fs.cache_entry_for(&manifest.entries[2]).expect("entry");
        assert!(
            entry0.data_path.exists(),
            "file_0 should survive (was touched)"
        );
        assert!(!entry1.data_path.exists(), "file_1 should be evicted (LRU)");
        assert!(entry2.data_path.exists(), "file_2 should remain");

        let _ = stop_tx.send(());
        let _ = server_handle.join();
    }
}
