use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use fs2::FileExt;
use tracing::debug;

use crate::cache::{BlockCache, BlockMeta, Cache, CacheEntry, CacheMeta};
use crate::download_gate::DownloadGate;
use crate::download_pool::DownloadPool;
use crate::http::{HttpClient, RangeStatus};
use crate::manifest::{Manifest, ManifestEntry};
use crate::tree::{DirEntry, PathTree};

pub struct FetchFs {
    pub manifest: Manifest,
    pub tree: PathTree,
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
        manifest: Manifest,
        tree: PathTree,
        cache: Cache,
        block_cache: Option<BlockCache>,
        downloader: Arc<HttpClient>,
        download_gate: Arc<DownloadGate>,
        download_pool: Arc<DownloadPool>,
        streaming: bool,
    ) -> Self {
        Self {
            manifest,
            tree,
            cache,
            block_cache,
            downloader,
            download_gate,
            download_pool,
            streaming,
            mount_time: Utc::now(),
        }
    }

    pub fn list_dir(&self, path: &str) -> Option<Vec<DirEntry>> {
        self.tree.list_dir(path)
    }

    pub fn cache_entry_for(&self, entry: &ManifestEntry) -> std::io::Result<CacheEntry> {
        self.cache.entry_for(&entry.url)
    }

    pub fn cache_meta_for(&self, entry: &ManifestEntry) -> CacheMeta {
        CacheMeta {
            url: entry.url.clone(),
            size: entry.size,
            mtime: entry.mtime,
        }
    }

    pub fn ensure_meta(&self, entry: &ManifestEntry) -> Option<CacheMeta> {
        let cache_entry = self.cache_entry_for(entry).ok()?;
        if cache_entry.meta_path.exists() {
            return Cache::load_meta(&cache_entry).ok();
        }
        let mut meta = self.cache_meta_for(entry);
        if meta.size.is_some() && meta.mtime.is_some() {
            let _ = Cache::write_meta(&cache_entry, &meta);
            return Some(meta);
        }
        debug!("fetching metadata for {}", entry.url);
        let _permit = self.download_gate.acquire();
        if let Ok(head) = self.downloader.head_with_fallback(&entry.url) {
            if meta.size.is_none() {
                meta.size = head.size;
            }
            if meta.mtime.is_none() {
                meta.mtime = head.mtime;
            }
            if Cache::write_meta(&cache_entry, &meta).is_ok() {
                debug!("cached metadata for {}", entry.url);
                return Some(meta);
            }
        }
        debug!("metadata unavailable for {}", entry.url);
        None
    }

    pub fn ensure_cached(&self, entry: &ManifestEntry) -> std::io::Result<(CacheEntry, PathBuf)> {
        let cache_entry = self.cache_entry_for(entry)?;
        let mut meta = self.cache_meta_for(entry);
        if meta.size.is_none() || meta.mtime.is_none() {
            let _permit = self.download_gate.acquire();
            if let Ok(head) = self.downloader.head_with_fallback(&entry.url) {
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
        if let Some(block_cache) = &self.block_cache {
            return self.read_range_block_cache(entry, offset, size, block_cache);
        }
        if !self.streaming && offset == 0 {
            let known_size = entry
                .size
                .or_else(|| self.ensure_meta(entry).and_then(|meta| meta.size));
            if let Some(total) = known_size && (size as u64) >= total {
                let (_entry, data_path) = self.ensure_cached(entry)?;
                return read_from_path(&data_path, offset, size);
            }
        }
        let cache_entry = self.cache_entry_for(entry)?;
        if cache_entry.data_path.exists() {
            return read_from_path(&cache_entry.data_path, offset, size);
        }

        let _permit = self.download_gate.acquire();
        let end = offset.saturating_add(size as u64).saturating_sub(1);
        let result = self.downloader.get_range(&entry.url, offset, end)?;
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
        let block_entry = block_cache.entry_for(&entry.url)?;
        let mut meta = if block_entry.meta_path.exists() {
            BlockCache::load_meta(&block_entry)?
        } else {
            BlockMeta {
                url: entry.url.clone(),
                size: entry.size,
                mtime: entry.mtime,
                block_size: block_cache.block_size(),
                no_range: false,
            }
        };
        if meta.url != entry.url || meta.block_size != block_cache.block_size() {
            meta = BlockMeta {
                url: entry.url.clone(),
                size: entry.size,
                mtime: entry.mtime,
                block_size: block_cache.block_size(),
                no_range: false,
            };
        }

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
                    drop(lock_file);
                    continue;
                }
                let start = block * block_size;
                let end = start + block_size - 1;
                let result = self.downloader.get_range(&entry.url, start, end)?;
                match result.status {
                    RangeStatus::Partial => {
                        if meta.size.is_none() {
                            meta.size = result.meta.size;
                        }
                        if meta.mtime.is_none() {
                            meta.mtime = result.meta.mtime;
                        }
                        let tmp_path = block_path.with_extension("tmp");
                        std::fs::write(&tmp_path, result.data)?;
                        std::fs::rename(&tmp_path, &block_path)?;
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
                            self.cache.store_bytes(&cache_entry, &result.data, &cache_meta)?;
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
    SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64)
        + std::time::Duration::from_nanos(nanos)
}

fn read_from_path(path: &PathBuf, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
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
            entries: vec![ManifestEntry {
                path: "data/file.txt".to_string(),
                url: "http://127.0.0.1:1".to_string(),
                size: Some(10),
                mtime: None,
            }],
        };
        let tree = manifest.build_tree().expect("tree");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(cache_dir.path());
        let block_cache = Some(BlockCache::new(cache_dir.path().join("blocks"), 4096));
        let downloader = HttpClient::new(0, 0, 100, 100).expect("client");
        let fs = FetchFs::new(
            manifest,
            tree,
            cache,
            block_cache,
            Arc::new(downloader),
            Arc::new(DownloadGate::new(1)),
            Arc::new(crate::download_pool::DownloadPool::new(1)),
            false,
        );

        let entry = fs.manifest.entries.first().expect("entry");
        let data = fs.read_range(entry, 0, 0).expect("read");
        assert!(data.is_empty());
    }

    #[test]
    fn block_cache_serializes_downloads_before_lock() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        listener
            .set_nonblocking(true)
            .expect("nonblocking");

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

        let manifest = Manifest {
            version: 1,
            entries: vec![ManifestEntry {
                path: "data/file.txt".to_string(),
                url: format!("http://{}", addr),
                size: None,
                mtime: None,
            }],
        };
        let tree = manifest.build_tree().expect("tree");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(cache_dir.path());
        let block_cache = Some(BlockCache::new(cache_dir.path().join("blocks"), 4096));
        let downloader = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let fs = FetchFs::new(
            manifest,
            tree,
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
        let entry = fs.manifest.entries[0].clone();
        let entry_first = entry.clone();
        let entry_second = entry.clone();

        let first = thread::spawn(move || {
            fs_first
                .read_range(&entry_first, 0, 4)
                .expect("read first")
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).expect("ready");

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
}
