use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use fs2::FileExt;
use tracing::debug;

use crate::cache::{BlockCache, BlockMeta, Cache, CacheEntry, CacheMeta};
use crate::download_gate::DownloadGate;
use crate::http::{HttpClient, RangeStatus};
use crate::manifest::{Manifest, ManifestEntry};
use crate::tree::{DirEntry, PathTree};

struct LockCleanup {
    path: PathBuf,
}

impl Drop for LockCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub struct FetchFs {
    pub manifest: Manifest,
    pub tree: PathTree,
    pub cache: Cache,
    pub block_cache: Option<BlockCache>,
    pub downloader: Arc<HttpClient>,
    pub download_gate: Arc<DownloadGate>,
    pub streaming: bool,
    pub mount_time: DateTime<Utc>,
}

impl FetchFs {
    pub fn new(
        manifest: Manifest,
        tree: PathTree,
        cache: Cache,
        block_cache: Option<BlockCache>,
        downloader: Arc<HttpClient>,
        download_gate: Arc<DownloadGate>,
        streaming: bool,
    ) -> Self {
        Self {
            manifest,
            tree,
            cache,
            block_cache,
            downloader,
            download_gate,
            streaming,
            mount_time: Utc::now(),
        }
    }

    pub fn list_dir(&self, path: &str) -> Option<Vec<DirEntry>> {
        self.tree.list_dir(path)
    }

    #[allow(dead_code)]
    pub fn lookup_entry(&self, path: &str) -> Option<&ManifestEntry> {
        self.tree
            .lookup(path)
            .and_then(|idx| self.manifest.entries.get(idx))
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
            if let Some(total) = known_size {
                if (size as u64) >= total {
                    let (_entry, data_path) = self.ensure_cached(entry)?;
                    return read_from_path(&data_path, offset, size);
                }
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
                let lock_path = block_path.with_extension("lock");
                let lock_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&lock_path)?;
                lock_file.lock_exclusive()?;
                let _cleanup = LockCleanup {
                    path: lock_path.clone(),
                };
                if block_path.exists() {
                    continue;
                }
                let start = block * block_size;
                let end = start + block_size - 1;
                let _permit = self.download_gate.acquire();
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
                        return read_from_path(&data_path, offset, size);
                    }
                    RangeStatus::Empty => {
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
            false,
        );

        let entry = fs.manifest.entries.first().expect("entry");
        let data = fs.read_range(entry, 0, 0).expect("read");
        assert!(data.is_empty());
    }
}
