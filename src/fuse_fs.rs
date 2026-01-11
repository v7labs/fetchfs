use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request,
};
use libc::{EIO, ENOENT, EROFS};

use crate::filesystem::FetchFs;
use crate::manifest::ManifestEntry;

// Keep attrs fresh while files are fetched lazily.
const TTL: Duration = Duration::from_secs(1);
#[cfg(target_os = "macos")]
const NO_XATTR: i32 = libc::ENOATTR;
#[cfg(not(target_os = "macos"))]
const NO_XATTR: i32 = libc::ENOTSUP;

#[derive(Debug, Clone)]
struct NodeInfo {
    path: String,
    kind: FileType,
    entry_index: Option<usize>,
}

pub struct FuseFS {
    fs: FetchFs,
    path_to_inode: HashMap<String, u64>,
    inode_to_node: HashMap<u64, NodeInfo>,
    handles: Mutex<HashMap<u64, usize>>,
    next_fh: AtomicU64,
}

impl FuseFS {
    pub fn new(fs: FetchFs) -> Self {
        let mut path_to_inode = HashMap::new();
        let mut inode_to_node = HashMap::new();
        let mut next_inode = 2u64;

        path_to_inode.insert(String::new(), 1);
        inode_to_node.insert(
            1,
            NodeInfo {
                path: String::new(),
                kind: FileType::Directory,
                entry_index: None,
            },
        );

        let mut dir_paths = Vec::new();
        for (idx, entry) in fs.manifest.entries.iter().enumerate() {
            let mut current = String::new();
            for segment in entry
                .path
                .split('/')
                .take(entry.path.split('/').count() - 1)
            {
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(segment);
                dir_paths.push(current.clone());
            }
            path_to_inode.insert(entry.path.clone(), next_inode);
            inode_to_node.insert(
                next_inode,
                NodeInfo {
                    path: entry.path.clone(),
                    kind: FileType::RegularFile,
                    entry_index: Some(idx),
                },
            );
            next_inode += 1;
        }

        dir_paths.sort();
        dir_paths.dedup();
        for path in dir_paths {
            if path_to_inode.contains_key(&path) {
                continue;
            }
            path_to_inode.insert(path.clone(), next_inode);
            inode_to_node.insert(
                next_inode,
                NodeInfo {
                    path,
                    kind: FileType::Directory,
                    entry_index: None,
                },
            );
            next_inode += 1;
        }

        Self {
            fs,
            path_to_inode,
            inode_to_node,
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        }
    }

    pub fn mount_options(allow_other: bool) -> Vec<MountOption> {
        let mut options = vec![
            MountOption::RO,
            MountOption::FSName("fetchfs".into()),
            MountOption::AutoUnmount,
        ];
        if allow_other {
            options.push(MountOption::AllowOther);
        }
        options
    }

    fn inode_for_path(&self, path: &str) -> Option<u64> {
        self.path_to_inode.get(path).copied()
    }

    fn node_for_inode(&self, inode: u64) -> Option<&NodeInfo> {
        self.inode_to_node.get(&inode)
    }

    fn file_attr(&self, inode: u64, entry: Option<&ManifestEntry>) -> FileAttr {
        let kind = entry
            .map(|_| FileType::RegularFile)
            .unwrap_or(FileType::Directory);
        let meta = entry.and_then(|entry| self.fs.ensure_meta(entry));
        let size = if let Some(entry) = entry {
            if let Some(cached) = self.cached_size(entry) {
                cached
            } else {
                meta.as_ref()
                    .and_then(|meta| meta.size)
                    .or(entry.size)
                    .unwrap_or(0)
            }
        } else {
            0
        };
        let mtime = if let Some(entry) = entry {
            meta.and_then(|meta| meta.mtime)
                .map(crate::filesystem::system_time_from_datetime)
                .unwrap_or_else(|| self.fs.mtime_for(entry))
        } else {
            self.fs.mount_system_time()
        };
        FileAttr {
            ino: inode,
            size,
            blocks: 1,
            atime: SystemTime::UNIX_EPOCH,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm: if kind == FileType::Directory {
                0o555
            } else {
                0o444
            },
            nlink: 1,
            // SAFETY: libc::geteuid/getegid are side-effect-free and do not violate memory safety.
            uid: unsafe { libc::geteuid() },
            // SAFETY: libc::geteuid/getegid are side-effect-free and do not violate memory safety.
            gid: unsafe { libc::getegid() },
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn cached_size(&self, entry: &ManifestEntry) -> Option<u64> {
        let cache_entry = self.fs.cache_entry_for(entry).ok()?;
        if !cache_entry.data_path.exists() {
            return None;
        }
        std::fs::metadata(cache_entry.data_path)
            .ok()
            .map(|m| m.len())
    }
}

impl Filesystem for FuseFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_node = match self.node_for_inode(parent) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let name = match name.to_str() {
            Some(name) => name,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let path = if parent_node.path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_node.path, name)
        };
        let inode = match self.inode_for_path(&path) {
            Some(inode) => inode,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let node = self.node_for_inode(inode).expect("inode exists");
        let entry = node
            .entry_index
            .and_then(|idx| self.fs.manifest.entries.get(idx));
        let attr = self.file_attr(inode, entry);
        reply.entry(&TTL, &attr, 0);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let node = match self.node_for_inode(ino) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let entry = node
            .entry_index
            .and_then(|idx| self.fs.manifest.entries.get(idx));
        let attr = self.file_attr(ino, entry);
        reply.attr(&TTL, &attr);
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        if self.node_for_inode(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        if (mask & libc::W_OK) != 0 {
            reply.error(EROFS);
            return;
        }
        reply.ok();
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        if self.node_for_inode(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        reply.error(NO_XATTR);
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, _size: u32, reply: ReplyXattr) {
        if self.node_for_inode(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        reply.size(0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let node = match self.node_for_inode(ino) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if node.kind != FileType::Directory {
            reply.error(ENOENT);
            return;
        }
        let mut entries: Vec<(u64, FileType, OsString)> = Vec::new();
        entries.push((ino, FileType::Directory, OsString::from(".")));
        entries.push((1, FileType::Directory, OsString::from("..")));

        if let Some(children) = self.fs.list_dir(&node.path) {
            for child in children {
                let child_path = if node.path.is_empty() {
                    child.name.clone()
                } else {
                    format!("{}/{}", node.path, child.name)
                };
                if let Some(child_ino) = self.inode_for_path(&child_path) {
                    let kind = if child.is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    entries.push((child_ino, kind, OsString::from(child.name)));
                }
            }
        }

        let start = if offset < 0 { 0 } else { offset as usize };
        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(start) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if (_flags & libc::O_ACCMODE) != libc::O_RDONLY {
            reply.error(EROFS);
            return;
        }
        let node = match self.node_for_inode(ino) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let entry_index = match node.entry_index {
            Some(index) => index,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        let mut handles = self.handles.lock().expect("handles lock");
        handles.insert(fh, entry_index);
        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let entry_index = {
            let handles = self.handles.lock().expect("handles lock");
            match handles.get(&fh) {
                Some(index) => *index,
                None => {
                    reply.error(EIO);
                    return;
                }
            }
        };
        let entry = match self.fs.manifest.entries.get(entry_index) {
            Some(entry) => entry,
            None => {
                reply.error(EIO);
                return;
            }
        };
        if offset < 0 {
            reply.error(EIO);
            return;
        }
        match self.fs.read_range(entry, offset as u64, size) {
            Ok(data) => reply.data(&data),
            Err(_) => reply.error(EIO),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let mut handles = self.handles.lock().expect("handles lock");
        handles.remove(&fh);
        reply.ok();
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::Cache;
    use crate::download_gate::DownloadGate;
    use crate::filesystem::FetchFs;
    use crate::http::HttpClient;
    use crate::manifest::{Manifest, ManifestEntry};

    #[test]
    fn inode_mapping_includes_root_dirs_and_files() {
        let manifest = Manifest {
            version: 1,
            entries: vec![
                ManifestEntry {
                    path: "data/file.txt".to_string(),
                    url: "http://127.0.0.1:1".to_string(),
                    size: Some(1),
                    mtime: None,
                },
                ManifestEntry {
                    path: "data/sub/other.txt".to_string(),
                    url: "http://127.0.0.1:1".to_string(),
                    size: Some(2),
                    mtime: None,
                },
            ],
        };
        let tree = manifest.build_tree().expect("tree");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let cache = Cache::new(cache_dir.path());
        let http = HttpClient::new(0, 0, 100, 100).expect("client");
        let fs = FetchFs::new(
            manifest,
            tree,
            cache,
            None,
            std::sync::Arc::new(http),
            std::sync::Arc::new(DownloadGate::new(1)),
            std::sync::Arc::new(crate::download_pool::DownloadPool::new(1)),
            false,
        );
        let fuse = FuseFS::new(fs);

        assert!(fuse.inode_for_path("").is_some());
        assert!(fuse.inode_for_path("data").is_some());
        assert!(fuse.inode_for_path("data/sub").is_some());
        assert!(fuse.inode_for_path("data/file.txt").is_some());
        assert!(fuse.inode_for_path("data/sub/other.txt").is_some());
    }
}
