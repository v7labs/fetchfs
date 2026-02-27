use std::collections::{BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request,
};
use libc::{EIO, ENOENT, EROFS};

use crate::filesystem::FetchFs;
use crate::manifest::{Manifest, ManifestEntry};
use crate::syscall_trace::SyscallTracer;
use crate::tree::PathTree;

const TTL: Duration = Duration::from_secs(1);
#[cfg(target_os = "macos")]
const NO_XATTR: i32 = libc::ENOATTR;
#[cfg(not(target_os = "macos"))]
const NO_XATTR: i32 = libc::ENOTSUP;

const ROOT_INO: u64 = fuser::FUSE_ROOT_ID;
const FETCHFS_JSON_INO: u64 = u64::MAX;
const FETCHFS_JSON_NAME: &str = ".fetchfs.json";

#[derive(Debug, Clone)]
struct NodeInfo {
    path: String,
    kind: FileType,
    entry_index: Option<usize>,
}

pub struct MountState {
    pub manifest: Manifest,
    pub tree: PathTree,
    pub generation: u64,
    path_to_inode: HashMap<String, u64>,
    inode_to_node: HashMap<u64, NodeInfo>,
    fetchfs_json_content: Vec<u8>,
}

impl MountState {
    pub fn build(manifest: Manifest, tree: PathTree, generation: u64) -> Self {
        let mut path_to_inode = HashMap::new();
        let mut inode_to_node = HashMap::new();
        let mut next_inode = 2u64;

        path_to_inode.insert(String::new(), ROOT_INO);
        inode_to_node.insert(
            ROOT_INO,
            NodeInfo {
                path: String::new(),
                kind: FileType::Directory,
                entry_index: None,
            },
        );

        let mut dir_paths = BTreeSet::new();
        for (idx, entry) in manifest.entries.iter().enumerate() {
            let segments: Vec<_> = entry.path.split('/').collect();
            let mut current = String::new();
            for &segment in &segments[..segments.len().saturating_sub(1)] {
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(segment);
                dir_paths.insert(current.clone());
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

        let fetchfs_json_content = build_fetchfs_json(manifest.version, &manifest.revision);

        Self {
            manifest,
            tree,
            generation,
            path_to_inode,
            inode_to_node,
            fetchfs_json_content,
        }
    }

    fn inode_for_path(&self, path: &str) -> Option<u64> {
        self.path_to_inode.get(path).copied()
    }

    fn node_for_inode(&self, inode: u64) -> Option<&NodeInfo> {
        self.inode_to_node.get(&inode)
    }

    fn path_for_inode(&self, inode: u64) -> Option<&str> {
        self.inode_to_node.get(&inode).map(|n| n.path.as_str())
    }
}

fn build_fetchfs_json(version: u32, revision: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"version": version, "revision": revision})).unwrap()
}

pub struct FuseFS {
    fs: FetchFs,
    state: Arc<ArcSwap<MountState>>,
    handles: Mutex<HashMap<u64, Option<ManifestEntry>>>,
    next_fh: AtomicU64,
    tracer: Option<SyscallTracer>,
    uid: u32,
    gid: u32,
}

impl FuseFS {
    pub fn new(
        fs: FetchFs,
        state: Arc<ArcSwap<MountState>>,
        tracer: Option<SyscallTracer>,
    ) -> Self {
        // SAFETY: geteuid/getegid are side-effect-free and do not violate memory safety.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        Self {
            fs,
            state,
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            tracer,
            uid,
            gid,
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

    fn resolve_path_for_trace<'a>(&self, ino: u64, state: &'a MountState) -> Option<&'a str> {
        if ino == FETCHFS_JSON_INO {
            Some(FETCHFS_JSON_NAME)
        } else {
            state.path_for_inode(ino)
        }
    }

    fn make_attr(&self, ino: u64, kind: FileType, size: u64, mtime: SystemTime) -> FileAttr {
        FileAttr {
            ino,
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
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn file_attr(&self, inode: u64, entry: Option<&ManifestEntry>) -> FileAttr {
        let kind = entry
            .map(|_| FileType::RegularFile)
            .unwrap_or(FileType::Directory);
        let meta = entry.and_then(|e| self.fs.ensure_meta(e));
        let size = entry
            .map(|e| {
                self.cached_size(e)
                    .or_else(|| meta.as_ref().and_then(|m| m.size))
                    .or(e.size)
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        let mtime = entry
            .map(|e| {
                meta.and_then(|m| m.mtime)
                    .map(crate::filesystem::system_time_from_datetime)
                    .unwrap_or_else(|| self.fs.mtime_for(e))
            })
            .unwrap_or_else(|| self.fs.mount_system_time());
        self.make_attr(inode, kind, size, mtime)
    }

    fn fetchfs_json_attr(&self, state: &MountState) -> FileAttr {
        self.make_attr(
            FETCHFS_JSON_INO,
            FileType::RegularFile,
            state.fetchfs_json_content.len() as u64,
            self.fs.mount_system_time(),
        )
    }

    fn cached_size(&self, entry: &ManifestEntry) -> Option<u64> {
        if let Some(data) = entry.inline_data() {
            return Some(data.len() as u64);
        }
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
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            let parent_path = state.path_for_inode(parent);
            tracer.lookup(parent, name.to_string_lossy().as_ref(), parent_path);
        }
        let parent_node = match state.node_for_inode(parent) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let name_str = match name.to_str() {
            Some(name) => name,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if parent == ROOT_INO && name_str == FETCHFS_JSON_NAME {
            let attr = self.fetchfs_json_attr(&state);
            reply.entry(&TTL, &attr, state.generation);
            return;
        }
        let path = if parent_node.path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_node.path, name_str)
        };
        let inode = match state.inode_for_path(&path) {
            Some(inode) => inode,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let node = state.node_for_inode(inode).expect("inode exists");
        let entry = node
            .entry_index
            .and_then(|idx| state.manifest.entries.get(idx));
        let attr = self.file_attr(inode, entry);
        reply.entry(&TTL, &attr, state.generation);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.getattr(ino, fh, self.resolve_path_for_trace(ino, &state));
        }
        if ino == FETCHFS_JSON_INO {
            let attr = self.fetchfs_json_attr(&state);
            reply.attr(&TTL, &attr);
            return;
        }
        let node = match state.node_for_inode(ino) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let entry = node
            .entry_index
            .and_then(|idx| state.manifest.entries.get(idx));
        let attr = self.file_attr(ino, entry);
        reply.attr(&TTL, &attr);
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.access(ino, mask, self.resolve_path_for_trace(ino, &state));
        }
        if ino != FETCHFS_JSON_INO && state.node_for_inode(ino).is_none() {
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
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.getxattr(
                ino,
                name.to_string_lossy().as_ref(),
                size,
                self.resolve_path_for_trace(ino, &state),
            );
        }
        if ino != FETCHFS_JSON_INO && state.node_for_inode(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        reply.error(NO_XATTR);
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.listxattr(ino, size, self.resolve_path_for_trace(ino, &state));
        }
        if ino != FETCHFS_JSON_INO && state.node_for_inode(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        reply.size(0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.readdir(ino, fh, offset, state.path_for_inode(ino));
        }
        let node = match state.node_for_inode(ino) {
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
        entries.push((ROOT_INO, FileType::Directory, OsString::from("..")));

        if let Some(children) = state.tree.list_dir(&node.path) {
            for child in children {
                let child_path = if node.path.is_empty() {
                    child.name.clone()
                } else {
                    format!("{}/{}", node.path, child.name)
                };
                if let Some(child_ino) = state.inode_for_path(&child_path) {
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

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.open(ino, flags, self.resolve_path_for_trace(ino, &state));
        }
        if (flags & libc::O_ACCMODE) != libc::O_RDONLY {
            reply.error(EROFS);
            return;
        }
        if ino == FETCHFS_JSON_INO {
            let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
            let mut handles = self.handles.lock().expect("handles lock");
            handles.insert(fh, None);
            reply.opened(fh, 0);
            return;
        }
        let node = match state.node_for_inode(ino) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let entry = match node
            .entry_index
            .and_then(|idx| state.manifest.entries.get(idx))
        {
            Some(entry) => entry.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        let mut handles = self.handles.lock().expect("handles lock");
        handles.insert(fh, Some(entry));
        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if let Some(tracer) = &self.tracer {
            let state = self.state.load();
            tracer.read(
                ino,
                fh,
                offset,
                size,
                self.resolve_path_for_trace(ino, &state),
            );
        }
        let handle = {
            let handles = self.handles.lock().expect("handles lock");
            match handles.get(&fh) {
                Some(h) => h.clone(),
                None => {
                    reply.error(EIO);
                    return;
                }
            }
        };
        if offset < 0 {
            reply.error(EIO);
            return;
        }
        match handle {
            None => {
                let state = self.state.load();
                let content = &state.fetchfs_json_content;
                let off = offset as usize;
                if off >= content.len() {
                    reply.data(&[]);
                } else {
                    let end = (off + size as usize).min(content.len());
                    reply.data(&content[off..end]);
                }
            }
            Some(entry) => match self.fs.read_range(&entry, offset as u64, size) {
                Ok(data) => reply.data(&data),
                Err(_) => reply.error(EIO),
            },
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.release(
                ino,
                fh,
                flags,
                flush,
                self.resolve_path_for_trace(ino, &state),
            );
        }
        let mut handles = self.handles.lock().expect("handles lock");
        handles.remove(&fh);
        reply.ok();
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.flush(
                ino,
                fh,
                lock_owner,
                self.resolve_path_for_trace(ino, &state),
            );
        }
        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let state = self.state.load();
        if let Some(tracer) = &self.tracer {
            tracer.write(
                ino,
                fh,
                offset,
                data.len() as u32,
                flags,
                self.resolve_path_for_trace(ino, &state),
            );
        }
        reply.error(EROFS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inode_mapping_includes_root_dirs_and_files() {
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![
                ManifestEntry::test_remote("data/file.txt", "http://127.0.0.1:1", Some(1)),
                ManifestEntry::test_remote("data/sub/other.txt", "http://127.0.0.1:1", Some(2)),
            ],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = MountState::build(manifest, tree, 0);

        assert!(state.inode_for_path("").is_some());
        assert!(state.inode_for_path("data").is_some());
        assert!(state.inode_for_path("data/sub").is_some());
        assert!(state.inode_for_path("data/file.txt").is_some());
        assert!(state.inode_for_path("data/sub/other.txt").is_some());
    }

    #[test]
    fn inode_mapping_works_with_inline_entries() {
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![
                ManifestEntry::test_inline("data/inline.txt", b"hello"),
                ManifestEntry::test_remote("data/remote.txt", "http://127.0.0.1:1", Some(10)),
            ],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = MountState::build(manifest, tree, 0);

        assert!(state.inode_for_path("data/inline.txt").is_some());
        assert!(state.inode_for_path("data/remote.txt").is_some());
    }

    #[test]
    fn fetchfs_json_content_with_revision() {
        let manifest = Manifest {
            version: 1,
            revision: "abc123".to_string(),
            entries: vec![],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = MountState::build(manifest, tree, 0);

        let content: serde_json::Value =
            serde_json::from_slice(&state.fetchfs_json_content).expect("json");
        assert_eq!(content["version"], 1);
        assert_eq!(content["revision"], "abc123");
    }

    #[test]
    fn fetchfs_json_content_empty_revision() {
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = MountState::build(manifest, tree, 0);

        let content: serde_json::Value =
            serde_json::from_slice(&state.fetchfs_json_content).expect("json");
        assert_eq!(content["version"], 1);
        assert_eq!(content["revision"], "");
    }

    #[test]
    fn fetchfs_json_not_listed_in_root() {
        let manifest = Manifest {
            version: 1,
            revision: "v1".to_string(),
            entries: vec![ManifestEntry::test_remote(
                "file.txt",
                "http://127.0.0.1:1",
                Some(1),
            )],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = MountState::build(manifest, tree, 0);

        let root_entries = state.tree.list_dir("").expect("root listing");
        assert!(
            root_entries.iter().all(|e| e.name != FETCHFS_JSON_NAME),
            ".fetchfs.json should not appear in directory listing"
        );
    }

    #[test]
    fn fetchfs_json_lookup_resolves_in_root() {
        let manifest = Manifest {
            version: 1,
            revision: "v1".to_string(),
            entries: vec![ManifestEntry::test_remote(
                "file.txt",
                "http://127.0.0.1:1",
                Some(1),
            )],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = MountState::build(manifest, tree, 0);

        assert!(
            state.inode_for_path(FETCHFS_JSON_NAME).is_none(),
            ".fetchfs.json should not be in the inode map"
        );
    }

    #[test]
    fn mount_state_rebuild_updates_revision_and_files() {
        let manifest_v1 = Manifest {
            version: 1,
            revision: "rev1".to_string(),
            entries: vec![ManifestEntry::test_remote(
                "old.txt",
                "http://127.0.0.1:1/old",
                Some(1),
            )],
        };
        let tree_v1 = manifest_v1.build_tree().expect("tree");
        let state_v1 = MountState::build(manifest_v1, tree_v1, 0);

        assert!(state_v1.inode_for_path("old.txt").is_some());
        assert!(state_v1.inode_for_path("new.txt").is_none());
        let v1_json: serde_json::Value =
            serde_json::from_slice(&state_v1.fetchfs_json_content).expect("json");
        assert_eq!(v1_json["revision"], "rev1");

        let manifest_v2 = Manifest {
            version: 1,
            revision: "rev2".to_string(),
            entries: vec![ManifestEntry::test_remote(
                "new.txt",
                "http://127.0.0.1:1/new",
                Some(2),
            )],
        };
        let tree_v2 = manifest_v2.build_tree().expect("tree");
        let state_v2 = MountState::build(manifest_v2, tree_v2, 1);

        assert!(state_v2.inode_for_path("old.txt").is_none());
        assert!(state_v2.inode_for_path("new.txt").is_some());
        let v2_json: serde_json::Value =
            serde_json::from_slice(&state_v2.fetchfs_json_content).expect("json");
        assert_eq!(v2_json["revision"], "rev2");
    }

    #[test]
    fn arc_swap_state_swap_is_atomic() {
        let manifest = Manifest {
            version: 1,
            revision: "initial".to_string(),
            entries: vec![ManifestEntry::test_remote(
                "file.txt",
                "http://127.0.0.1:1",
                Some(1),
            )],
        };
        let tree = manifest.build_tree().expect("tree");
        let state = Arc::new(ArcSwap::from_pointee(MountState::build(manifest, tree, 0)));

        let loaded = state.load();
        let json: serde_json::Value =
            serde_json::from_slice(&loaded.fetchfs_json_content).expect("json");
        assert_eq!(json["revision"], "initial");
        assert!(loaded.inode_for_path("file.txt").is_some());

        let new_manifest = Manifest {
            version: 1,
            revision: "updated".to_string(),
            entries: vec![ManifestEntry::test_remote(
                "other.txt",
                "http://127.0.0.1:1",
                Some(2),
            )],
        };
        let new_tree = new_manifest.build_tree().expect("tree");
        state.store(Arc::new(MountState::build(new_manifest, new_tree, 1)));

        let loaded = state.load();
        let json: serde_json::Value =
            serde_json::from_slice(&loaded.fetchfs_json_content).expect("json");
        assert_eq!(json["revision"], "updated");
        assert!(loaded.inode_for_path("file.txt").is_none());
        assert!(loaded.inode_for_path("other.txt").is_some());
    }
}
