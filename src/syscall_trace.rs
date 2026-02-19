use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SyscallEvent<'a> {
    pub syscall: &'static str,
    pub timestamp_ns: u128,
    #[serde(flatten)]
    pub args: SyscallArgs<'a>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SyscallArgs<'a> {
    Lookup {
        parent: u64,
        name: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_path: Option<&'a str>,
    },
    Getattr {
        ino: u64,
        fh: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Access {
        ino: u64,
        mask: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Getxattr {
        ino: u64,
        name: &'a str,
        size: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Listxattr {
        ino: u64,
        size: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Readdir {
        ino: u64,
        fh: u64,
        offset: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Open {
        ino: u64,
        flags: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Read {
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Release {
        ino: u64,
        fh: u64,
        flags: i32,
        flush: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Flush {
        ino: u64,
        fh: u64,
        lock_owner: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
    Write {
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<&'a str>,
    },
}

pub struct SyscallTracer {
    socket: UnixDatagram,
}

impl SyscallTracer {
    pub fn new<P: AsRef<Path>>(socket_path: P) -> std::io::Result<Self> {
        let socket = UnixDatagram::unbound()?;
        socket.connect(socket_path)?;
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    fn timestamp_ns() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }

    pub fn trace(&self, syscall: &'static str, args: SyscallArgs<'_>) {
        let event = SyscallEvent {
            syscall,
            timestamp_ns: Self::timestamp_ns(),
            args,
        };
        if let Ok(json) = serde_json::to_vec(&event) {
            let _ = self.socket.send(&json);
        }
    }

    pub fn lookup(&self, parent: u64, name: &str, parent_path: Option<&str>) {
        self.trace("lookup", SyscallArgs::Lookup { parent, name, parent_path });
    }

    pub fn getattr(&self, ino: u64, fh: Option<u64>, path: Option<&str>) {
        self.trace("getattr", SyscallArgs::Getattr { ino, fh, path });
    }

    pub fn access(&self, ino: u64, mask: i32, path: Option<&str>) {
        self.trace("access", SyscallArgs::Access { ino, mask, path });
    }

    pub fn getxattr(&self, ino: u64, name: &str, size: u32, path: Option<&str>) {
        self.trace("getxattr", SyscallArgs::Getxattr { ino, name, size, path });
    }

    pub fn listxattr(&self, ino: u64, size: u32, path: Option<&str>) {
        self.trace("listxattr", SyscallArgs::Listxattr { ino, size, path });
    }

    pub fn readdir(&self, ino: u64, fh: u64, offset: i64, path: Option<&str>) {
        self.trace("readdir", SyscallArgs::Readdir { ino, fh, offset, path });
    }

    pub fn open(&self, ino: u64, flags: i32, path: Option<&str>) {
        self.trace("open", SyscallArgs::Open { ino, flags, path });
    }

    pub fn read(&self, ino: u64, fh: u64, offset: i64, size: u32, path: Option<&str>) {
        self.trace("read", SyscallArgs::Read { ino, fh, offset, size, path });
    }

    pub fn release(&self, ino: u64, fh: u64, flags: i32, flush: bool, path: Option<&str>) {
        self.trace("release", SyscallArgs::Release { ino, fh, flags, flush, path });
    }

    pub fn flush(&self, ino: u64, fh: u64, lock_owner: u64, path: Option<&str>) {
        self.trace("flush", SyscallArgs::Flush { ino, fh, lock_owner, path });
    }

    pub fn write(&self, ino: u64, fh: u64, offset: i64, size: u32, flags: i32, path: Option<&str>) {
        self.trace("write", SyscallArgs::Write { ino, fh, offset, size, flags, path });
    }
}
