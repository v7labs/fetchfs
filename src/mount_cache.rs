use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tracing::{info, warn};

const MOUNTS_DIR: &str = "mounts";
const PID_LOCK: &str = "pid.lock";

/// Create a new per-mount instance directory under `base/mounts/<id>/`.
/// Writes the current PID to `pid.lock` inside the directory.
/// Returns the instance directory path.
pub fn create_instance_dir(base: &Path) -> io::Result<PathBuf> {
    let id = generate_id();
    let instance = base.join(MOUNTS_DIR).join(id);
    fs::create_dir_all(&instance)?;

    let pid = std::process::id();
    fs::write(instance.join(PID_LOCK), pid.to_string())?;

    Ok(instance)
}

/// Remove the instance directory and all its contents.
pub fn remove_instance_dir(instance_dir: &Path) {
    if instance_dir.exists() {
        if let Err(err) = fs::remove_dir_all(instance_dir) {
            warn!(
                "failed to remove instance cache dir {:?}: {err}",
                instance_dir
            );
        } else {
            info!("removed instance cache dir {:?}", instance_dir);
        }
    }
}

/// Scan `base/mounts/` for instance directories whose owning process is no
/// longer alive. Removes any orphaned directories.
pub fn cleanup_orphaned_mounts(base: &Path) {
    let mounts = base.join(MOUNTS_DIR);
    let entries = match fs::read_dir(&mounts) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return,
        Err(err) => {
            warn!("failed to read mounts dir {:?}: {err}", mounts);
            return;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                warn!("failed to read mount dir entry: {err}");
                continue;
            }
        };
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let pid_file = path.join(PID_LOCK);
        let pid = match fs::read_to_string(&pid_file) {
            Ok(content) => content,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                warn!("no pid.lock in {:?}, removing", path);
                remove_instance_dir(&path);
                continue;
            }
            Err(err) => {
                warn!("failed to read {:?}: {err}, skipping", pid_file);
                continue;
            }
        };
        let pid: u32 = match pid.trim().parse() {
            Ok(pid) => pid,
            Err(err) => {
                warn!("corrupt pid.lock in {:?}: {err}, removing", path);
                remove_instance_dir(&path);
                continue;
            }
        };
        if !is_process_alive(pid) {
            info!("cleaning up orphaned mount cache (pid {pid}): {:?}", path);
            remove_instance_dir(&path);
        }
    }
}

fn is_process_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 only checks if process exists, no signal is sent.
    let ret = unsafe { libc::kill(pid as i32, 0) };
    if ret == 0 {
        return true;
    }
    // EPERM means the process exists but belongs to another user.
    // Only ESRCH means the process is truly gone.
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn generate_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    format!("{pid}-{ts}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_orphaned_mounts_removes_dead_pid_dirs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mounts = tmp.path().join(MOUNTS_DIR);
        let orphan = mounts.join("dead-instance");
        fs::create_dir_all(&orphan).expect("mkdir");
        fs::write(orphan.join(PID_LOCK), "999999999").expect("write pid");

        cleanup_orphaned_mounts(tmp.path());

        assert!(!orphan.exists());
    }

    #[test]
    fn cleanup_orphaned_mounts_preserves_live_pid_dirs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mounts = tmp.path().join(MOUNTS_DIR);
        let live = mounts.join("live-instance");
        fs::create_dir_all(&live).expect("mkdir");
        let pid = std::process::id();
        fs::write(live.join(PID_LOCK), pid.to_string()).expect("write pid");

        cleanup_orphaned_mounts(tmp.path());

        assert!(live.exists());
    }

    #[test]
    fn create_instance_dir_writes_pid_lock() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let instance = create_instance_dir(tmp.path()).expect("create");

        assert!(instance.exists());
        let pid_content = fs::read_to_string(instance.join(PID_LOCK)).expect("read pid");
        assert_eq!(pid_content, std::process::id().to_string());
    }

    #[test]
    fn remove_instance_dir_cleans_up() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let instance = create_instance_dir(tmp.path()).expect("create");
        fs::write(instance.join("some_cached_file"), b"data").expect("write");

        remove_instance_dir(&instance);

        assert!(!instance.exists());
    }
}
