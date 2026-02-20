mod cache;
mod download_gate;
mod download_pool;
mod filesystem;
mod fuse_fs;
mod http;
mod manifest;
mod syscall_trace;
mod tree;

use std::collections::HashSet;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use std::process::ExitCode;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::cache::{Cache, default_cache_dir};
use crate::download_gate::DownloadGate;
use crate::download_pool::DownloadPool;
use crate::filesystem::FetchFs;
use crate::fuse_fs::FuseFS;
use crate::http::HttpClient;
use crate::manifest::Manifest;
use crate::syscall_trace::SyscallTracer;

#[derive(Parser)]
#[command(
    name = "fetchfs",
    version,
    about = "Manifest-driven lazy URL FUSE filesystem"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Mount {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        mountpoint: PathBuf,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        verbose: bool,
        #[arg(long, default_value_t = 32)]
        max_concurrent: usize,
        #[arg(long, default_value_t = 3)]
        retries: u32,
        #[arg(long, default_value_t = 200)]
        retry_base_ms: u64,
        #[arg(long, default_value_t = 5000)]
        connect_timeout_ms: u64,
        #[arg(long, default_value_t = 30000)]
        request_timeout_ms: u64,
        #[arg(long)]
        cache_max_bytes: Option<u64>,
        #[arg(long)]
        cache_max_age_days: Option<u64>,
        #[arg(long)]
        block_cache_size: Option<u64>,
        #[arg(long, default_value_t = false)]
        streaming: bool,
        #[arg(long, default_value_t = false)]
        allow_other: bool,
        #[arg(long)]
        trace_socket: Option<PathBuf>,
        /// Comma-separated list of syscalls to trace (e.g. "read,open"). If omitted, all are traced.
        #[arg(long)]
        trace_filter: Option<String>,
    },
    Clean {
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Stats {
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Mount {
            manifest,
            mountpoint,
            cache_dir,
            verbose,
            max_concurrent,
            retries,
            retry_base_ms,
            connect_timeout_ms,
            request_timeout_ms,
            cache_max_bytes,
            cache_max_age_days,
            block_cache_size,
            streaming,
            allow_other,
            trace_socket,
            trace_filter,
        } => {
            init_logging(verbose);

            if !mountpoint.exists() {
                error!("mountpoint does not exist: {}", mountpoint.display());
                return ExitCode::FAILURE;
            }
            if let Ok(mut entries) = std::fs::read_dir(&mountpoint)
                && entries.next().is_some()
            {
                error!("mountpoint is not empty: {}", mountpoint.display());
                return ExitCode::FAILURE;
            }

            let manifest = match Manifest::load_from_path(&manifest) {
                Ok(manifest) => manifest,
                Err(err) => {
                    error!("failed to load manifest: {err}");
                    return ExitCode::FAILURE;
                }
            };
            let tree = match manifest.build_tree() {
                Ok(tree) => tree,
                Err(err) => {
                    error!("failed to build manifest tree: {err}");
                    return ExitCode::FAILURE;
                }
            };
            let file_count = manifest.entries.len();
            let cache_dir = cache_dir.unwrap_or_else(|| {
                default_cache_dir().unwrap_or_else(|_| PathBuf::from(".cache/fetchfs"))
            });
            let cache = Cache::new(cache_dir.clone());
            if let Err(err) = cache.evict(cache_max_bytes, cache_max_age_days) {
                error!("failed to evict cache entries: {err}");
            }
            let block_cache = block_cache_size
                .map(|size| crate::cache::BlockCache::new(cache_dir.join("blocks"), size));
            let downloader = match HttpClient::new(
                retries,
                retry_base_ms,
                connect_timeout_ms,
                request_timeout_ms,
            ) {
                Ok(client) => client,
                Err(err) => {
                    error!("failed to initialize http client: {err}");
                    return ExitCode::FAILURE;
                }
            };
            let fs = FetchFs::new(
                manifest,
                tree,
                cache,
                block_cache,
                Arc::new(downloader),
                Arc::new(DownloadGate::new(max_concurrent)),
                Arc::new(DownloadPool::new(max_concurrent)),
                streaming,
            );
            let tracer = if let Some(socket_path) = trace_socket {
                let filter_log = trace_filter.as_ref().map(|f| {
                    let mut v: Vec<_> = f.split(',').map(|s| s.trim()).collect();
                    v.sort();
                    v.join(",")
                });
                let filter = trace_filter.map(|f| {
                    f.split(',').map(|s| s.trim().to_string()).collect::<HashSet<_>>()
                });
                match SyscallTracer::new(&socket_path, filter) {
                    Ok(tracer) => {
                        if let Some(f) = filter_log {
                            info!("syscall tracing enabled: {} (filter: {})", socket_path.display(), f);
                        } else {
                            info!("syscall tracing enabled: {}", socket_path.display());
                        }
                        Some(tracer)
                    }
                    Err(err) => {
                        error!("failed to create syscall tracer at {}: {err}", socket_path.display());
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                None
            };
            let fuse_fs = FuseFS::new(fs, tracer);
            let mount_options = FuseFS::mount_options(allow_other);

            info!(
                "ready to mount {} files at {} (cache: {})",
                file_count,
                mountpoint.display(),
                cache_dir.display()
            );

            if let Err(err) = install_signal_handler(&mountpoint) {
                error!("failed to install signal handler: {err}");
            }

            if let Err(err) = fuser::mount2(fuse_fs, &mountpoint, &mount_options) {
                error!("failed to mount: {err}");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Commands::Clean { cache_dir } => {
            init_logging(false);
            let cache_dir = cache_dir.unwrap_or_else(|| {
                default_cache_dir().unwrap_or_else(|_| PathBuf::from(".cache/fetchfs"))
            });
            let cache = Cache::new(cache_dir);
            if let Err(err) = cache.clean() {
                error!("failed to clean cache: {err}");
                ExitCode::FAILURE
            } else {
                info!("cache cleared");
                ExitCode::SUCCESS
            }
        }
        Commands::Stats { cache_dir } => {
            init_logging(false);
            let cache_dir = cache_dir.unwrap_or_else(|| {
                default_cache_dir().unwrap_or_else(|_| PathBuf::from(".cache/fetchfs"))
            });
            let cache = Cache::new(cache_dir.clone());
            match cache.stats() {
                Ok(stats) => {
                    info!(
                        "cache stats: {} files, {} bytes (path: {})",
                        stats.files,
                        stats.bytes,
                        cache_dir.display()
                    );
                    ExitCode::SUCCESS
                }
                Err(err) => {
                    error!("failed to read cache stats: {err}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

fn init_logging(verbose: bool) {
    let filter = if verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn install_signal_handler(mountpoint: &Path) -> Result<(), std::io::Error> {
    use signal_hook::consts::signal::{SIGINT, SIGTERM};
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
    let mountpoint = mountpoint.to_path_buf();
    std::thread::spawn(move || {
        if let Some(_signal) = signals.forever().next()
            && let Err(err) = unmount_fs(&mountpoint)
            && err.kind() != std::io::ErrorKind::InvalidInput
        {
            warn!("unmount failed: {err}");
        }
    });
    Ok(())
}

#[cfg(target_os = "linux")]
fn unmount_fs(mountpoint: &Path) -> Result<(), std::io::Error> {
    let cstr = std::ffi::CString::new(mountpoint.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid mountpoint"))?;
    let result = unsafe { libc::umount(cstr.as_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn unmount_fs(mountpoint: &Path) -> Result<(), std::io::Error> {
    let cstr = std::ffi::CString::new(mountpoint.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid mountpoint"))?;
    let result = unsafe { libc::unmount(cstr.as_ptr(), 0) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
