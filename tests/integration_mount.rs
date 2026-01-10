use std::fs;
use std::io::{Read, Seek, Write};
use std::net::TcpListener;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use serial_test::serial;

fn find_or_build_binary() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_fetchfs") {
        return std::path::PathBuf::from(path);
    }
    let manifest_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| manifest_dir.join("target"));
    let bin_path = target_dir.join("debug").join(if cfg!(windows) {
        "fetchfs.exe"
    } else {
        "fetchfs"
    });
    if bin_path.exists() {
        return bin_path;
    }
    let status = Command::new("cargo")
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_dir.join("Cargo.toml"))
        .status()
        .expect("cargo build");
    if !status.success() {
        panic!("cargo build failed");
    }
    bin_path
}

fn spawn_http_server<F>(handler: F) -> (std::net::SocketAddr, std::thread::JoinHandle<()>)
where
    F: Fn(&str, &mut std::net::TcpStream) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let handler = std::sync::Arc::new(handler);
    let server_thread = thread::spawn(move || {
        let _ = listener.set_nonblocking(true);
        let mut last_activity = Instant::now();
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    last_activity = Instant::now();
                    let mut buffer = [0u8; 2048];
                    let bytes = match stream.read(&mut buffer) {
                        Ok(0) => continue,
                        Ok(bytes) => bytes,
                        Err(_) => continue,
                    };
                    let request = String::from_utf8_lossy(&buffer[..bytes]);
                    handler(&request, &mut stream);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    if last_activity.elapsed() > Duration::from_secs(3) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });
    (addr, server_thread)
}

fn write_manifest(dir: &std::path::Path, content: &str) -> std::path::PathBuf {
    let manifest = dir.join("manifest.json");
    fs::write(&manifest, content).expect("manifest");
    manifest
}

fn spawn_mount(
    manifest: &std::path::Path,
    mountpoint: &std::path::Path,
    cache_dir: &std::path::Path,
    extra_args: &[&str],
) -> std::process::Child {
    let bin = find_or_build_binary();
    let mut args = vec![
        "mount",
        "--manifest",
        manifest.to_str().expect("manifest str"),
        "--mountpoint",
        mountpoint.to_str().expect("mountpoint str"),
        "--cache-dir",
        cache_dir.to_str().expect("cache dir str"),
    ];
    args.extend(extra_args);
    Command::new(bin).args(args).spawn().expect("spawn")
}

fn shutdown_mount(mut child: std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
#[ignore]
#[serial]
fn mount_and_read_presigned_url() {
    if std::env::var("FETCHFS_E2E").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mountpoint = tmp.path().join("mnt");
    let cache_dir = tmp.path().join("cache");
    fs::create_dir_all(&mountpoint).expect("mountpoint");

    let (addr, server_thread) = spawn_http_server(|request, stream| {
        if request.starts_with("HEAD") {
            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Length: 5\r\n",
                "Last-Modified: Tue, 20 Dec 2025 14:05:12 GMT\r\n",
                "\r\n"
            );
            let _ = stream.write_all(response.as_bytes());
            return;
        }
        if let Some(range_line) = request.lines().find(|line| line.starts_with("Range:")) {
            if range_line.contains("bytes=0-0") {
                let response = concat!(
                    "HTTP/1.1 206 Partial Content\r\n",
                    "Content-Length: 1\r\n",
                    "Content-Range: bytes 0-0/5\r\n",
                    "\r\n",
                    "h"
                );
                let _ = stream.write_all(response.as_bytes());
                return;
            }
        }
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Length: 5\r\n",
            "\r\n",
            "hello"
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let manifest_content = format!(
        r#"{{
  "version": 1,
  "entries": [
    {{
      "path": "/data/file.txt",
      "url": "http://{}"
    }}
  ]
}}"#,
        addr
    );
    let manifest = write_manifest(tmp.path(), &manifest_content);

    let child = spawn_mount(&manifest, &mountpoint, &cache_dir, &[]);

    thread::sleep(Duration::from_secs(1));
    let data_path = mountpoint.join("data").join("file.txt");
    let contents = fs::read_to_string(&data_path).expect("read file");
    assert_eq!(contents, "hello");

    shutdown_mount(child);
    let _ = server_thread.join();
}

#[test]
#[ignore]
#[serial]
fn mount_and_read_with_block_cache_overlap() {
    if std::env::var("FETCHFS_E2E").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mountpoint = tmp.path().join("mnt");
    let cache_dir = tmp.path().join("cache");
    fs::create_dir_all(&mountpoint).expect("mountpoint");

    let ranges = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let ranges_for_server = std::sync::Arc::clone(&ranges);
    let (addr, server_thread) = spawn_http_server(move |request, stream| {
        if request.starts_with("HEAD") {
            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Length: 6\r\n",
                "Last-Modified: Tue, 20 Dec 2025 14:05:12 GMT\r\n",
                "\r\n"
            );
            let _ = stream.write_all(response.as_bytes());
            return;
        }
        if let Some(range_line) = request.lines().find(|line| line.starts_with("Range:")) {
            if let Some(range_value) = range_line.splitn(2, ':').nth(1) {
                let mut ranges = ranges_for_server.lock().expect("ranges lock");
                ranges.push(range_value.trim().to_string());
            }
            let body = b"abcdef";
            if let Some(range_value) = range_line.splitn(2, '=').nth(1) {
                let parts: Vec<&str> = range_value.trim().split('-').collect();
                let start: usize = parts.get(0).and_then(|v| v.parse().ok()).unwrap_or(0);
                let end: usize = parts.get(1).and_then(|v| v.parse().ok()).unwrap_or(start);
                let end = end.min(body.len().saturating_sub(1));
                let chunk = &body[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\n\r\n",
                    chunk.len(),
                    start,
                    end,
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(chunk);
                return;
            }
        }
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Length: 6\r\n",
            "\r\n",
            "abcdef"
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let manifest_content = format!(
        r#"{{
  "version": 1,
  "entries": [
    {{
      "path": "/data/file.txt",
      "url": "http://{}"
    }}
  ]
}}"#,
        addr
    );
    let manifest = write_manifest(tmp.path(), &manifest_content);

    let child = spawn_mount(
        &manifest,
        &mountpoint,
        &cache_dir,
        &["--block-cache-size", "2"],
    );

    thread::sleep(Duration::from_secs(1));
    let data_path = mountpoint.join("data").join("file.txt");
    let mut file = fs::File::open(&data_path).expect("open file");
    let mut buf = [0u8; 3];
    file.read_exact(&mut buf).expect("read exact");
    assert_eq!(&buf, b"abc");
    let ranges_after_first = {
        let ranges = ranges.lock().expect("ranges lock");
        ranges.len()
    };

    file.seek(std::io::SeekFrom::Start(1)).expect("seek");
    let mut buf = [0u8; 2];
    file.read_exact(&mut buf).expect("read overlap");
    assert_eq!(&buf, b"bc");
    let ranges_after_second = {
        let ranges = ranges.lock().expect("ranges lock");
        ranges.len()
    };
    assert_eq!(ranges_after_first, ranges_after_second);

    shutdown_mount(child);
    let _ = server_thread.join();
}

#[test]
#[ignore]
#[serial]
fn mount_block_cache_no_range_single_get() {
    if std::env::var("FETCHFS_E2E").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mountpoint = tmp.path().join("mnt");
    let cache_dir = tmp.path().join("cache");
    fs::create_dir_all(&mountpoint).expect("mountpoint");

    let gets = std::sync::Arc::new(std::sync::Mutex::new(0u32));
    let gets_for_server = std::sync::Arc::clone(&gets);
    let (addr, server_thread) = spawn_http_server(move |request, stream| {
        if request.starts_with("HEAD") {
            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Length: 6\r\n",
                "Last-Modified: Tue, 20 Dec 2025 14:05:12 GMT\r\n",
                "\r\n"
            );
            let _ = stream.write_all(response.as_bytes());
            return;
        }
        if request.starts_with("GET") {
            let mut gets = gets_for_server.lock().expect("gets lock");
            *gets += 1;
        }
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Length: 6\r\n",
            "\r\n",
            "abcdef"
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let manifest_content = format!(
        r#"{{
  "version": 1,
  "entries": [
    {{
      "path": "/data/file.txt",
      "url": "http://{}",
      "size": 6,
      "mtime": "2025-12-20T14:05:12Z"
    }}
  ]
}}"#,
        addr
    );
    let manifest = write_manifest(tmp.path(), &manifest_content);

    let child = spawn_mount(
        &manifest,
        &mountpoint,
        &cache_dir,
        &["--block-cache-size", "2"],
    );

    thread::sleep(Duration::from_secs(1));
    let data_path = mountpoint.join("data").join("file.txt");
    let contents = fs::read_to_string(&data_path).expect("read file");
    assert_eq!(contents, "abcdef");

    let contents = fs::read_to_string(&data_path).expect("read file again");
    assert_eq!(contents, "abcdef");

    let get_count = *gets.lock().expect("gets lock");
    assert_eq!(get_count, 1);

    shutdown_mount(child);
    let _ = server_thread.join();
}

#[test]
#[ignore]
#[serial]
fn mount_request_timeout_errors() {
    if std::env::var("FETCHFS_E2E").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mountpoint = tmp.path().join("mnt");
    let cache_dir = tmp.path().join("cache");
    fs::create_dir_all(&mountpoint).expect("mountpoint");

    let (addr, server_thread) = spawn_http_server(|_request, _stream| {
        thread::sleep(Duration::from_secs(2));
    });

    let manifest_content = format!(
        r#"{{
  "version": 1,
  "entries": [
    {{
      "path": "/data/slow.txt",
      "url": "http://{}",
      "size": 4,
      "mtime": "2025-12-20T14:05:12Z"
    }}
  ]
}}"#,
        addr
    );
    let manifest = write_manifest(tmp.path(), &manifest_content);

    let child = spawn_mount(
        &manifest,
        &mountpoint,
        &cache_dir,
        &["--request-timeout-ms", "200"],
    );

    thread::sleep(Duration::from_secs(1));
    let data_path = mountpoint.join("data").join("slow.txt");
    let read_result = fs::read_to_string(&data_path);
    assert!(read_result.is_err());

    shutdown_mount(child);
    let _ = server_thread.join();
}

#[test]
#[ignore]
#[serial]
fn mount_open_write_rejected() {
    if std::env::var("FETCHFS_E2E").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mountpoint = tmp.path().join("mnt");
    let cache_dir = tmp.path().join("cache");
    fs::create_dir_all(&mountpoint).expect("mountpoint");

    let manifest_content = format!(
        r#"{{
  "version": 1,
  "entries": [
    {{
      "path": "/data/file.txt",
      "url": "http://127.0.0.1:1"
    }}
  ]
}}"#,
    );
    let manifest = write_manifest(tmp.path(), &manifest_content);

    let child = spawn_mount(&manifest, &mountpoint, &cache_dir, &[]);

    thread::sleep(Duration::from_secs(1));
    let data_path = mountpoint.join("data").join("file.txt");
    let open_result = fs::OpenOptions::new().write(true).open(&data_path);
    assert!(open_result.is_err());

    shutdown_mount(child);
}

#[test]
#[ignore]
#[serial]
fn mount_range_eof_returns_empty() {
    if std::env::var("FETCHFS_E2E").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let mountpoint = tmp.path().join("mnt");
    let cache_dir = tmp.path().join("cache");
    fs::create_dir_all(&mountpoint).expect("mountpoint");

    let (addr, server_thread) = spawn_http_server(|request, stream| {
        if request.starts_with("HEAD") {
            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Length: 3\r\n",
                "\r\n"
            );
            let _ = stream.write_all(response.as_bytes());
            return;
        }
        if let Some(range_line) = request.lines().find(|line| line.starts_with("Range:")) {
            if let Some(range_value) = range_line.splitn(2, '=').nth(1) {
                let parts: Vec<&str> = range_value.trim().split('-').collect();
                let start: usize = parts.get(0).and_then(|v| v.parse().ok()).unwrap_or(0);
                if start >= 3 {
                    let response = "HTTP/1.1 416 Range Not Satisfiable\r\n\r\n";
                    let _ = stream.write_all(response.as_bytes());
                    return;
                }
            }
        }
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Length: 3\r\n",
            "\r\n",
            "hey"
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let manifest_content = format!(
        r#"{{
  "version": 1,
  "entries": [
    {{
      "path": "/data/file.txt",
      "url": "http://{}"
    }}
  ]
}}"#,
        addr
    );
    let manifest = write_manifest(tmp.path(), &manifest_content);

    let child = spawn_mount(&manifest, &mountpoint, &cache_dir, &[]);

    thread::sleep(Duration::from_secs(1));
    let data_path = mountpoint.join("data").join("file.txt");
    let mut file = fs::File::open(&data_path).expect("open file");
    file.seek(std::io::SeekFrom::Start(10)).expect("seek");
    let mut buf = [0u8; 8];
    let read = file.read(&mut buf).expect("read");
    assert_eq!(read, 0);

    shutdown_mount(child);
    let _ = server_thread.join();
}
