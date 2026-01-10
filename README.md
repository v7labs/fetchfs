# fetchfs

Manifest-driven, read-only FUSE filesystem that lazily fetches HTTP/HTTPS URLs on demand.

Written by Simon Edwardsson (simedw.com).

## Status

Core path is implemented: manifest parsing, cache, metadata probing (HEAD/Range), and FUSE operations.

## Requirements

- Rust (stable)
- Linux with FUSE, or macOS with macFUSE installed
- Network access to the URLs in the manifest

## Build

```bash
cargo build
```

## Manifest

Each entry maps a virtual path to a URL. Paths are normalized, so `/foo/bar` and `foo/bar` are treated the same.

Example:

```json
{
  "version": 1,
  "entries": [
    {
      "path": "/data/reports/2024/q4.pdf",
      "url": "https://example.com/signed/q4.pdf?sig=...",
      "size": 4938291,
      "mtime": "2025-12-20T14:05:12Z"
    },
    {
      "path": "data/images/logo.png",
      "url": "https://cdn.example.com/logo.png"
    }
  ]
}
```

## Run

Create a mountpoint directory and run:

```bash
cargo run -- mount \
  --manifest /path/to/manifest.json \
  --mountpoint /path/to/mount \
  --cache-dir ~/.cache/fetchfs
```

Full example with optional flags:

```bash
cargo run -- mount \
  --manifest /path/to/manifest.json \
  --mountpoint /path/to/mount \
  --cache-dir ~/.cache/fetchfs \
  --allow-other \
  --max-concurrent 32 \
  --retries 3 \
  --retry-base-ms 200 \
  --connect-timeout-ms 5000 \
  --request-timeout-ms 30000 \
  --block-cache-size 262144 \
  --cache-max-bytes 1073741824 \
  --cache-max-age-days 7
```

Notes:

- The mountpoint must exist and be empty.
- Files are fetched on demand and cached on disk.
- The filesystem is read-only.
- Use `--streaming` to start downloads in the background and read as data arrives.
- Use `--block-cache-size` to enable range-friendly block caching (bytes per block, minimum 4096).
- Use `--connect-timeout-ms`/`--request-timeout-ms` to bound network operations (request timeout covers headers + body).
- Open/write attempts fail immediately with read-only errors.
- `stat` may trigger metadata probing if `size`/`mtime` are missing.
- `--allow-other` permits other local users to read the mount (disabled by default).
- The filesystem runs in the foreground only; use a service manager if you want daemonization.

## Cache Maintenance

```bash
cargo run -- clean --cache-dir ~/.cache/fetchfs
cargo run -- stats --cache-dir ~/.cache/fetchfs
```
