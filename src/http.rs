use std::io;
use std::path::Path;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use httpdate::parse_http_date;
use reqwest::blocking::Client;
use tracing::debug;

#[derive(Debug, Clone, Default)]
pub struct ObjectMeta {
    pub size: Option<u64>,
    pub mtime: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy)]
pub enum RangeStatus {
    Partial,
    Full,
    Empty,
}

#[derive(Debug, Clone)]
pub struct RangeResult {
    pub status: RangeStatus,
    pub data: Vec<u8>,
    pub meta: ObjectMeta,
}

pub trait UrlDownloader: Send + Sync {
    fn download_full(&self, url: &str, dest: &Path) -> io::Result<()>;
}

#[derive(Debug)]
pub struct HttpClient {
    client: Client,
    retries: u32,
    base_delay_ms: u64,
}

impl HttpClient {
    pub fn new(
        retries: u32,
        base_delay_ms: u64,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> io::Result<Self> {
        let client = Client::builder()
            // Cap redirects to avoid loops while supporting common URL patterns.
            .redirect(reqwest::redirect::Policy::limited(10))
            .connect_timeout(std::time::Duration::from_millis(connect_timeout_ms))
            .timeout(std::time::Duration::from_millis(request_timeout_ms))
            .build()
            .map_err(io::Error::other)?;
        Ok(Self {
            client,
            retries,
            base_delay_ms,
        })
    }

    pub fn head_with_fallback(&self, url: &str) -> io::Result<ObjectMeta> {
        match self.head(url) {
            Ok(meta) => Ok(meta),
            Err(_) => self.head_range_fallback(url),
        }
    }

    pub fn head(&self, url: &str) -> io::Result<ObjectMeta> {
        let response = self
            .client
            .head(url)
            .send()
            .map_err(|err| io::Error::other(format!("head request failed for {url}: {err}")))?;
        if !response.status().is_success() {
            return Err(io::Error::other(format!(
                "head failed for {url} with status {}",
                response.status(),
            )));
        }
        Ok(meta_from_headers(response.headers()))
    }

    pub fn get_range(&self, url: &str, start: u64, end: u64) -> io::Result<RangeResult> {
        self.get_range_with_retry(url, start, end)
    }

    fn get_range_with_retry(&self, url: &str, start: u64, end: u64) -> io::Result<RangeResult> {
        let mut last_err = None;
        for attempt in 0..=self.retries {
            debug!(
                "range attempt {} for {} ({}-{})",
                attempt + 1,
                url,
                start,
                end
            );
            match self.get_range_once(url, start, end) {
                Ok(result) => return Ok(result),
                Err(err) => {
                    last_err = Some(err);
                    if attempt < self.retries {
                        let delay = self.base_delay_ms.saturating_mul(1u64 << attempt);
                        std::thread::sleep(std::time::Duration::from_millis(delay));
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::other(format!("range request failed for {url}"))
        }))
    }

    fn get_range_once(&self, url: &str, start: u64, end: u64) -> io::Result<RangeResult> {
        let range = format!("bytes={start}-{end}");
        let response = self
            .client
            .get(url)
            .header(reqwest::header::RANGE, range)
            .send()
            .map_err(|err| io::Error::other(format!("range request failed for {url}: {err}")))?;
        let status = response.status();
        let headers = response.headers().clone();
        let data = response
            .bytes()
            .map_err(io::Error::other)?
            .to_vec();

        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            let mut meta = meta_from_headers(&headers);
            if let Some(total) = headers
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|val| val.to_str().ok())
                .and_then(parse_content_range_total)
            {
                meta.size = Some(total);
            }
            return Ok(RangeResult {
                status: RangeStatus::Partial,
                data,
                meta,
            });
        }

        if status.is_success() {
            return Ok(RangeResult {
                status: RangeStatus::Full,
                data,
                meta: meta_from_headers(&headers),
            });
        }

        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(RangeResult {
                status: RangeStatus::Empty,
                data: Vec::new(),
                meta: ObjectMeta::default(),
            });
        }

        Err(io::Error::other(format!(
            "range request failed for {url} with status {}",
            status,
        )))
    }

    pub fn download_full_with_retry(&self, url: &str, dest: &Path) -> io::Result<()> {
        let mut last_err = None;
        for attempt in 0..=self.retries {
            debug!("download attempt {} for {}", attempt + 1, url);
            match self.download_full_once(url, dest) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_err = Some(err);
                    if attempt < self.retries {
                        let delay = self.base_delay_ms.saturating_mul(1u64 << attempt);
                        std::thread::sleep(std::time::Duration::from_millis(delay));
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::other(format!("download failed for {url}"))
        }))
    }

    fn head_range_fallback(&self, url: &str) -> io::Result<ObjectMeta> {
        let response = self
            .client
            .get(url)
            .header(reqwest::header::RANGE, "bytes=0-0")
            .send()
            .map_err(|err| io::Error::other(format!("range probe failed for {url}: {err}")))?;
        if !response.status().is_success() && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
        {
            return Ok(ObjectMeta::default());
        }
        let mut meta = meta_from_headers(response.headers());
        if let Some(total) = response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|val| val.to_str().ok())
            .and_then(parse_content_range_total)
        {
            meta.size = Some(total);
        }
        Ok(meta)
    }

    fn download_full_once(&self, url: &str, dest: &Path) -> io::Result<()> {
        let mut response = self
            .client
            .get(url)
            .send()
            .map_err(|err| io::Error::other(format!("download request failed for {url}: {err}")))?;
        if !response.status().is_success() {
            return Err(io::Error::other(format!(
                "download failed for {url} with status {}",
                response.status(),
            )));
        }
        let mut file = std::fs::File::create(dest)?;
        io::copy(&mut response, &mut file)?;
        Ok(())
    }
}

impl UrlDownloader for HttpClient {
    fn download_full(&self, url: &str, dest: &Path) -> io::Result<()> {
        self.download_full_with_retry(url, dest)
    }
}

fn meta_from_headers(headers: &reqwest::header::HeaderMap) -> ObjectMeta {
    let size = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|val| val.to_str().ok())
        .and_then(|val| val.parse::<u64>().ok());
    let mtime = headers
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|val| val.to_str().ok())
        .and_then(parse_http_date_safe);
    ObjectMeta { size, mtime }
}

fn parse_content_range_total(value: &str) -> Option<u64> {
    let total = value.rsplit('/').next()?;
    total.parse::<u64>().ok()
}

fn parse_http_date_safe(raw: &str) -> Option<DateTime<Utc>> {
    let parsed = parse_http_date(raw).ok()?;
    let system: SystemTime = parsed;
    Some(DateTime::<Utc>::from(system))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn get_range_retries_on_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_server = Arc::clone(&calls);
        let server_thread = thread::spawn(move || {
            for _ in 0..3 {
                let (mut stream, _) = match listener.accept() {
                    Ok(conn) => conn,
                    Err(_) => return,
                };
                let mut buffer = [0u8; 2048];
                let _ = stream.read(&mut buffer);
                let call = calls_server.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    let response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
                    let _ = stream.write_all(response.as_bytes());
                    continue;
                }
                let response = concat!(
                    "HTTP/1.1 206 Partial Content\r\n",
                    "Content-Length: 1\r\n",
                    "Content-Range: bytes 0-0/5\r\n",
                    "\r\n",
                    "h"
                );
                let _ = stream.write_all(response.as_bytes());
                break;
            }
        });

        let client = HttpClient::new(1, 10, 1000, 2000).expect("client");
        let url = format!("http://{}", addr);
        let result = client.get_range(&url, 0, 0).expect("range");
        assert!(matches!(result.status, RangeStatus::Partial));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let _ = server_thread.join();
        thread::sleep(Duration::from_millis(10));
    }

    #[test]
    fn get_range_prefers_content_range_total() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server_thread = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 2048];
                let _ = stream.read(&mut buffer);
                let response = concat!(
                    "HTTP/1.1 206 Partial Content\r\n",
                    "Content-Length: 1\r\n",
                    "Content-Range: bytes 0-0/5\r\n",
                    "\r\n",
                    "h"
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let client = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let url = format!("http://{}", addr);
        let result = client.get_range(&url, 0, 0).expect("range");
        assert_eq!(result.meta.size, Some(5));

        let _ = server_thread.join();
    }

    #[test]
    fn head_errors_on_non_success() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server_thread = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 1024];
                let _ = stream.read(&mut buffer);
                let response = "HTTP/1.1 404 Not Found\r\n\r\n";
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let client = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let url = format!("http://{}", addr);
        assert!(client.head(&url).is_err());

        let _ = server_thread.join();
    }

    #[test]
    fn get_range_errors_on_non_success() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server_thread = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 1024];
                let _ = stream.read(&mut buffer);
                let response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let client = HttpClient::new(0, 0, 1000, 2000).expect("client");
        let url = format!("http://{}", addr);
        assert!(client.get_range(&url, 0, 0).is_err());

        let _ = server_thread.join();
    }
}
