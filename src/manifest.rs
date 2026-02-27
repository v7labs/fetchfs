use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::tree::PathTree;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    #[allow(dead_code)]
    pub version: u32,
    #[serde(default)]
    pub revision: String,
    #[serde(deserialize_with = "deserialize_entries")]
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Clone)]
pub enum FileSource {
    Remote { url: String },
    Inline { data: Arc<[u8]> },
}

#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub path: String,
    pub source: FileSource,
    pub size: Option<u64>,
    pub mtime: Option<DateTime<Utc>>,
}

impl ManifestEntry {
    pub fn url(&self) -> Option<&str> {
        match &self.source {
            FileSource::Remote { url } => Some(url),
            FileSource::Inline { .. } => None,
        }
    }

    pub fn inline_data(&self) -> Option<&[u8]> {
        match &self.source {
            FileSource::Inline { data } => Some(data),
            FileSource::Remote { .. } => None,
        }
    }

    pub fn known_size(&self) -> Option<u64> {
        match &self.source {
            FileSource::Inline { data } => Some(data.len() as u64),
            FileSource::Remote { .. } => self.size,
        }
    }

    pub fn require_url(&self) -> std::io::Result<&str> {
        self.url().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("entry '{}' has no remote URL (inline source)", self.path),
            )
        })
    }
}

#[cfg(test)]
impl ManifestEntry {
    pub fn test_remote(path: &str, url: &str, size: Option<u64>) -> Self {
        Self {
            path: path.to_string(),
            source: FileSource::Remote {
                url: url.to_string(),
            },
            size,
            mtime: None,
        }
    }

    pub fn test_remote_with_mtime(
        path: &str,
        url: &str,
        size: Option<u64>,
        mtime: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            path: path.to_string(),
            source: FileSource::Remote {
                url: url.to_string(),
            },
            size,
            mtime,
        }
    }

    pub fn test_inline(path: &str, data: &[u8]) -> Self {
        Self {
            path: path.to_string(),
            source: FileSource::Inline {
                data: Arc::from(data),
            },
            size: None,
            mtime: None,
        }
    }
}

#[derive(Deserialize)]
struct RawManifestEntry {
    path: String,
    url: Option<String>,
    data: Option<String>,
    #[serde(default)]
    encoding: Option<ContentEncoding>,
    size: Option<u64>,
    mtime: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ContentEncoding {
    Utf8,
    Base64,
}

fn deserialize_entries<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ManifestEntry>, D::Error> {
    let raw_entries: Vec<RawManifestEntry> = Vec::deserialize(deserializer)?;
    raw_entries
        .into_iter()
        .map(|raw| raw.try_into().map_err(serde::de::Error::custom))
        .collect()
}

impl TryFrom<RawManifestEntry> for ManifestEntry {
    type Error = ManifestError;

    fn try_from(raw: RawManifestEntry) -> Result<Self, ManifestError> {
        let source = match (raw.url, raw.data) {
            (Some(url), None) => FileSource::Remote { url },
            (None, Some(text)) => {
                let bytes = match raw.encoding {
                    Some(ContentEncoding::Base64) => BASE64
                        .decode(&text)
                        .map_err(|e| ManifestError::InvalidData(e.to_string()))?,
                    Some(ContentEncoding::Utf8) | None => text.into_bytes(),
                };
                FileSource::Inline {
                    data: Arc::from(bytes),
                }
            }
            (Some(_), Some(_)) => {
                return Err(ManifestError::InvalidSource(
                    "entry must have either `url` or `data`, not both".into(),
                ));
            }
            (None, None) => {
                return Err(ManifestError::InvalidSource(
                    "entry must have either `url` or `data`".into(),
                ));
            }
        };

        Ok(ManifestEntry {
            path: raw.path,
            source,
            size: raw.size,
            mtime: raw.mtime,
        })
    }
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest: {0}")]
    Json(#[from] serde_json::Error),
    #[error("duplicate manifest path: {0}")]
    DuplicatePath(String),
    #[error("invalid manifest path: {0}")]
    InvalidPath(String),
    #[error("invalid manifest url: {0}")]
    InvalidUrl(String),
    #[error("invalid inline data: {0}")]
    InvalidData(String),
    #[error("invalid entry source: {0}")]
    InvalidSource(String),
}

impl Manifest {
    pub fn load_from_path(path: &Path) -> Result<Self, ManifestError> {
        let raw = fs::read_to_string(path)?;
        let mut manifest: Manifest = serde_json::from_str(&raw)?;
        manifest.normalize()?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        let mut seen = HashSet::new();
        for entry in &self.entries {
            if !seen.insert(entry.path.as_str()) {
                return Err(ManifestError::DuplicatePath(entry.path.clone()));
            }
            if let FileSource::Remote { url } = &entry.source {
                validate_url(url)?;
            }
        }
        Ok(())
    }

    pub fn build_tree(&self) -> Result<PathTree, ManifestError> {
        PathTree::build(&self.entries)
    }

    fn normalize(&mut self) -> Result<(), ManifestError> {
        for entry in &mut self.entries {
            entry.path = normalize_path(&entry.path)?;
        }
        Ok(())
    }
}

pub fn normalize_path(path: &str) -> Result<String, ManifestError> {
    if path.contains('\\') {
        return Err(ManifestError::InvalidPath(path.to_string()));
    }
    if path.ends_with('/') {
        return Err(ManifestError::InvalidPath(path.to_string()));
    }
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(ManifestError::InvalidPath(path.to_string()));
    }
    let mut segments = Vec::new();
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(ManifestError::InvalidPath(path.to_string()));
        }
        segments.push(segment);
    }
    Ok(segments.join("/"))
}

fn validate_url(raw: &str) -> Result<(), ManifestError> {
    let url = Url::parse(raw).map_err(|_| ManifestError::InvalidUrl(raw.to_string()))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(ManifestError::InvalidUrl(raw.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_accepts_absolute_and_relative() {
        assert_eq!(normalize_path("/data/file.txt").unwrap(), "data/file.txt");
        assert_eq!(normalize_path("data/file.txt").unwrap(), "data/file.txt");
    }

    #[test]
    fn deserialize_version_and_revision() {
        let json = r#"{"version":1,"revision":"abc123","entries":[]}"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.revision, "abc123");
    }

    #[test]
    fn deserialize_revision_defaults_to_empty() {
        let json = r#"{"version":1,"entries":[]}"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.revision, "");
    }

    #[test]
    fn normalize_rejects_invalid_paths() {
        let bad_paths = [
            "",
            "/",
            "trailing/",
            "double//slash",
            "dot/./file",
            "dotdot/../file",
            "back\\slash",
        ];
        for path in bad_paths {
            assert!(
                normalize_path(path).is_err(),
                "path should be invalid: {path}"
            );
        }
    }

    #[test]
    fn deserialize_url_entry() {
        let json =
            r#"{"version":1,"entries":[{"path":"f.txt","url":"https://example.com/f","size":10}]}"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].url(), Some("https://example.com/f"));
        assert!(manifest.entries[0].inline_data().is_none());
        assert_eq!(manifest.entries[0].known_size(), Some(10));
    }

    #[test]
    fn deserialize_inline_utf8_entry() {
        let json = r#"{"version":1,"entries":[{"path":"f.txt","data":"hello world"}]}"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.entries.len(), 1);
        assert!(manifest.entries[0].url().is_none());
        assert_eq!(
            manifest.entries[0].inline_data(),
            Some(b"hello world".as_slice())
        );
        assert_eq!(manifest.entries[0].known_size(), Some(11));
    }

    #[test]
    fn deserialize_inline_base64_entry() {
        let json =
            r#"{"version":1,"entries":[{"path":"f.bin","data":"SGVsbG8=","encoding":"base64"}]}"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.entries[0].inline_data(), Some(b"Hello".as_slice()));
        assert_eq!(manifest.entries[0].known_size(), Some(5));
    }

    #[test]
    fn deserialize_inline_explicit_utf8() {
        let json =
            r#"{"version":1,"entries":[{"path":"f.txt","data":"raw text","encoding":"utf8"}]}"#;
        let manifest: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(
            manifest.entries[0].inline_data(),
            Some(b"raw text".as_slice())
        );
    }

    #[test]
    fn deserialize_rejects_both_url_and_data() {
        let json =
            r#"{"version":1,"entries":[{"path":"f.txt","url":"https://x.com/f","data":"hi"}]}"#;
        assert!(serde_json::from_str::<Manifest>(json).is_err());
    }

    #[test]
    fn deserialize_rejects_neither_url_nor_data() {
        let json = r#"{"version":1,"entries":[{"path":"f.txt"}]}"#;
        assert!(serde_json::from_str::<Manifest>(json).is_err());
    }

    #[test]
    fn deserialize_rejects_invalid_base64() {
        let json = r#"{"version":1,"entries":[{"path":"f.bin","data":"not valid base64!!!","encoding":"base64"}]}"#;
        assert!(serde_json::from_str::<Manifest>(json).is_err());
    }

    #[test]
    fn validate_skips_url_check_for_inline() {
        let manifest = Manifest {
            version: 1,
            revision: String::new(),
            entries: vec![ManifestEntry::test_inline("f.txt", b"hello")],
        };
        assert!(manifest.validate().is_ok());
    }
}
