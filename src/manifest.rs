use std::collections::HashSet;
use std::fs;
use std::path::Path;

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
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ManifestEntry {
    pub path: String,
    pub url: String,
    pub size: Option<u64>,
    pub mtime: Option<DateTime<Utc>>,
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
            validate_url(&entry.url)?;
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
}
