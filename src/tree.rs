use std::collections::BTreeMap;

use crate::manifest::{ManifestEntry, ManifestError};

#[derive(Debug, Default)]
struct TreeNode {
    children: BTreeMap<String, TreeNode>,
    entry_index: Option<usize>,
}

#[derive(Debug, Default)]
pub struct PathTree {
    root: TreeNode,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    #[allow(dead_code)]
    pub entry_index: Option<usize>,
}

impl PathTree {
    pub fn build(entries: &[ManifestEntry]) -> Result<Self, ManifestError> {
        let mut tree = PathTree::default();
        for (idx, entry) in entries.iter().enumerate() {
            tree.insert(&entry.path, idx)?;
        }
        Ok(tree)
    }

    pub fn lookup(&self, path: &str) -> Option<usize> {
        let node = self.find_node(path)?;
        node.entry_index
    }

    pub fn list_dir(&self, path: &str) -> Option<Vec<DirEntry>> {
        let node = self.find_node(path)?;
        let mut entries = Vec::new();
        for (name, child) in &node.children {
            debug_assert!(
                child.entry_index.is_none() || child.children.is_empty(),
                "node with both file entry and children"
            );
            let is_dir = child.entry_index.is_none();
            entries.push(DirEntry {
                name: name.clone(),
                is_dir,
                entry_index: child.entry_index,
            });
        }
        Some(entries)
    }

    fn insert(&mut self, path: &str, entry_index: usize) -> Result<(), ManifestError> {
        let mut node = &mut self.root;
        for segment in path.split('/') {
            if node.entry_index.is_some() {
                return Err(ManifestError::InvalidPath(path.to_string()));
            }
            node = node.children.entry(segment.to_string()).or_default();
        }
        if node.entry_index.is_some() || !node.children.is_empty() {
            return Err(ManifestError::InvalidPath(path.to_string()));
        }
        node.entry_index = Some(entry_index);
        Ok(())
    }

    fn find_node(&self, path: &str) -> Option<&TreeNode> {
        if path.is_empty() || path == "." {
            return Some(&self.root);
        }
        let mut node = &self.root;
        for segment in path.split('/') {
            node = node.children.get(segment)?;
        }
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_lookup_and_list_dir() {
        let entries = vec![
            ManifestEntry {
                path: "data/file1.txt".to_string(),
                url: "https://example.com/file1.txt".to_string(),
                size: Some(1),
                mtime: None,
            },
            ManifestEntry {
                path: "data/sub/file2.txt".to_string(),
                url: "https://example.com/file2.txt".to_string(),
                size: Some(1),
                mtime: None,
            },
        ];
        let tree = PathTree::build(&entries).expect("tree build");

        assert_eq!(tree.lookup("data/file1.txt"), Some(0));
        assert_eq!(tree.lookup("data/sub/file2.txt"), Some(1));
        assert!(tree.lookup("missing").is_none());

        let root_entries = tree.list_dir("").expect("root entries");
        assert!(
            root_entries
                .iter()
                .any(|entry| entry.name == "data" && entry.is_dir)
        );
    }
}
