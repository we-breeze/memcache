//! [`GroupDocument`] — a zero-copy index of namespace byte ranges within one
//! Vintage statics-config group's YAML.
//!
//! The group's `all` entry value is a YAML document whose top-level keys are
//! cache namespaces:
//!
//! ```yaml
//! namespace-a:
//!   master:
//!   - 192.0.2.30:15138
//!
//! namespace-b:
//!   master:
//!   - 192.0.2.27:15138
//! ```
//!
//! [`GroupDocument::parse`] scans the bytes once, recording each top-level
//! `name:` key's byte range. [`GroupDocument::namespace`] then returns a
//! zero-copy [`vintage::ConfigContent`] slice for one namespace — no YAML body
//! is copied.

use std::collections::HashMap;

use vintage::ConfigContent;

use crate::cacheservice::CacheServiceError;

/// Upper bound on the parsed group body size.
const MAX_GROUP_BYTES: usize = 16 * 1024 * 1024;
/// Upper bound on the number of namespaces in one group.
const MAX_NAMESPACES: usize = 4096;
/// Upper bound on a single namespace name length.
const MAX_NAMESPACE_NAME_LEN: usize = 256;

/// Errors raised while splitting a group body into namespace ranges.
#[derive(Debug, thiserror::Error)]
pub enum GroupDocumentError {
    /// The group body exceeded the size limit.
    #[error("group body exceeds {limit} bytes")]
    TooLarge { limit: usize },
    /// The group body was not valid UTF-8.
    #[error("group body is not valid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    /// A namespace name was too long.
    #[error("namespace name exceeds {limit} bytes: {name:?}")]
    NameTooLong { name: String, limit: usize },
    /// A duplicate namespace name was found.
    #[error("duplicate namespace name: {0}")]
    DuplicateNamespace(String),
    /// The namespace count exceeded the limit.
    #[error("namespace count exceeds {limit}")]
    TooManyNamespaces { limit: usize },
    /// A top-level line was not a `name:` mapping key.
    #[error("invalid top-level line at byte {offset}: {line:?}")]
    InvalidTopLevel { offset: usize, line: String },
}

impl From<GroupDocumentError> for CacheServiceError {
    fn from(error: GroupDocumentError) -> Self {
        CacheServiceError::Yaml(error.to_string())
    }
}

/// A zero-copy index of namespace byte ranges within a group body.
pub struct GroupDocument {
    content: ConfigContent,
    index: HashMap<Box<str>, std::ops::Range<usize>>,
}

impl GroupDocument {
    /// Scans `content` and builds the namespace index. The content is held by
    /// reference; [`Self::namespace`] slices it without copying.
    pub fn parse(content: ConfigContent) -> Result<Self, GroupDocumentError> {
        if content.len() > MAX_GROUP_BYTES {
            return Err(GroupDocumentError::TooLarge {
                limit: MAX_GROUP_BYTES,
            });
        }
        // Validate UTF-8 up front so byte offsets align with char positions and
        // downstream YAML parsing can assume a &str.
        let bytes = content.as_bytes();
        let _ = std::str::from_utf8(bytes)?;

        let mut index: HashMap<Box<str>, std::ops::Range<usize>> = HashMap::new();
        let mut current_name: Option<(String, usize)> = None;

        let mut pos = 0;
        while pos < bytes.len() {
            let line_end = bytes[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|i| pos + i)
                .unwrap_or(bytes.len());
            let line = &bytes[pos..line_end];

            // A top-level key starts at column 0 and is not blank/comment.
            let is_top_level = !line.is_empty()
                && line[0] != b' '
                && line[0] != b'\t'
                && line[0] != b'#'
                && line[0] != b'-'
                && line[0] != b'\r';

            if is_top_level {
                // Parse `name:` (a mapping key). The key runs up to the first
                // `:` that is followed by end-of-line or whitespace.
                let key = parse_top_level_key(line).ok_or_else(|| {
                    GroupDocumentError::InvalidTopLevel {
                        offset: pos,
                        line: String::from_utf8_lossy(line).into_owned(),
                    }
                })?;
                if key.len() > MAX_NAMESPACE_NAME_LEN {
                    return Err(GroupDocumentError::NameTooLong {
                        name: key.clone(),
                        limit: MAX_NAMESPACE_NAME_LEN,
                    });
                }
                // Close the previous namespace's range at this line's start.
                if let Some((prev_name, prev_start)) = current_name.take() {
                    let prev_key: Box<str> = prev_name.into_boxed_str();
                    if index.contains_key(&prev_key) {
                        return Err(GroupDocumentError::DuplicateNamespace(
                            prev_key.into_string(),
                        ));
                    }
                    index.insert(prev_key, prev_start..pos);
                }
                if index.len() >= MAX_NAMESPACES {
                    return Err(GroupDocumentError::TooManyNamespaces {
                        limit: MAX_NAMESPACES,
                    });
                }
                current_name = Some((key, pos));
            }

            pos = if line_end < bytes.len() {
                line_end + 1
            } else {
                bytes.len()
            };
        }

        // Close the final namespace at EOF.
        if let Some((prev_name, prev_start)) = current_name.take() {
            let prev_key: Box<str> = prev_name.into_boxed_str();
            if index.contains_key(&prev_key) {
                return Err(GroupDocumentError::DuplicateNamespace(
                    prev_key.into_string(),
                ));
            }
            index.insert(prev_key, prev_start..bytes.len());
        }

        Ok(Self { content, index })
    }

    /// The full group content.
    pub fn content(&self) -> &ConfigContent {
        &self.content
    }

    /// A zero-copy slice for `name`, or `None` if the namespace is absent.
    pub fn namespace(&self, name: &str) -> Option<ConfigContent> {
        let range = self.index.get(name)?.clone();
        // The range was built from validated offsets within the content, so
        // slicing cannot go out of bounds.
        self.content.slice(range).ok()
    }

    /// The set of namespace names in this document.
    pub fn namespaces(&self) -> impl Iterator<Item = &str> {
        self.index.keys().map(|s| s.as_ref())
    }
}

/// Parses a top-level YAML mapping key from a line like `namespace-a:`.
/// Returns the key without the trailing `:`, or `None` if the line is not a
/// simple `key:` mapping.
fn parse_top_level_key(line: &[u8]) -> Option<String> {
    // Find the first `:` that is followed by end-of-line, whitespace, or is the
    // last non-CR byte.
    let trimmed = line.strip_suffix(b"\r").unwrap_or(line);
    let colon = trimmed.iter().position(|&b| b == b':')?;
    let key_bytes = &trimmed[..colon];
    let after = &trimmed[colon + 1..];
    // After the colon only whitespace (or nothing) is allowed for a clean
    // mapping key; a value on the same line is still a valid top-level key
    // (e.g. `hash: crc32`), but for namespaces we expect `name:` with nothing
    // after. We accept either, since the byte range we record is the whole
    // block regardless.
    if key_bytes.is_empty() || key_bytes[0] == b' ' {
        return None;
    }
    // Reject keys with interior control characters or spaces (not valid YAML
    // bare keys we care about).
    if key_bytes
        .iter()
        .any(|&b| b == b' ' || b == b'\t' || b.is_ascii_control())
    {
        return None;
    }
    let _ = after;
    Some(String::from_utf8_lossy(key_bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use vintage::Revision;

    fn content(yaml: &str) -> ConfigContent {
        ConfigContent::new(
            Revision::new("sign-x".to_string()),
            Bytes::copy_from_slice(yaml.as_bytes()),
        )
    }

    const TWO_NS: &str =
        "namespace-a:\n  master:\n  - 1.1.1.1:11211\nnamespace-b:\n  master:\n  - 2.2.2.2:11211\n";

    #[test]
    fn splits_namespaces() {
        let doc = GroupDocument::parse(content(TWO_NS)).unwrap();
        let a = doc.namespace("namespace-a").unwrap();
        assert!(a.as_str().unwrap().starts_with("namespace-a:"));
        assert!(!a.as_str().unwrap().contains("namespace-b"));
        let b = doc.namespace("namespace-b").unwrap();
        assert!(b.as_str().unwrap().starts_with("namespace-b:"));
    }

    #[test]
    fn missing_namespace_is_none() {
        let doc = GroupDocument::parse(content(TWO_NS)).unwrap();
        assert!(doc.namespace("nope").is_none());
    }

    #[test]
    fn rejects_duplicate_namespace() {
        let yaml = "ns:\n  master:\n  - 1.1.1.1:11211\nns:\n  master:\n  - 2.2.2.2:11211\n";
        assert!(matches!(
            GroupDocument::parse(content(yaml)),
            Err(GroupDocumentError::DuplicateNamespace(_))
        ));
    }

    #[test]
    fn slices_share_buffer_no_copy() {
        let doc = GroupDocument::parse(content(TWO_NS)).unwrap();
        let a = doc.namespace("namespace-a").unwrap();
        // The slice is a view into the original content's buffer; its revision
        // matches.
        assert_eq!(a.revision().as_str(), "sign-x");
        assert_eq!(doc.namespace("namespace-a").unwrap().len(), a.len());
    }

    #[test]
    fn empty_body_is_empty_document() {
        let doc = GroupDocument::parse(content("")).unwrap();
        assert_eq!(doc.namespaces().count(), 0);
    }
}
