// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::s3::utils::url_encode;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Multimap for string key and string value
pub type Multimap = multimap::MultiMap<String, String>;

/// Collapses multiple spaces into a single space (avoids regex overhead).
///
/// Returns `Cow::Borrowed` when no transformation is needed (common case),
/// avoiding allocation for header values that don't contain consecutive spaces.
#[inline]
fn collapse_spaces(s: &str) -> Cow<'_, str> {
    let trimmed = s.trim();
    if !trimmed.contains("  ") {
        return Cow::Borrowed(trimmed);
    }
    let mut result = String::with_capacity(trimmed.len());
    let mut prev_space = false;
    for c in trimmed.chars() {
        if c == ' ' {
            if !prev_space {
                result.push(' ');
                prev_space = true;
            }
        } else {
            result.push(c);
            prev_space = false;
        }
    }
    Cow::Owned(result)
}

pub trait MultimapExt {
    /// Adds a key-value pair to the multimap
    fn add<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V);

    /// Adds a multimap to the current multimap
    fn add_multimap(&mut self, other: Multimap);

    fn add_version(&mut self, version: Option<String>);

    #[must_use]
    fn take_version(self) -> Option<String>;

    /// Converts multimap to HTTP query string
    fn to_query_string(&self) -> String;

    /// Converts multimap to canonical query string
    fn get_canonical_query_string(&self) -> String;

    /// Converts multimap to signed headers and canonical headers
    fn get_canonical_headers(&self) -> (String, String);
}

impl MultimapExt for Multimap {
    fn add<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
        self.insert(key.into(), value.into());
    }
    fn add_multimap(&mut self, other: Multimap) {
        for (key, values) in other.into_iter() {
            self.insert_many(key.clone(), values);
        }
    }
    fn add_version(&mut self, version: Option<String>) {
        if let Some(v) = version {
            self.insert("versionId".into(), v);
        }
    }
    fn take_version(mut self) -> Option<String> {
        self.remove("versionId").and_then(|mut v| v.pop())
    }
    fn to_query_string(&self) -> String {
        let mut query = String::new();
        for (key, values) in self.iter_all() {
            for value in values {
                if !query.is_empty() {
                    query.push('&');
                }
                query.push_str(&url_encode(key));
                query.push('=');
                query.push_str(&url_encode(value));
            }
        }
        query
    }

    fn get_canonical_query_string(&self) -> String {
        // Use BTreeMap for automatic sorting (avoids explicit sort)
        let mut sorted: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut total_len = 0usize;

        for (key, values) in self.iter_all() {
            for value in values {
                // Pre-calculate total length to avoid reallocations.
                // Most S3 query params are alphanumeric (uploadId, partNumber, versionId)
                // so we use actual length + 20% buffer for occasional URL encoding.
                total_len += key.len() + 1 + value.len() + 2; // key=value&
            }
            sorted
                .entry(key.as_str())
                .or_default()
                .extend(values.iter().map(|s| s.as_str()));
        }

        // Add 20% buffer for URL encoding overhead
        let mut query = String::with_capacity(total_len + total_len / 5);
        for (key, values) in sorted {
            for value in values {
                if !query.is_empty() {
                    query.push('&');
                }
                query.push_str(&url_encode(key));
                query.push('=');
                query.push_str(&url_encode(value));
            }
        }

        query
    }

    fn get_canonical_headers(&self) -> (String, String) {
        // Use BTreeMap for automatic sorting (avoids explicit sort)
        let mut btmap: BTreeMap<String, String> = BTreeMap::new();

        // Pre-calculate sizes for better allocation
        let mut key_bytes = 0usize;
        let mut value_bytes = 0usize;

        for (k, values) in self.iter_all() {
            let key = k.to_lowercase();
            if key == "authorization" || key == "user-agent" {
                continue;
            }

            // Sort values in place if needed
            let mut vs: Vec<&String> = values.iter().collect();
            vs.sort();

            let mut value =
                String::with_capacity(vs.iter().map(|v| v.len()).sum::<usize>() + vs.len());
            for v in vs {
                if !value.is_empty() {
                    value.push(',');
                }
                value.push_str(&collapse_spaces(v));
            }

            key_bytes += key.len();
            value_bytes += value.len();
            btmap.insert(key, value);
        }

        // Pre-allocate output strings
        let header_count = btmap.len();
        let mut signed_headers = String::with_capacity(key_bytes + header_count);
        let mut canonical_headers =
            String::with_capacity(key_bytes + value_bytes + header_count * 2);

        let mut add_delim = false;
        for (key, value) in &btmap {
            if add_delim {
                signed_headers.push(';');
                canonical_headers.push('\n');
            }

            signed_headers.push_str(key);

            canonical_headers.push_str(key);
            canonical_headers.push(':');
            canonical_headers.push_str(value);

            add_delim = true;
        }

        (signed_headers, canonical_headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collapse_spaces_no_consecutive_spaces() {
        // Should return Cow::Borrowed (no allocation)
        let result = collapse_spaces("hello world");
        assert_eq!(result, "hello world");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_with_consecutive_spaces() {
        // Should return Cow::Owned with spaces collapsed
        let result = collapse_spaces("hello  world");
        assert_eq!(result, "hello world");
        assert!(matches!(result, Cow::Owned(_)));

        let result = collapse_spaces("hello   world");
        assert_eq!(result, "hello world");

        let result = collapse_spaces("a  b  c  d");
        assert_eq!(result, "a b c d");
    }

    #[test]
    fn test_collapse_spaces_multiple_groups() {
        let result = collapse_spaces("hello  world  foo   bar");
        assert_eq!(result, "hello world foo bar");
    }

    #[test]
    fn test_collapse_spaces_leading_trailing() {
        // Leading and trailing spaces should be trimmed
        let result = collapse_spaces("  hello world  ");
        assert_eq!(result, "hello world");
        assert!(matches!(result, Cow::Borrowed(_)));

        let result = collapse_spaces("  hello  world  ");
        assert_eq!(result, "hello world");
        assert!(matches!(result, Cow::Owned(_)));
    }

    #[test]
    fn test_collapse_spaces_only_spaces() {
        let result = collapse_spaces("   ");
        assert_eq!(result, "");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_empty_string() {
        let result = collapse_spaces("");
        assert_eq!(result, "");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_single_space() {
        let result = collapse_spaces(" ");
        assert_eq!(result, "");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_no_spaces() {
        let result = collapse_spaces("helloworld");
        assert_eq!(result, "helloworld");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_tabs_not_collapsed() {
        // Only spaces are collapsed, not tabs
        let result = collapse_spaces("hello\t\tworld");
        assert_eq!(result, "hello\t\tworld");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_mixed_whitespace() {
        // Tabs and spaces mixed - only consecutive spaces collapsed
        let result = collapse_spaces("hello  \t  world");
        assert_eq!(result, "hello \t world");
    }

    #[test]
    fn test_collapse_spaces_realistic_header_value() {
        // Realistic header value that should not need modification
        let result = collapse_spaces("application/json");
        assert_eq!(result, "application/json");
        assert!(matches!(result, Cow::Borrowed(_)));

        let result = collapse_spaces("bytes=0-1023");
        assert_eq!(result, "bytes=0-1023");
        assert!(matches!(result, Cow::Borrowed(_)));
    }
}
