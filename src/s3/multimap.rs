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

use crate::s3::utils::urlencode;
use lazy_static::lazy_static;
use multimap::MultiMap;
use regex::Regex;
use std::collections::BTreeMap;
pub use urlencoding::decode as urldecode;

/// Multimap for string key and string value
pub type Multimap = MultiMap<String, String>;

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
            for value in values {
                self.insert(key.clone(), value);
            }
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
                query.push_str(&urlencode(key));
                query.push('=');
                query.push_str(&urlencode(value));
            }
        }
        query
    }

    fn get_canonical_query_string(&self) -> String {
        let mut keys: Vec<String> = Vec::new();
        for (key, _) in self.iter() {
            keys.push(key.to_string());
        }
        keys.sort();

        let mut query = String::new();
        for key in keys {
            match self.get_vec(key.as_str()) {
                Some(values) => {
                    for value in values {
                        if !query.is_empty() {
                            query.push('&');
                        }
                        query.push_str(&urlencode(key.as_str()));
                        query.push('=');
                        query.push_str(&urlencode(value));
                    }
                }
                None => todo!(), // This never happens.
            };
        }

        query
    }

    fn get_canonical_headers(&self) -> (String, String) {
        lazy_static! {
            static ref MULTI_SPACE_REGEX: Regex = Regex::new("( +)").unwrap();
        }
        let mut btmap: BTreeMap<String, String> = BTreeMap::new();

        for (k, values) in self.iter_all() {
            let key = k.to_lowercase();
            if "authorization" == key || "user-agent" == key {
                continue;
            }

            let mut vs = values.clone();
            vs.sort();

            let mut value = String::new();
            for v in vs {
                if !value.is_empty() {
                    value.push(',');
                }
                let s: String = MULTI_SPACE_REGEX.replace_all(&v, " ").trim().to_string();
                value.push_str(&s);
            }
            btmap.insert(key.clone(), value.clone());
        }

        let mut signed_headers = String::new();
        let mut canonical_headers = String::new();
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
