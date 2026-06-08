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

//! Shared AWS credentials file provider.

use crate::s3::creds::{Credentials, Provider};
use async_trait::async_trait;
use std::env;
use std::path::PathBuf;
use std::sync::RwLock;

/// Credential provider that reads credentials from the shared AWS credentials
/// file (e.g. `~/.aws/credentials`).
///
/// The file location defaults to `AWS_SHARED_CREDENTIALS_FILE` when set,
/// otherwise `$HOME/.aws/credentials` (`%USERPROFILE%\.aws\credentials` on
/// Windows). The profile defaults to `AWS_PROFILE` (then `AWS_DEFAULT_PROFILE`),
/// otherwise `default`. Profile sections are matched as `[name]` or
/// `[profile name]`. The file is parsed lazily on first [`Provider::fetch`] and
/// the result is cached.
#[derive(Debug)]
pub struct FileProvider {
    filename: Option<PathBuf>,
    profile: Option<String>,
    cache: RwLock<Option<Credentials>>,
}

impl Default for FileProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl FileProvider {
    /// Returns a provider that resolves the file path and profile from the
    /// environment using the AWS defaults.
    pub fn new() -> Self {
        FileProvider {
            filename: None,
            profile: None,
            cache: RwLock::new(None),
        }
    }

    /// Overrides the credentials file path.
    pub fn filename(mut self, path: impl Into<PathBuf>) -> Self {
        self.filename = Some(path.into());
        self
    }

    /// Overrides the profile name to read.
    pub fn profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }

    fn resolved_filename(&self) -> Option<PathBuf> {
        if let Some(path) = &self.filename {
            return Some(path.clone());
        }
        if let Ok(path) = env::var("AWS_SHARED_CREDENTIALS_FILE")
            && !path.is_empty()
        {
            return Some(PathBuf::from(path));
        }
        let home = env::var("HOME")
            .ok()
            .or_else(|| env::var("USERPROFILE").ok())
            .filter(|v| !v.is_empty())?;
        Some(PathBuf::from(home).join(".aws").join("credentials"))
    }

    fn resolved_profile(&self) -> String {
        if let Some(profile) = &self.profile {
            return profile.clone();
        }
        env::var("AWS_PROFILE")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                env::var("AWS_DEFAULT_PROFILE")
                    .ok()
                    .filter(|v| !v.is_empty())
            })
            .unwrap_or_else(|| "default".to_string())
    }

    fn load(&self) -> Credentials {
        let Some(path) = self.resolved_filename() else {
            return Credentials::empty();
        };
        let Ok(content) = std::fs::read_to_string(&path) else {
            return Credentials::empty();
        };
        parse_ini_profile(&content, &self.resolved_profile()).unwrap_or_else(Credentials::empty)
    }
}

/// Parses the requested `profile` section out of credentials-file `content`.
///
/// Returns the credentials when both an access key and secret key are present.
/// Matches both `[profile]` and `[profile <name>]` section headers.
fn parse_ini_profile(content: &str, profile: &str) -> Option<Credentials> {
    let mut in_section = false;
    let mut access_key: Option<String> = None;
    let mut secret_key: Option<String> = None;
    let mut session_token: Option<String> = None;

    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(section) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            let section = section.trim();
            let name = section.strip_prefix("profile ").unwrap_or(section).trim();
            in_section = name == profile;
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().to_string();
        match key.trim().to_ascii_lowercase().as_str() {
            "aws_access_key_id" => access_key = Some(value),
            "aws_secret_access_key" => secret_key = Some(value),
            "aws_session_token" => session_token = Some(value),
            _ => {}
        }
    }

    match (access_key, secret_key) {
        (Some(access_key), Some(secret_key))
            if !access_key.is_empty() && !secret_key.is_empty() =>
        {
            Some(Credentials {
                access_key,
                secret_key,
                session_token: session_token.filter(|v| !v.is_empty()),
            })
        }
        _ => None,
    }
}

#[async_trait]
impl Provider for FileProvider {
    fn fetch(&self) -> Credentials {
        if let Some(creds) = self.cache.read().unwrap().as_ref() {
            return creds.clone();
        }
        let creds = self.load();
        *self.cache.write().unwrap() = Some(creds.clone());
        creds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
# comment line
[default]
aws_access_key_id = DEFAULT_AK
aws_secret_access_key = DEFAULT_SK

[work]
aws_access_key_id = WORK_AK
aws_secret_access_key = WORK_SK
aws_session_token = WORK_TOKEN

[profile spaced]
aws_access_key_id = SPACED_AK
aws_secret_access_key = SPACED_SK
"#;

    #[test]
    fn parses_default_profile() {
        let creds = parse_ini_profile(SAMPLE, "default").unwrap();
        assert_eq!(creds.access_key, "DEFAULT_AK");
        assert_eq!(creds.secret_key, "DEFAULT_SK");
        assert!(creds.session_token.is_none());
    }

    #[test]
    fn parses_named_profile_with_token() {
        let creds = parse_ini_profile(SAMPLE, "work").unwrap();
        assert_eq!(creds.access_key, "WORK_AK");
        assert_eq!(creds.secret_key, "WORK_SK");
        assert_eq!(creds.session_token.as_deref(), Some("WORK_TOKEN"));
    }

    #[test]
    fn matches_profile_prefixed_section() {
        let creds = parse_ini_profile(SAMPLE, "spaced").unwrap();
        assert_eq!(creds.access_key, "SPACED_AK");
        assert_eq!(creds.secret_key, "SPACED_SK");
    }

    #[test]
    fn unknown_profile_returns_none() {
        assert!(parse_ini_profile(SAMPLE, "missing").is_none());
    }

    #[test]
    fn incomplete_profile_returns_none() {
        let content = "[partial]\naws_access_key_id = ONLY_AK\n";
        assert!(parse_ini_profile(content, "partial").is_none());
    }
}
