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

//! Environment-variable credential provider.

use crate::s3::creds::{Credentials, Provider};
use async_trait::async_trait;
use std::env;

/// Credential provider that reads credentials from the standard AWS
/// environment variables.
///
/// Looks up `AWS_ACCESS_KEY_ID` (falling back to `AWS_ACCESS_KEY`),
/// `AWS_SECRET_ACCESS_KEY` (falling back to `AWS_SECRET_KEY`), and the optional
/// `AWS_SESSION_TOKEN`. The environment is read on every [`Provider::fetch`] so
/// changes are picked up without reconstructing the provider.
#[derive(Clone, Debug, Default)]
pub struct EnvProvider;

impl EnvProvider {
    /// Returns a new environment credential provider.
    pub fn new() -> Self {
        EnvProvider
    }
}

fn non_empty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

#[async_trait]
impl Provider for EnvProvider {
    fn fetch(&self) -> Credentials {
        let access_key = non_empty("AWS_ACCESS_KEY_ID")
            .or_else(|| non_empty("AWS_ACCESS_KEY"))
            .unwrap_or_default();
        let secret_key = non_empty("AWS_SECRET_ACCESS_KEY")
            .or_else(|| non_empty("AWS_SECRET_KEY"))
            .unwrap_or_default();
        let session_token = non_empty("AWS_SESSION_TOKEN");
        Credentials {
            access_key,
            secret_key,
            session_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serializes tests that mutate process-wide environment variables.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_ACCESS_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SECRET_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    fn reads_primary_variables() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            env::set_var("AWS_ACCESS_KEY_ID", "AKID");
            env::set_var("AWS_SECRET_ACCESS_KEY", "SECRET");
            env::set_var("AWS_SESSION_TOKEN", "TOKEN");
        }
        let creds = EnvProvider::new().fetch();
        assert_eq!(creds.access_key, "AKID");
        assert_eq!(creds.secret_key, "SECRET");
        assert_eq!(creds.session_token.as_deref(), Some("TOKEN"));
        clear_env();
    }

    #[test]
    fn falls_back_to_legacy_variables() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            env::set_var("AWS_ACCESS_KEY", "LEGACY_AK");
            env::set_var("AWS_SECRET_KEY", "LEGACY_SK");
        }
        let creds = EnvProvider::new().fetch();
        assert_eq!(creds.access_key, "LEGACY_AK");
        assert_eq!(creds.secret_key, "LEGACY_SK");
        assert!(creds.session_token.is_none());
        clear_env();
    }

    #[test]
    fn empty_when_unset() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let creds = EnvProvider::new().fetch();
        assert!(creds.is_empty());
        assert!(creds.session_token.is_none());
    }
}
