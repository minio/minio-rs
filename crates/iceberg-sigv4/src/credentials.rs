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

//! AWS credentials for SigV4 signing.
//!
//! Provides the [`Credentials`] struct for storing AWS access keys
//! and optional session tokens (for temporary STS credentials).

use std::fmt;

/// AWS credentials for signing requests.
///
/// Supports both permanent credentials (access key + secret key) and
/// temporary credentials from STS (with session token).
///
/// # Example
///
/// ```
/// use iceberg_sigv4::Credentials;
///
/// // Permanent credentials
/// let creds = Credentials::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
///
/// // Temporary credentials (from STS)
/// let temp_creds = Credentials::with_session_token(
///     "ASIATEMPORARY",
///     "temporarysecret",
///     "session-token-from-sts",
/// );
/// ```
#[derive(Clone)]
pub struct Credentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl Credentials {
    /// Creates new permanent credentials.
    ///
    /// # Arguments
    ///
    /// * `access_key` - AWS Access Key ID
    /// * `secret_key` - AWS Secret Access Key
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    /// Creates temporary credentials with a session token.
    ///
    /// Use this for credentials obtained from AWS STS (Security Token Service).
    ///
    /// # Arguments
    ///
    /// * `access_key` - Temporary AWS Access Key ID
    /// * `secret_key` - Temporary AWS Secret Access Key
    /// * `session_token` - Session token from STS
    pub fn with_session_token(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: Some(session_token.into()),
        }
    }

    /// Returns the access key.
    #[inline]
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Returns the secret key.
    #[inline]
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    /// Returns the session token, if present.
    #[inline]
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// Returns true if this represents temporary credentials (has session token).
    #[inline]
    pub fn is_temporary(&self) -> bool {
        self.session_token.is_some()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permanent_credentials() {
        let creds = Credentials::new("AKIATEST", "secretkey");
        assert_eq!(creds.access_key(), "AKIATEST");
        assert_eq!(creds.secret_key(), "secretkey");
        assert!(creds.session_token().is_none());
        assert!(!creds.is_temporary());
    }

    #[test]
    fn test_temporary_credentials() {
        let creds = Credentials::with_session_token("ASIATEMP", "tempsecret", "token123");
        assert_eq!(creds.access_key(), "ASIATEMP");
        assert_eq!(creds.secret_key(), "tempsecret");
        assert_eq!(creds.session_token(), Some("token123"));
        assert!(creds.is_temporary());
    }

    #[test]
    fn test_debug_redacts_secrets() {
        let creds = Credentials::with_session_token("AKIATEST", "secretkey", "mysessiontokenvalue");
        let debug_str = format!("{:?}", creds);

        // Access key should be visible
        assert!(debug_str.contains("AKIATEST"));

        // Secrets should be redacted
        assert!(!debug_str.contains("secretkey"));
        assert!(!debug_str.contains("mysessiontokenvalue"));
        assert!(debug_str.contains("[REDACTED]"));
    }

    #[test]
    fn test_clone() {
        let creds = Credentials::new("AKIATEST", "secret");
        let cloned = creds.clone();
        assert_eq!(creds.access_key(), cloned.access_key());
        assert_eq!(creds.secret_key(), cloned.secret_key());
    }
}
