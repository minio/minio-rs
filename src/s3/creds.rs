// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Credential providers

use crate::s3::error::ValidationErr;
use crate::s3::utils::{UtcTime, from_iso8601utc, utc_now};
use chrono::Duration;
use std::sync::RwLock;
use xmltree::Element;

/// STS API version used by the MinIO Security Token Service.
const STS_VERSION: &str = "2011-06-15";

/// Fraction of a credential's lifetime that must elapse before it is
/// refreshed, mirroring Go's `defaultExpiryWindow` of 0.8 (refresh once 80% of
/// the lifetime has passed, i.e. when only the final 20% remains).
const DEFAULT_EXPIRY_WINDOW_RATIO: f64 = 0.8;

/// Credentials containing access key, secret key, and optional session token.
#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

/// Provider trait to fetch credentials.
pub trait Provider: std::fmt::Debug {
    fn fetch(&self) -> Credentials;
}

/// Static credential provider.
#[derive(Clone, Debug)]
pub struct StaticProvider {
    creds: Credentials,
}

impl StaticProvider {
    /// Returns a static provider with given access key, secret key, and optional session token.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::creds::StaticProvider;
    /// let provider = StaticProvider::new("minioadmin", "minio123", None);
    /// ```
    pub fn new(access_key: &str, secret_key: &str, session_token: Option<&str>) -> StaticProvider {
        StaticProvider {
            creds: Credentials {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                session_token: session_token.map(|v| v.to_string()),
            },
        }
    }
}

impl Provider for StaticProvider {
    fn fetch(&self) -> Credentials {
        self.creds.clone()
    }
}

fn validate_config_name(name: &str) -> Result<(), ValidationErr> {
    if name.trim().is_empty() {
        return Err(ValidationErr::InvalidIdpConfigName(
            "config name must not be empty or whitespace".to_string(),
        ));
    }
    Ok(())
}

/// LDAP STS credential provider that obtains temporary credentials from the
/// MinIO Security Token Service via the
/// [`AssumeRoleWithLDAPIdentity`](https://min.io/docs/minio/linux/developers/minio-drivers.html)
/// action (MinIO extension).
///
/// Credentials are fetched over HTTP and cached until shortly before their
/// reported expiration. Because [`Provider::fetch`] is synchronous and
/// infallible, the network exchange happens in [`LdapIdentityProvider::refresh`]
/// (async) and must be primed before the provider is used for signing; see
/// [`LdapIdentityProvider::fetch_credentials`].
#[derive(Debug)]
pub struct LdapIdentityProvider {
    sts_endpoint: String,
    ldap_username: String,
    ldap_password: String,
    policy: Option<String>,
    duration_seconds: Option<u32>,
    config_name: Option<String>,
    cache: RwLock<Option<CachedCredentials>>,
}

#[derive(Clone, Debug)]
struct CachedCredentials {
    creds: Credentials,
    refresh_after: UtcTime,
}

impl LdapIdentityProvider {
    /// Returns a new LDAP STS provider targeting `sts_endpoint` for the given
    /// LDAP `username` and `password`.
    pub fn new(
        sts_endpoint: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        LdapIdentityProvider {
            sts_endpoint: sts_endpoint.into(),
            ldap_username: username.into(),
            ldap_password: password.into(),
            policy: None,
            duration_seconds: None,
            config_name: None,
            cache: RwLock::new(None),
        }
    }

    /// Sets the session policy applied to the generated credentials.
    pub fn policy(mut self, policy: impl Into<String>) -> Self {
        self.policy = Some(policy.into());
        self
    }

    /// Sets the requested credential lifetime in seconds (`DurationSeconds`).
    pub fn duration_seconds(mut self, seconds: u32) -> Self {
        self.duration_seconds = Some(seconds);
        self
    }

    /// Sets the LDAP `ConfigName` (MinIO extension, go #2173). Returns an error
    /// if the name is empty or whitespace.
    pub fn config_name(mut self, name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        validate_config_name(&name)?;
        self.config_name = Some(name);
        Ok(self)
    }

    /// Builds the form-urlencoded STS request body for the configured options.
    fn build_request_body(&self) -> String {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer
            .append_pair("Action", "AssumeRoleWithLDAPIdentity")
            .append_pair("Version", STS_VERSION)
            .append_pair("LDAPUsername", &self.ldap_username)
            .append_pair("LDAPPassword", &self.ldap_password);
        if let Some(policy) = &self.policy {
            serializer.append_pair("Policy", policy);
        }
        if let Some(duration) = self.duration_seconds {
            serializer.append_pair("DurationSeconds", &duration.to_string());
        }
        if let Some(config_name) = &self.config_name {
            serializer.append_pair("ConfigName", config_name);
        }
        serializer.finish()
    }

    /// Performs the STS exchange and stores the result in the cache.
    pub async fn refresh(&self) -> Result<Credentials, ValidationErr> {
        let body = self.build_request_body();
        let response = reqwest::Client::new()
            .post(&self.sts_endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(ValidationErr::from)?;

        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(ValidationErr::XmlError {
                message: format!("STS request failed with status {status}: {text}"),
                source: None,
            });
        }

        let cached = parse_ldap_identity_response(&text)?;
        let creds = cached.creds.clone();
        *self.cache.write().unwrap() = Some(cached);
        Ok(creds)
    }

    /// Returns cached credentials, refreshing via [`refresh`](Self::refresh)
    /// when the cache is empty or within the expiry window.
    pub async fn fetch_credentials(&self) -> Result<Credentials, ValidationErr> {
        if let Some(creds) = self.cached_if_valid() {
            return Ok(creds);
        }
        self.refresh().await
    }

    fn cached_if_valid(&self) -> Option<Credentials> {
        let guard = self.cache.read().unwrap();
        let cached = guard.as_ref()?;
        if utc_now() < cached.refresh_after {
            Some(cached.creds.clone())
        } else {
            None
        }
    }
}

fn xml_error(message: &str) -> ValidationErr {
    ValidationErr::XmlError {
        message: message.to_string(),
        source: None,
    }
}

fn parse_ldap_identity_response(xml: &str) -> Result<CachedCredentials, ValidationErr> {
    let now = utc_now();
    let root = Element::parse(xml.as_bytes())?;
    let credentials = root
        .get_child("AssumeRoleWithLDAPIdentityResult")
        .and_then(|result| result.get_child("Credentials"))
        .ok_or_else(|| xml_error("missing Credentials in STS response"))?;

    let text = |name: &str| -> Option<String> {
        credentials
            .get_child(name)
            .and_then(|element| element.get_text())
            .map(|cow| cow.into_owned())
    };

    let access_key =
        text("AccessKeyId").ok_or_else(|| xml_error("missing AccessKeyId in STS response"))?;
    let secret_key = text("SecretAccessKey")
        .ok_or_else(|| xml_error("missing SecretAccessKey in STS response"))?;
    let session_token = text("SessionToken");
    let expiration = match text("Expiration") {
        Some(value) => from_iso8601utc(&value)?,
        None => now + Duration::hours(1),
    };

    Ok(CachedCredentials {
        creds: Credentials {
            access_key,
            secret_key,
            session_token,
        },
        refresh_after: refresh_deadline(now, expiration),
    })
}

/// Computes the instant after which credentials should be refreshed, applying
/// the expiry-window ratio to the time remaining until `expiration`.
fn refresh_deadline(now: UtcTime, expiration: UtcTime) -> UtcTime {
    let remaining = expiration - now;
    if remaining <= Duration::zero() {
        return expiration;
    }
    let elapsed_before_refresh = remaining.num_milliseconds() as f64 * DEFAULT_EXPIRY_WINDOW_RATIO;
    now + Duration::milliseconds(elapsed_before_refresh as i64)
}

impl Provider for LdapIdentityProvider {
    fn fetch(&self) -> Credentials {
        self.cache
            .read()
            .unwrap()
            .as_ref()
            .map(|cached| cached.creds.clone())
            .unwrap_or_else(|| Credentials {
                access_key: String::new(),
                secret_key: String::new(),
                session_token: None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn parse_body(body: &str) -> HashMap<String, String> {
        url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect()
    }

    #[test]
    fn build_request_body_minimal() {
        let provider = LdapIdentityProvider::new("http://localhost:9000", "alice", "secret123");
        let params = parse_body(&provider.build_request_body());

        assert_eq!(params["Action"], "AssumeRoleWithLDAPIdentity");
        assert_eq!(params["Version"], "2011-06-15");
        assert_eq!(params["LDAPUsername"], "alice");
        assert_eq!(params["LDAPPassword"], "secret123");
        assert!(!params.contains_key("Policy"));
        assert!(!params.contains_key("DurationSeconds"));
        assert!(!params.contains_key("ConfigName"));
    }

    #[test]
    fn build_request_body_with_options() {
        let provider = LdapIdentityProvider::new("http://localhost:9000", "bob", "pw")
            .policy("{\"Version\":\"2012-10-17\"}")
            .duration_seconds(3600)
            .config_name("ldap-corp")
            .unwrap();
        let params = parse_body(&provider.build_request_body());

        assert_eq!(params["ConfigName"], "ldap-corp");
        assert_eq!(params["DurationSeconds"], "3600");
        assert_eq!(params["Policy"], "{\"Version\":\"2012-10-17\"}");
        assert_eq!(params["LDAPUsername"], "bob");
    }

    #[test]
    fn config_name_rejects_empty() {
        let provider = LdapIdentityProvider::new("http://localhost:9000", "u", "p");
        assert!(matches!(
            provider.config_name("   "),
            Err(ValidationErr::InvalidIdpConfigName(_))
        ));
    }

    #[test]
    fn parse_response_extracts_credentials() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithLDAPIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithLDAPIdentityResult>
    <Credentials>
      <AccessKeyId>AKIATEST</AccessKeyId>
      <SecretAccessKey>SECRETTEST</SecretAccessKey>
      <SessionToken>TOKENTEST</SessionToken>
      <Expiration>2030-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithLDAPIdentityResult>
</AssumeRoleWithLDAPIdentityResponse>"#;

        let cached = parse_ldap_identity_response(xml).unwrap();
        assert_eq!(cached.creds.access_key, "AKIATEST");
        assert_eq!(cached.creds.secret_key, "SECRETTEST");
        assert_eq!(cached.creds.session_token.as_deref(), Some("TOKENTEST"));
        let expiration = from_iso8601utc("2030-01-01T00:00:00Z").unwrap();
        assert!(cached.refresh_after > utc_now());
        assert!(cached.refresh_after < expiration);
    }

    #[test]
    fn refresh_deadline_applies_ratio() {
        let now = from_iso8601utc("2030-01-01T00:00:00Z").unwrap();
        let expiration = now + Duration::seconds(100);
        let deadline = refresh_deadline(now, expiration);
        assert_eq!(deadline, now + Duration::seconds(80));
    }

    #[test]
    fn refresh_deadline_past_expiration() {
        let now = from_iso8601utc("2030-01-01T00:00:00Z").unwrap();
        let expiration = now - Duration::seconds(5);
        assert_eq!(refresh_deadline(now, expiration), expiration);
    }

    #[test]
    fn parse_response_missing_credentials_errors() {
        let xml = r#"<AssumeRoleWithLDAPIdentityResponse><AssumeRoleWithLDAPIdentityResult></AssumeRoleWithLDAPIdentityResult></AssumeRoleWithLDAPIdentityResponse>"#;
        assert!(matches!(
            parse_ldap_identity_response(xml),
            Err(ValidationErr::XmlError { .. })
        ));
    }

    #[test]
    fn fetch_returns_empty_before_priming() {
        let provider = LdapIdentityProvider::new("http://localhost:9000", "u", "p");
        let creds = provider.fetch();
        assert!(creds.access_key.is_empty());
        assert!(creds.secret_key.is_empty());
        assert!(creds.session_token.is_none());
    }

    #[test]
    fn cached_if_valid_respects_expiry_window() {
        let provider = LdapIdentityProvider::new("http://localhost:9000", "u", "p");
        let creds = Credentials {
            access_key: "AK".to_string(),
            secret_key: "SK".to_string(),
            session_token: None,
        };

        *provider.cache.write().unwrap() = Some(CachedCredentials {
            creds: creds.clone(),
            refresh_after: utc_now() + Duration::hours(1),
        });
        assert_eq!(provider.cached_if_valid().unwrap().access_key, "AK");
        assert_eq!(provider.fetch().access_key, "AK");

        *provider.cache.write().unwrap() = Some(CachedCredentials {
            creds,
            refresh_after: utc_now() - Duration::seconds(1),
        });
        assert!(provider.cached_if_valid().is_none());
    }
}
