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

//! STS `AssumeRole` credential provider.

use crate::s3::creds::{
    CachedCredentials, Credentials, Provider, STS_VERSION, parse_sts_credentials, xml_error,
};
use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{CONTENT_TYPE, HOST, X_AMZ_CONTENT_SHA256, X_AMZ_DATE};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::signer::{SigningKeyCache, sign_v4_with_service_type};
use crate::s3::types::Region;
use crate::s3::utils::{sha256_hash, to_amz_date, utc_now};
use async_trait::async_trait;
use http::Method;
use std::sync::RwLock;
use std::time::Duration as StdDuration;

const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(60);
const DEFAULT_SESSION_NAME: &str = "minio-rs-assume-role";
const STS_SERVICE: &str = "sts";

/// Credential provider that obtains temporary credentials from an STS endpoint
/// via the [`AssumeRole`](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
/// action. The STS request is signed with AWS Signature V4 using the supplied
/// long-term access and secret keys.
///
/// Like other network-backed providers, [`Provider::fetch`] returns cached
/// credentials; call [`Provider::ensure_credentials`] (awaited) to perform the
/// exchange and prime the cache.
pub struct AssumeRoleProvider {
    sts_endpoint: String,
    access_key: String,
    secret_key: String,
    region: Region,
    role_arn: Option<String>,
    role_session_name: Option<String>,
    duration_seconds: Option<u32>,
    policy: Option<String>,
    external_id: Option<String>,
    signing_key_cache: RwLock<SigningKeyCache>,
    cache: RwLock<Option<CachedCredentials>>,
}

impl std::fmt::Debug for AssumeRoleProvider {
    /// Redacts the secret key so it is never emitted in plaintext.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssumeRoleProvider")
            .field("sts_endpoint", &self.sts_endpoint)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("region", &self.region)
            .field("role_arn", &self.role_arn)
            .field("role_session_name", &self.role_session_name)
            .field("duration_seconds", &self.duration_seconds)
            .field("policy", &self.policy)
            .field("external_id", &self.external_id)
            .field("cache", &self.cache)
            .finish()
    }
}

impl AssumeRoleProvider {
    /// Returns a new provider targeting `sts_endpoint`, signing the request with
    /// the given long-term `access_key`/`secret_key`.
    pub fn new(
        sts_endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Result<Self, ValidationErr> {
        Ok(AssumeRoleProvider {
            sts_endpoint: sts_endpoint.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            region: Region::new("us-east-1")?,
            role_arn: None,
            role_session_name: None,
            duration_seconds: None,
            policy: None,
            external_id: None,
            signing_key_cache: RwLock::new(SigningKeyCache::new()),
            cache: RwLock::new(None),
        })
    }

    /// Sets the region used for the SigV4 signing scope (default `us-east-1`).
    pub fn region(mut self, region: Region) -> Self {
        self.region = region;
        self
    }

    /// Sets the role ARN to assume (`RoleArn`).
    pub fn role_arn(mut self, role_arn: impl Into<String>) -> Self {
        self.role_arn = Some(role_arn.into());
        self
    }

    /// Sets the role session name (`RoleSessionName`).
    pub fn role_session_name(mut self, name: impl Into<String>) -> Self {
        self.role_session_name = Some(name.into());
        self
    }

    /// Sets the requested credential lifetime in seconds (`DurationSeconds`).
    pub fn duration_seconds(mut self, seconds: u32) -> Self {
        self.duration_seconds = Some(seconds);
        self
    }

    /// Sets the session policy applied to the generated credentials.
    pub fn policy(mut self, policy: impl Into<String>) -> Self {
        self.policy = Some(policy.into());
        self
    }

    /// Sets the external ID (`ExternalId`).
    pub fn external_id(mut self, external_id: impl Into<String>) -> Self {
        self.external_id = Some(external_id.into());
        self
    }

    /// Builds the form-urlencoded STS request body for the configured options.
    fn build_request_body(&self) -> String {
        let session_name = self
            .role_session_name
            .clone()
            .unwrap_or_else(|| DEFAULT_SESSION_NAME.to_string());
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer
            .append_pair("Action", "AssumeRole")
            .append_pair("Version", STS_VERSION)
            .append_pair("RoleSessionName", &session_name);
        if let Some(role_arn) = &self.role_arn {
            serializer.append_pair("RoleArn", role_arn);
        }
        if let Some(duration) = self.duration_seconds {
            serializer.append_pair("DurationSeconds", &duration.to_string());
        }
        if let Some(policy) = &self.policy {
            serializer.append_pair("Policy", policy);
        }
        if let Some(external_id) = &self.external_id {
            serializer.append_pair("ExternalId", external_id);
        }
        serializer.finish()
    }

    /// Performs the signed STS exchange and stores the result in the cache.
    pub async fn refresh(&self) -> Result<Credentials, ValidationErr> {
        let body = self.build_request_body();
        let url = reqwest::Url::parse(&self.sts_endpoint)
            .map_err(|e| xml_error(&format!("invalid STS endpoint: {e}")))?;

        let host = match url.port() {
            Some(port) => format!(
                "{}:{port}",
                url.host_str()
                    .ok_or_else(|| xml_error("STS endpoint has no host"))?
            ),
            None => url
                .host_str()
                .ok_or_else(|| xml_error("STS endpoint has no host"))?
                .to_string(),
        };
        let path = url.path();
        let content_sha256 = sha256_hash(body.as_bytes());
        let date = utc_now();

        let mut headers = Multimap::new();
        headers.add(HOST, host);
        headers.add(CONTENT_TYPE, "application/x-www-form-urlencoded");
        headers.add(X_AMZ_DATE, to_amz_date(date));
        headers.add(X_AMZ_CONTENT_SHA256, content_sha256.clone());

        let query_params = Multimap::new();
        sign_v4_with_service_type(
            &self.signing_key_cache,
            STS_SERVICE,
            &Method::POST,
            path,
            &self.region,
            &mut headers,
            &query_params,
            &self.access_key,
            &self.secret_key,
            &content_sha256,
            date,
        );

        let mut request = reqwest::Client::new()
            .post(url.clone())
            .timeout(REQUEST_TIMEOUT)
            .body(body);
        for (key, values) in headers.iter_all() {
            for value in values {
                request = request.header(key, value);
            }
        }

        let response = request.send().await.map_err(ValidationErr::from)?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(xml_error(&format!(
                "STS AssumeRole failed with status {status}: {text}"
            )));
        }

        let cached = parse_sts_credentials(&text, "AssumeRoleResult")?;
        let creds = cached.creds.clone();
        *self.cache.write().unwrap() = Some(cached);
        Ok(creds)
    }
}

#[async_trait]
impl Provider for AssumeRoleProvider {
    fn fetch(&self) -> Credentials {
        self.cache
            .read()
            .unwrap()
            .as_ref()
            .map(|cached| cached.creds.clone())
            .unwrap_or_else(Credentials::empty)
    }

    async fn ensure_credentials(&self) -> Result<Credentials, ValidationErr> {
        if let Some(cached) = self.cache.read().unwrap().as_ref()
            && utc_now() < cached.refresh_after
        {
            return Ok(cached.creds.clone());
        }
        self.refresh().await
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
        let provider = AssumeRoleProvider::new("http://localhost:9000", "ak", "sk").unwrap();
        let params = parse_body(&provider.build_request_body());
        assert_eq!(params["Action"], "AssumeRole");
        assert_eq!(params["Version"], "2011-06-15");
        assert_eq!(params["RoleSessionName"], DEFAULT_SESSION_NAME);
        assert!(!params.contains_key("RoleArn"));
        assert!(!params.contains_key("DurationSeconds"));
    }

    #[test]
    fn build_request_body_with_options() {
        let provider = AssumeRoleProvider::new("http://localhost:9000", "ak", "sk")
            .unwrap()
            .role_arn("arn:aws:iam::123:role/test")
            .role_session_name("sess")
            .duration_seconds(3600)
            .policy("{}")
            .external_id("ext-1");
        let params = parse_body(&provider.build_request_body());
        assert_eq!(params["RoleArn"], "arn:aws:iam::123:role/test");
        assert_eq!(params["RoleSessionName"], "sess");
        assert_eq!(params["DurationSeconds"], "3600");
        assert_eq!(params["Policy"], "{}");
        assert_eq!(params["ExternalId"], "ext-1");
    }

    #[test]
    fn parses_assume_role_response() {
        let xml = r#"<?xml version="1.0"?>
<AssumeRoleResponse>
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>AKID</AccessKeyId>
      <SecretAccessKey>SECRET</SecretAccessKey>
      <SessionToken>TOKEN</SessionToken>
      <Expiration>2030-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;
        let cached = parse_sts_credentials(xml, "AssumeRoleResult").unwrap();
        assert_eq!(cached.creds.access_key, "AKID");
        assert_eq!(cached.creds.secret_key, "SECRET");
        assert_eq!(cached.creds.session_token.as_deref(), Some("TOKEN"));
    }

    #[test]
    fn fetch_empty_before_priming() {
        let provider = AssumeRoleProvider::new("http://localhost:9000", "ak", "sk").unwrap();
        assert!(provider.fetch().is_empty());
    }

    #[tokio::test]
    async fn assume_role_sts_flow_is_signed() {
        use crate::s3::creds::mock_http::{self, Request, Responder};
        use std::sync::Arc;

        const STS_XML: &str = r#"<?xml version="1.0"?>
<AssumeRoleResponse><AssumeRoleResult><Credentials>
<AccessKeyId>ARAK</AccessKeyId><SecretAccessKey>ARSK</SecretAccessKey>
<SessionToken>ARTOKEN</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration>
</Credentials></AssumeRoleResult></AssumeRoleResponse>"#;

        // Require a SigV4 Authorization header so the test fails if the request
        // is not signed.
        let responder: Responder = Arc::new(|req: &Request| {
            let signed = req
                .headers
                .get("authorization")
                .is_some_and(|v| v.starts_with("AWS4-HMAC-SHA256"));
            if req.method == "POST" && signed {
                (200, STS_XML.to_string())
            } else {
                (400, "unsigned request".to_string())
            }
        });
        let server = mock_http::start(responder).await;

        let creds = AssumeRoleProvider::new(&server.base_url, "ak", "sk")
            .unwrap()
            .role_arn("arn:aws:iam::123456789012:role/test")
            .refresh()
            .await
            .unwrap();
        assert_eq!(creds.access_key, "ARAK");
        assert_eq!(creds.secret_key, "ARSK");
        assert_eq!(creds.session_token.as_deref(), Some("ARTOKEN"));
    }
}
