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

//! Web-identity STS credential provider (`AssumeRoleWithWebIdentity`).

use crate::s3::creds::{
    CachedCredentials, Credentials, Provider, STS_VERSION, parse_sts_credentials, xml_error,
};
use crate::s3::error::ValidationErr;
use crate::s3::utils::utc_now;
use async_trait::async_trait;
use std::env;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration as StdDuration;

const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(60);
const DEFAULT_SESSION_NAME: &str = "minio-rs-web-identity";

/// Credential provider that obtains temporary credentials from an STS endpoint
/// via the [`AssumeRoleWithWebIdentity`](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html)
/// action, using an OIDC/JWT token (e.g. an EKS service-account token).
///
/// The role ARN, web-identity token file, and session name default to the
/// `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, and `AWS_ROLE_SESSION_NAME`
/// environment variables respectively, and may be overridden with the builder
/// methods. Like other network-backed providers, [`Provider::fetch`] returns
/// cached credentials; call [`Provider::ensure_credentials`] to perform the
/// exchange.
#[derive(Debug)]
pub struct WebIdentityProvider {
    sts_endpoint: String,
    role_arn: Option<String>,
    role_session_name: Option<String>,
    token_file: Option<PathBuf>,
    duration_seconds: Option<u32>,
    policy: Option<String>,
    cache: RwLock<Option<CachedCredentials>>,
}

impl WebIdentityProvider {
    /// Returns a new provider targeting `sts_endpoint`, reading the role ARN,
    /// token file, and session name from the environment unless overridden.
    pub fn new(sts_endpoint: impl Into<String>) -> Self {
        WebIdentityProvider {
            sts_endpoint: sts_endpoint.into(),
            role_arn: None,
            role_session_name: None,
            token_file: None,
            duration_seconds: None,
            policy: None,
            cache: RwLock::new(None),
        }
    }

    /// Overrides the role ARN (`RoleArn`).
    pub fn role_arn(mut self, role_arn: impl Into<String>) -> Self {
        self.role_arn = Some(role_arn.into());
        self
    }

    /// Overrides the role session name (`RoleSessionName`).
    pub fn role_session_name(mut self, name: impl Into<String>) -> Self {
        self.role_session_name = Some(name.into());
        self
    }

    /// Overrides the path to the file containing the web-identity token.
    pub fn token_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.token_file = Some(path.into());
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

    fn resolved_role_arn(&self) -> Result<String, ValidationErr> {
        self.role_arn
            .clone()
            .or_else(|| env::var("AWS_ROLE_ARN").ok().filter(|v| !v.is_empty()))
            .ok_or_else(|| xml_error("web identity role ARN is not set (AWS_ROLE_ARN)"))
    }

    fn resolved_session_name(&self) -> String {
        self.role_session_name
            .clone()
            .or_else(|| {
                env::var("AWS_ROLE_SESSION_NAME")
                    .ok()
                    .filter(|v| !v.is_empty())
            })
            .unwrap_or_else(|| DEFAULT_SESSION_NAME.to_string())
    }

    fn read_token(&self) -> Result<String, ValidationErr> {
        let path = match &self.token_file {
            Some(path) => path.clone(),
            None => env::var("AWS_WEB_IDENTITY_TOKEN_FILE")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .ok_or_else(|| {
                    xml_error("web identity token file is not set (AWS_WEB_IDENTITY_TOKEN_FILE)")
                })?,
        };
        let token = std::fs::read_to_string(&path)
            .map_err(|e| xml_error(&format!("failed to read web identity token file: {e}")))?;
        Ok(token.trim().to_string())
    }

    /// Builds the form-urlencoded STS request body for the given token.
    fn build_request_body(&self, token: &str) -> Result<String, ValidationErr> {
        let role_arn = self.resolved_role_arn()?;
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer
            .append_pair("Action", "AssumeRoleWithWebIdentity")
            .append_pair("Version", STS_VERSION)
            .append_pair("RoleArn", &role_arn)
            .append_pair("RoleSessionName", &self.resolved_session_name())
            .append_pair("WebIdentityToken", token);
        if let Some(duration) = self.duration_seconds {
            serializer.append_pair("DurationSeconds", &duration.to_string());
        }
        if let Some(policy) = &self.policy {
            serializer.append_pair("Policy", policy);
        }
        Ok(serializer.finish())
    }

    /// Performs the STS exchange and stores the result in the cache.
    pub async fn refresh(&self) -> Result<Credentials, ValidationErr> {
        let token = self.read_token()?;
        let body = self.build_request_body(&token)?;

        let response = reqwest::Client::new()
            .post(&self.sts_endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(ValidationErr::from)?;

        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(xml_error(&format!(
                "STS AssumeRoleWithWebIdentity failed with status {status}: {text}"
            )));
        }

        let cached = parse_sts_credentials(&text, "AssumeRoleWithWebIdentityResult")?;
        let creds = cached.creds.clone();
        *self.cache.write().unwrap() = Some(cached);
        Ok(creds)
    }
}

#[async_trait]
impl Provider for WebIdentityProvider {
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
        let provider = WebIdentityProvider::new("http://localhost:9000")
            .role_arn("arn:aws:iam::123:role/test");
        let params = parse_body(&provider.build_request_body("jwt-token").unwrap());
        assert_eq!(params["Action"], "AssumeRoleWithWebIdentity");
        assert_eq!(params["Version"], "2011-06-15");
        assert_eq!(params["RoleArn"], "arn:aws:iam::123:role/test");
        assert_eq!(params["RoleSessionName"], DEFAULT_SESSION_NAME);
        assert_eq!(params["WebIdentityToken"], "jwt-token");
        assert!(!params.contains_key("DurationSeconds"));
        assert!(!params.contains_key("Policy"));
    }

    #[test]
    fn build_request_body_with_options() {
        let provider = WebIdentityProvider::new("http://localhost:9000")
            .role_arn("arn:aws:iam::123:role/test")
            .role_session_name("session-1")
            .duration_seconds(900)
            .policy("{\"Version\":\"2012-10-17\"}");
        let params = parse_body(&provider.build_request_body("jwt").unwrap());
        assert_eq!(params["RoleSessionName"], "session-1");
        assert_eq!(params["DurationSeconds"], "900");
        assert_eq!(params["Policy"], "{\"Version\":\"2012-10-17\"}");
    }

    #[test]
    fn build_request_body_without_role_arn_errors() {
        // Serialize with other env-mutating credential tests, and avoid leaking
        // AWS_ROLE_ARN from the test environment.
        let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();
        let prev = env::var("AWS_ROLE_ARN").ok();
        unsafe { env::remove_var("AWS_ROLE_ARN") };
        let provider = WebIdentityProvider::new("http://localhost:9000");
        assert!(provider.build_request_body("jwt").is_err());
        if let Some(v) = prev {
            unsafe { env::set_var("AWS_ROLE_ARN", v) };
        }
    }

    #[test]
    fn parses_web_identity_response() {
        let xml = r#"<?xml version="1.0"?>
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>AKID</AccessKeyId>
      <SecretAccessKey>SECRET</SecretAccessKey>
      <SessionToken>TOKEN</SessionToken>
      <Expiration>2030-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;
        let cached = parse_sts_credentials(xml, "AssumeRoleWithWebIdentityResult").unwrap();
        assert_eq!(cached.creds.access_key, "AKID");
        assert_eq!(cached.creds.session_token.as_deref(), Some("TOKEN"));
    }

    #[test]
    fn fetch_empty_before_priming() {
        assert!(
            WebIdentityProvider::new("http://localhost:9000")
                .fetch()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn web_identity_sts_flow() {
        use crate::s3::creds::mock_http::{self, Request, Responder};
        use std::sync::Arc;

        const STS_XML: &str = r#"<?xml version="1.0"?>
<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>WIDAK</AccessKeyId><SecretAccessKey>WIDSK</SecretAccessKey>
<SessionToken>WIDTOKEN</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#;

        let responder: Responder = Arc::new(|req: &Request| {
            if req.method == "POST" {
                (200, STS_XML.to_string())
            } else {
                (404, String::new())
            }
        });
        let server = mock_http::start(responder).await;

        let token_path =
            std::env::temp_dir().join(format!("minio-rs-webid-{}.jwt", std::process::id()));
        std::fs::write(&token_path, "jwt-token").unwrap();

        let creds = WebIdentityProvider::new(&server.base_url)
            .role_arn("arn:aws:iam::123456789012:role/test")
            .token_file(token_path.clone())
            .refresh()
            .await
            .unwrap();
        let _ = std::fs::remove_file(&token_path);

        assert_eq!(creds.access_key, "WIDAK");
        assert_eq!(creds.secret_key, "WIDSK");
        assert_eq!(creds.session_token.as_deref(), Some("WIDTOKEN"));
    }
}
