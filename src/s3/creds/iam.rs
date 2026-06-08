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

//! IAM role credential provider (EC2 IMDSv2 and ECS/EKS container endpoints).

use crate::s3::creds::{CachedCredentials, Credentials, Provider, refresh_deadline};
use crate::s3::error::ValidationErr;
use crate::s3::utils::{from_iso8601utc, utc_now};
use async_trait::async_trait;
use chrono::Duration;
use std::env;
use std::sync::RwLock;
use std::time::Duration as StdDuration;

const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";
const ECS_LINK_LOCAL_ENDPOINT: &str = "http://169.254.170.2";
const TOKEN_TTL_SECONDS: &str = "21600";
const TOKEN_TTL_HEADER: &str = "X-aws-ec2-metadata-token-ttl-seconds";
const TOKEN_HEADER: &str = "X-aws-ec2-metadata-token";
const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const TOKEN_TIMEOUT: StdDuration = StdDuration::from_secs(1);

/// Builds an error for IAM/ECS failures. These responses are JSON (or plain
/// metadata), so a neutral string error is used rather than an XML error.
fn creds_error(message: impl Into<String>) -> ValidationErr {
    ValidationErr::StrError {
        message: message.into(),
        source: None,
    }
}

/// Credential provider that obtains temporary credentials from an EC2 instance
/// (IMDSv2) or from the ECS/EKS container credentials endpoint.
///
/// When `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
/// `AWS_CONTAINER_CREDENTIALS_FULL_URI` is set, the container endpoint is used;
/// otherwise the EC2 instance metadata service is queried using IMDSv2 (a token
/// is fetched first, then the instance role's credentials). The IMDS endpoint
/// defaults to `http://169.254.169.254` and can be overridden via
/// `AWS_EC2_METADATA_SERVICE_ENDPOINT` or [`IamRoleProvider::endpoint`].
///
/// Like other network-backed providers, [`Provider::fetch`] returns the cached
/// credentials; call [`Provider::ensure_credentials`] (awaited) to perform the
/// initial fetch and subsequent refreshes.
#[derive(Debug)]
pub struct IamRoleProvider {
    endpoint: Option<String>,
    cache: RwLock<Option<CachedCredentials>>,
}

impl Default for IamRoleProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(serde::Deserialize)]
struct IamCredentialsResponse {
    #[serde(rename = "Code")]
    code: Option<String>,
    #[serde(rename = "AccessKeyId")]
    access_key_id: Option<String>,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: Option<String>,
    #[serde(rename = "Token")]
    token: Option<String>,
    #[serde(rename = "Expiration")]
    expiration: Option<String>,
}

impl IamRoleProvider {
    /// Returns a provider using the default IMDS endpoint (or the container
    /// endpoint when the relevant environment variables are present).
    pub fn new() -> Self {
        IamRoleProvider {
            endpoint: None,
            cache: RwLock::new(None),
        }
    }

    /// Overrides the EC2 instance metadata service endpoint.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    fn imds_endpoint(&self) -> String {
        if let Some(endpoint) = &self.endpoint {
            return endpoint.trim_end_matches('/').to_string();
        }
        env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .map(|v| v.trim_end_matches('/').to_string())
            .unwrap_or_else(|| DEFAULT_IMDS_ENDPOINT.to_string())
    }

    /// Performs the credential fetch and stores the result in the cache.
    pub async fn refresh(&self) -> Result<Credentials, ValidationErr> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(ValidationErr::from)?;

        let body = match container_source() {
            // The relative URI is pinned to the link-local ECS endpoint, so the
            // authorization token is always safe to attach.
            Some(ContainerSource::Relative(url)) => {
                self.fetch_container(&client, &url, true).await?
            }
            // For an arbitrary full URI, only attach the bearer token when the
            // target is loopback/link-local or https, so it cannot be exfiltrated
            // to a remote cleartext host (mirrors minio-go's loopback guard).
            Some(ContainerSource::Full(url)) => {
                let attach_token = token_target_is_safe(&url);
                self.fetch_container(&client, &url, attach_token).await?
            }
            None => self.fetch_imds(&client).await?,
        };

        let cached = parse_iam_response(&body)?;
        let creds = cached.creds.clone();
        *self.cache.write().unwrap() = Some(cached);
        Ok(creds)
    }

    async fn fetch_container(
        &self,
        client: &reqwest::Client,
        url: &str,
        attach_token: bool,
    ) -> Result<String, ValidationErr> {
        let mut request = client.get(url);
        if attach_token && let Some(token) = container_authorization_token() {
            request = request.header("Authorization", token);
        }
        let response = request.send().await.map_err(ValidationErr::from)?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(creds_error(format!(
                "container credentials request failed with status {status}: {text}"
            )));
        }
        Ok(text)
    }

    async fn fetch_imds(&self, client: &reqwest::Client) -> Result<String, ValidationErr> {
        let base = self.imds_endpoint();

        // Fetch an IMDSv2 session token with a short timeout. If it is
        // unavailable (timeout, or a host that only speaks IMDSv1), proceed
        // without a token so the credential GETs fall back to IMDSv1, matching
        // minio-go's behavior.
        let token: Option<String> = match client
            .put(format!("{base}/latest/api/token"))
            .header(TOKEN_TTL_HEADER, TOKEN_TTL_SECONDS)
            .timeout(TOKEN_TIMEOUT)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => resp.text().await.ok(),
            _ => None,
        };

        let role = self
            .imds_get(
                client,
                format!("{base}/latest/meta-data/iam/security-credentials/"),
                &token,
            )
            .await?;
        let role = role.trim();
        if role.is_empty() {
            return Err(creds_error("no IAM role found in instance metadata"));
        }

        self.imds_get(
            client,
            format!("{base}/latest/meta-data/iam/security-credentials/{role}"),
            &token,
        )
        .await
    }

    async fn imds_get(
        &self,
        client: &reqwest::Client,
        url: String,
        token: &Option<String>,
    ) -> Result<String, ValidationErr> {
        let mut request = client.get(url);
        if let Some(token) = token {
            request = request.header(TOKEN_HEADER, token);
        }
        let body = request
            .send()
            .await
            .map_err(ValidationErr::from)?
            .error_for_status()
            .map_err(ValidationErr::from)?
            .text()
            .await?;
        Ok(body)
    }
}

enum ContainerSource {
    /// ECS relative URI, pinned to the link-local endpoint.
    Relative(String),
    /// Arbitrary full URI from `AWS_CONTAINER_CREDENTIALS_FULL_URI`.
    Full(String),
}

fn container_source() -> Option<ContainerSource> {
    if let Ok(relative) = env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        && !relative.is_empty()
    {
        return Some(ContainerSource::Relative(format!(
            "{ECS_LINK_LOCAL_ENDPOINT}{relative}"
        )));
    }
    env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI")
        .ok()
        .filter(|v| !v.is_empty())
        .map(ContainerSource::Full)
}

/// Returns true when the container authorization token may safely be sent to
/// `url`: an https endpoint, or a loopback/link-local host. This prevents the
/// bearer token from being exfiltrated to an arbitrary remote cleartext host,
/// matching minio-go's loopback guard for `AWS_CONTAINER_CREDENTIALS_FULL_URI`.
fn token_target_is_safe(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() == "https" {
        return true;
    }
    match parsed.host() {
        Some(url::Host::Domain(host)) => host == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback() || ip.is_link_local(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

fn container_authorization_token() -> Option<String> {
    if let Ok(token) = env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN")
        && !token.is_empty()
    {
        return Some(token);
    }
    let path = env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
        .ok()
        .filter(|v| !v.is_empty())?;
    std::fs::read_to_string(path)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Parses the JSON credentials document returned by the IMDS/ECS endpoints.
fn parse_iam_response(json: &str) -> Result<CachedCredentials, ValidationErr> {
    let now = utc_now();
    let parsed: IamCredentialsResponse = serde_json::from_str(json)
        .map_err(|e| creds_error(format!("invalid IAM credentials response: {e}")))?;

    if let Some(code) = &parsed.code
        && code != "Success"
    {
        return Err(creds_error(format!(
            "IAM credentials request returned code {code}"
        )));
    }

    let access_key = parsed
        .access_key_id
        .filter(|v| !v.is_empty())
        .ok_or_else(|| creds_error("missing AccessKeyId in IAM credentials response"))?;
    let secret_key = parsed
        .secret_access_key
        .filter(|v| !v.is_empty())
        .ok_or_else(|| creds_error("missing SecretAccessKey in IAM credentials response"))?;
    let session_token = parsed.token.filter(|v| !v.is_empty());

    let expiration = match parsed.expiration {
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

#[async_trait]
impl Provider for IamRoleProvider {
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

    #[test]
    fn parses_successful_response() {
        let json = r#"{
            "Code": "Success",
            "AccessKeyId": "AKIDTEST",
            "SecretAccessKey": "SECRETTEST",
            "Token": "TOKENTEST",
            "Expiration": "2030-01-01T00:00:00Z"
        }"#;
        let cached = parse_iam_response(json).unwrap();
        assert_eq!(cached.creds.access_key, "AKIDTEST");
        assert_eq!(cached.creds.secret_key, "SECRETTEST");
        assert_eq!(cached.creds.session_token.as_deref(), Some("TOKENTEST"));
        assert!(cached.refresh_after > utc_now());
    }

    #[test]
    fn parses_container_response_without_code() {
        let json = r#"{
            "AccessKeyId": "AKID",
            "SecretAccessKey": "SECRET",
            "Token": "TOKEN",
            "Expiration": "2030-01-01T00:00:00Z"
        }"#;
        let cached = parse_iam_response(json).unwrap();
        assert_eq!(cached.creds.access_key, "AKID");
    }

    #[test]
    fn rejects_non_success_code() {
        let json = r#"{"Code": "AssumeRoleUnauthorizedAccess"}"#;
        assert!(matches!(
            parse_iam_response(json),
            Err(ValidationErr::StrError { .. })
        ));
    }

    #[test]
    fn token_target_safety() {
        assert!(token_target_is_safe("https://creds.example.com/path"));
        assert!(token_target_is_safe("http://127.0.0.1:8080/creds"));
        assert!(token_target_is_safe("http://localhost/creds"));
        assert!(token_target_is_safe("http://169.254.170.2/v2/creds"));
        assert!(!token_target_is_safe("http://evil.example.com/creds"));
        assert!(!token_target_is_safe("not a url"));
    }

    #[test]
    fn rejects_missing_keys() {
        let json = r#"{"Code": "Success", "AccessKeyId": "AKID"}"#;
        assert!(parse_iam_response(json).is_err());
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_iam_response("not json").is_err());
    }

    #[test]
    fn fetch_empty_before_priming() {
        assert!(IamRoleProvider::new().fetch().is_empty());
    }

    mod network {
        use super::*;
        use crate::s3::creds::mock_http::{self, Request, Responder};
        use std::sync::Arc;
        use std::sync::LazyLock;
        use tokio::sync::Mutex;

        const CREDS_JSON: &str = r#"{
            "Code": "Success",
            "AccessKeyId": "IMDSAK",
            "SecretAccessKey": "IMDSSK",
            "Token": "IMDSTOKEN",
            "Expiration": "2030-01-01T00:00:00Z"
        }"#;

        // Serializes the network-flow tests: refresh() consults the container
        // environment variables first, so an ECS test setting them must not run
        // concurrently with an IMDS test.
        static NET_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

        fn clear_container_env() {
            for key in [
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_CONTAINER_AUTHORIZATION_TOKEN",
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            ] {
                unsafe { env::remove_var(key) };
            }
        }

        #[tokio::test]
        async fn imdsv2_full_flow_uses_token() {
            let _guard = NET_LOCK.lock().await;
            clear_container_env();

            // Role and credential endpoints require the IMDSv2 token, proving the
            // client fetched it via the token PUT and forwarded it.
            let responder: Responder = Arc::new(|req: &Request| {
                if req.method == "PUT" && req.path == "/latest/api/token" {
                    return (200, "imds-session-token".to_string());
                }
                if req.headers.get(&TOKEN_HEADER.to_ascii_lowercase())
                    != Some(&"imds-session-token".to_string())
                {
                    return (401, String::new());
                }
                match req.path.as_str() {
                    "/latest/meta-data/iam/security-credentials/" => (200, "RoleName".to_string()),
                    "/latest/meta-data/iam/security-credentials/RoleName" => {
                        (200, CREDS_JSON.to_string())
                    }
                    _ => (404, String::new()),
                }
            });
            let server = mock_http::start(responder).await;

            let creds = IamRoleProvider::new()
                .endpoint(&server.base_url)
                .refresh()
                .await
                .unwrap();
            assert_eq!(creds.access_key, "IMDSAK");
            assert_eq!(creds.secret_key, "IMDSSK");
            assert_eq!(creds.session_token.as_deref(), Some("IMDSTOKEN"));
        }

        #[tokio::test]
        async fn imdsv1_fallback_when_token_blocked() {
            let _guard = NET_LOCK.lock().await;
            clear_container_env();

            // Token endpoint is blocked (403, as with IMDSv1-only hosts); the
            // metadata endpoints serve credentials without requiring a token.
            let responder: Responder = Arc::new(|req: &Request| {
                if req.method == "PUT" && req.path == "/latest/api/token" {
                    return (403, String::new());
                }
                match req.path.as_str() {
                    "/latest/meta-data/iam/security-credentials/" => (200, "RoleName".to_string()),
                    "/latest/meta-data/iam/security-credentials/RoleName" => {
                        (200, CREDS_JSON.to_string())
                    }
                    _ => (404, String::new()),
                }
            });
            let server = mock_http::start(responder).await;

            let creds = IamRoleProvider::new()
                .endpoint(&server.base_url)
                .refresh()
                .await
                .unwrap();
            assert_eq!(creds.access_key, "IMDSAK");
        }

        #[tokio::test]
        async fn imds_no_role_errors() {
            let _guard = NET_LOCK.lock().await;
            clear_container_env();

            let responder: Responder = Arc::new(|req: &Request| {
                if req.method == "PUT" && req.path == "/latest/api/token" {
                    return (200, "tok".to_string());
                }
                // Empty role listing.
                (200, String::new())
            });
            let server = mock_http::start(responder).await;

            let result = IamRoleProvider::new()
                .endpoint(&server.base_url)
                .refresh()
                .await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn ecs_full_uri_flow() {
            let _guard = NET_LOCK.lock().await;
            clear_container_env();

            let responder: Responder = Arc::new(|req: &Request| {
                if req.path == "/v2/credentials" {
                    (200, CREDS_JSON.to_string())
                } else {
                    (404, String::new())
                }
            });
            let server = mock_http::start(responder).await;

            // Loopback host, so the authorization token (if any) is allowed.
            unsafe {
                env::set_var(
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                    format!("{}/v2/credentials", server.base_url),
                );
            }
            let creds = IamRoleProvider::new().refresh().await;
            clear_container_env();

            let creds = creds.unwrap();
            assert_eq!(creds.access_key, "IMDSAK");
            assert_eq!(creds.session_token.as_deref(), Some("IMDSTOKEN"));
        }
    }
}
