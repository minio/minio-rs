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
const DEFAULT_STS_ENDPOINT: &str = "https://sts.amazonaws.com";

fn env_non_empty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

/// Resolves the STS endpoint from the values of the environment variables:
/// `AWS_ENDPOINT_URL_STS` if set and not skipped by
/// `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` (case-insensitive), else the AWS
/// default for `region`. The global `AWS_ENDPOINT_URL` is deliberately not read.
/// Empty values count as unset, and the URL path is kept. The flag is `true`
/// when the endpoint came from `AWS_ENDPOINT_URL_STS`.
///
/// See <https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html>.
fn resolve_sts_endpoint(
    ignore_configured: Option<&str>,
    sts_url: Option<&str>,
    region: Option<&str>,
) -> (String, bool) {
    let ignore = ignore_configured.is_some_and(|v| v.eq_ignore_ascii_case("true"));
    if !ignore && let Some(url) = sts_url.filter(|v| !v.is_empty()) {
        return (url.to_string(), true);
    }
    let default = match region.filter(|r| !r.is_empty()) {
        Some(region) if region.starts_with("cn-") => {
            format!("https://sts.{region}.amazonaws.com.cn")
        }
        Some(region) => format!("https://sts.{region}.amazonaws.com"),
        None => DEFAULT_STS_ENDPOINT.to_string(),
    };
    (default, false)
}

/// Adds the PEM certificates in the file at `path` to the root certificates of
/// `builder`. The certificates are added to the default roots; they do not
/// replace them.
///
/// See <https://docs.aws.amazon.com/sdkref/latest/guide/feature-gen-config.html>.
#[cfg(any(
    feature = "default-tls",
    feature = "native-tls",
    feature = "rustls-tls"
))]
fn add_ca_bundle(
    mut builder: reqwest::ClientBuilder,
    path: &std::path::Path,
) -> Result<reqwest::ClientBuilder, ValidationErr> {
    let read_error = |e: std::io::Error| {
        xml_error(&format!(
            "unable to read AWS_CA_BUNDLE {}: {e}",
            path.display()
        ))
    };
    // A named pipe without a writer would block the read indefinitely.
    if !std::fs::metadata(path).map_err(read_error)?.is_file() {
        return Err(xml_error(&format!(
            "AWS_CA_BUNDLE {} is not a regular file",
            path.display()
        )));
    }
    let pem = std::fs::read(path).map_err(read_error)?;
    let certs = reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| {
        xml_error(&format!(
            "AWS_CA_BUNDLE {} is not a valid PEM bundle: {e}",
            path.display()
        ))
    })?;
    if certs.is_empty() {
        return Err(xml_error(&format!(
            "AWS_CA_BUNDLE {} contains no PEM certificates",
            path.display()
        )));
    }
    for cert in certs {
        builder = builder.add_root_certificate(cert);
    }
    Ok(builder)
}

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
///
/// [`WebIdentityProvider::new`] takes the STS endpoint explicitly;
/// [`WebIdentityProvider::from_env`] resolves it from the environment like the
/// AWS SDKs do. When the endpoint comes from `AWS_ENDPOINT_URL_STS`, the STS
/// request also trusts the PEM certificates in the file named by
/// `AWS_CA_BUNDLE`, in addition to the default root certificates.
#[derive(Debug)]
pub struct WebIdentityProvider {
    sts_endpoint: String,
    #[cfg_attr(
        not(any(
            feature = "default-tls",
            feature = "native-tls",
            feature = "rustls-tls"
        )),
        allow(dead_code)
    )]
    use_ca_bundle: bool,
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
            use_ca_bundle: false,
            role_arn: None,
            role_session_name: None,
            token_file: None,
            duration_seconds: None,
            policy: None,
            cache: RwLock::new(None),
        }
    }

    /// Returns a new provider whose STS endpoint is resolved from the
    /// environment, in this order:
    ///
    /// 1. `AWS_ENDPOINT_URL_STS`
    /// 2. the AWS default, `https://sts.<AWS_REGION>.amazonaws.com`
    ///    (`.amazonaws.com.cn` for `cn-` regions), or `https://sts.amazonaws.com`
    ///    when `AWS_REGION` is unset
    ///
    /// `AWS_ENDPOINT_URL_STS` is skipped when
    /// `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` is `true`. The global
    /// `AWS_ENDPOINT_URL` is not read. The URL path is kept, so
    /// `https://sts.example.com/sts/ns/name` is used as is. Call
    /// [`WebIdentityProvider::endpoint`] to override the result explicitly.
    ///
    /// `AWS_CA_BUNDLE` is honored only when the endpoint came from
    /// `AWS_ENDPOINT_URL_STS`.
    ///
    /// The role ARN, token file, and session name come from `AWS_ROLE_ARN`,
    /// `AWS_WEB_IDENTITY_TOKEN_FILE`, and `AWS_ROLE_SESSION_NAME` when the
    /// credentials are fetched, as with [`WebIdentityProvider::new`].
    pub fn from_env() -> Self {
        let (endpoint, from_sts_url) = resolve_sts_endpoint(
            env_non_empty("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS").as_deref(),
            env_non_empty("AWS_ENDPOINT_URL_STS").as_deref(),
            env_non_empty("AWS_REGION").as_deref(),
        );
        let mut provider = Self::new(endpoint);
        provider.use_ca_bundle = from_sts_url;
        provider
    }

    /// Overrides the STS endpoint, taking precedence over any environment
    /// variable. `AWS_CA_BUNDLE` is then not read.
    pub fn endpoint(mut self, sts_endpoint: impl Into<String>) -> Self {
        self.sts_endpoint = sts_endpoint.into();
        self.use_ca_bundle = false;
        *self.cache.get_mut().unwrap() = None;
        self
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

    /// Builds the HTTP client for the STS request. When the endpoint came from
    /// `AWS_ENDPOINT_URL_STS` and `AWS_CA_BUNDLE` is set, the client also trusts
    /// the certificates in that file, in addition to the default roots.
    fn http_client(&self) -> Result<reqwest::Client, ValidationErr> {
        let builder = crate::s3::client::http_client_builder();
        #[cfg(any(
            feature = "default-tls",
            feature = "native-tls",
            feature = "rustls-tls"
        ))]
        if self.use_ca_bundle
            && let Some(path) = env::var_os("AWS_CA_BUNDLE").filter(|v| !v.is_empty())
        {
            let path = std::path::Path::new(&path);
            return add_ca_bundle(builder, path)?.build().map_err(|e| {
                xml_error(&format!(
                    "unable to build an HTTP client that trusts AWS_CA_BUNDLE {}: {e}",
                    path.display()
                ))
            });
        }
        builder.build().map_err(ValidationErr::from)
    }

    /// Performs the STS exchange and stores the result in the cache.
    pub async fn refresh(&self) -> Result<Credentials, ValidationErr> {
        let token = self.read_token()?;
        let body = self.build_request_body(&token)?;

        let response = self
            .http_client()?
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

        let _guard = crate::s3::creds::test_support::ENV_LOCK.lock().await;
        let _env = EnvRestore::set(&[]);

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

    const ENV_VARS: [&str; 6] = [
        "AWS_EC2_METADATA_SERVICE_ENDPOINT",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
        "AWS_ENDPOINT_URL_STS",
        "AWS_ENDPOINT_URL",
        "AWS_REGION",
        "AWS_CA_BUNDLE",
    ];

    /// Sets exactly `vars` (clearing the other endpoint-related variables) and
    /// restores the previous environment on drop. The caller holds `ENV_LOCK`.
    struct EnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl EnvRestore {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            let prev = ENV_VARS.iter().map(|k| (*k, env::var_os(k))).collect();
            for key in ENV_VARS {
                unsafe { env::remove_var(key) };
            }
            for (key, value) in vars {
                unsafe { env::set_var(key, value) };
            }
            EnvRestore(prev)
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                match value {
                    Some(v) => unsafe { env::set_var(key, v) },
                    None => unsafe { env::remove_var(key) },
                }
            }
        }
    }

    const STS_URL: &str = "https://sts.example.com";

    fn resolved(ignore: Option<&str>, sts: Option<&str>, region: Option<&str>) -> String {
        resolve_sts_endpoint(ignore, sts, region).0
    }

    #[test]
    fn t4_sts_url_wins_over_region_default() {
        assert_eq!(resolved(None, Some(STS_URL), Some("eu-west-1")), STS_URL);
        assert_eq!(
            resolved(None, None, Some("eu-west-1")),
            "https://sts.eu-west-1.amazonaws.com"
        );
        assert_eq!(resolved(None, None, None), "https://sts.amazonaws.com");
    }

    #[test]
    fn t14_cn_region_default() {
        assert_eq!(
            resolved(None, None, Some("cn-north-1")),
            "https://sts.cn-north-1.amazonaws.com.cn"
        );
    }

    #[test]
    fn sts_endpoint_empty_values_count_as_unset() {
        assert_eq!(
            resolve_sts_endpoint(None, Some(""), Some("")),
            ("https://sts.amazonaws.com".to_string(), false)
        );
    }

    #[test]
    fn t6_ignore_flag_is_case_insensitive_and_only_true() {
        for flag in ["true", "TRUE", "True"] {
            assert_eq!(
                resolve_sts_endpoint(Some(flag), Some(STS_URL), Some("us-east-1")),
                ("https://sts.us-east-1.amazonaws.com".to_string(), false),
                "flag {flag}"
            );
        }
        for flag in ["false", "1", ""] {
            assert_eq!(
                resolve_sts_endpoint(Some(flag), Some(STS_URL), None),
                (STS_URL.to_string(), true),
                "flag {flag}"
            );
        }
    }

    #[test]
    fn t7_path_is_preserved() {
        let url = "https://sts.example.com/sts/ns/name";
        assert_eq!(resolved(None, Some(url), None), url);
    }

    #[test]
    fn t2_global_endpoint_url_is_ignored() {
        let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();
        let _env = EnvRestore::set(&[("AWS_ENDPOINT_URL", "https://gateway.example.com")]);
        let provider = WebIdentityProvider::from_env();
        assert_eq!(
            url::Url::parse(&provider.sts_endpoint).unwrap().host_str(),
            Some("sts.amazonaws.com")
        );
        assert!(!provider.use_ca_bundle);
    }

    #[test]
    fn from_env_resolves_endpoint_and_t5_ignore_flag_selects_default_host() {
        let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();

        let _env = EnvRestore::set(&[
            (
                "AWS_ENDPOINT_URL_STS",
                "https://sts.example.com/sts/ns/name",
            ),
            ("AWS_REGION", "eu-west-1"),
        ]);
        let provider = WebIdentityProvider::from_env();
        assert_eq!(provider.sts_endpoint, "https://sts.example.com/sts/ns/name");
        assert!(provider.use_ca_bundle);

        let _env = EnvRestore::set(&[
            ("AWS_ENDPOINT_URL_STS", "https://sts.example.com"),
            ("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true"),
            ("AWS_REGION", "eu-west-1"),
        ]);
        let provider = WebIdentityProvider::from_env();
        assert_eq!(
            url::Url::parse(&provider.sts_endpoint).unwrap().host_str(),
            Some("sts.eu-west-1.amazonaws.com")
        );
        assert!(!provider.use_ca_bundle);
    }

    #[test]
    fn endpoint_override_discards_cached_credentials() {
        let xml = r#"<?xml version="1.0"?>
<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>AKID</AccessKeyId><SecretAccessKey>SECRET</SecretAccessKey>
<SessionToken>TOKEN</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#;
        let provider = WebIdentityProvider::new("https://a.example.com");
        *provider.cache.write().unwrap() =
            Some(parse_sts_credentials(xml, "AssumeRoleWithWebIdentityResult").unwrap());
        assert!(!provider.fetch().is_empty());

        let provider = provider.endpoint("https://b.example.com");
        assert!(provider.fetch().is_empty());
    }

    #[test]
    fn t1_explicit_endpoint_wins_over_environment() {
        let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();
        let _env = EnvRestore::set(&[("AWS_ENDPOINT_URL_STS", "https://sts.example.com")]);
        let provider = WebIdentityProvider::from_env().endpoint("https://explicit.example.com");
        assert_eq!(provider.sts_endpoint, "https://explicit.example.com");
        assert!(!provider.use_ca_bundle);
    }

    #[tokio::test]
    async fn t7_from_env_exchanges_at_endpoint_path() {
        use crate::s3::creds::mock_http::{self, Request, Responder};
        use std::sync::Arc;

        const STS_XML: &str = r#"<?xml version="1.0"?>
<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>ENVAK</AccessKeyId><SecretAccessKey>ENVSK</SecretAccessKey>
<SessionToken>ENVTOKEN</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#;

        let responder: Responder = Arc::new(|req: &Request| {
            if req.method == "POST" && req.path == "/sts/ns/name" {
                (200, STS_XML.to_string())
            } else {
                (404, String::new())
            }
        });
        let server = mock_http::start(responder).await;

        let token_path =
            std::env::temp_dir().join(format!("minio-rs-webid-env-{}.jwt", std::process::id()));
        std::fs::write(&token_path, "jwt-token").unwrap();

        let _guard = crate::s3::creds::test_support::ENV_LOCK.lock().await;
        let endpoint = format!("{}/sts/ns/name", server.base_url);
        let _env = EnvRestore::set(&[("AWS_ENDPOINT_URL_STS", endpoint.as_str())]);
        let result = WebIdentityProvider::from_env()
            .role_arn("arn:aws:iam::123456789012:role/test")
            .token_file(token_path.clone())
            .refresh()
            .await;
        let _ = std::fs::remove_file(&token_path);

        assert_eq!(result.unwrap().access_key, "ENVAK");
    }

    #[cfg(any(
        feature = "default-tls",
        feature = "native-tls",
        feature = "rustls-tls"
    ))]
    mod ca_bundle {
        use super::*;

        const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBizCCATGgAwIBAgIUXxvmdBsn++PeLg5uNKVZUb0LLDkwCgYIKoZIzj0EAwIw\nGjEYMBYGA1UEAwwPc3RzLmV4YW1wbGUuY29tMCAXDTI2MTExODExMTkzNloYDzIx\nMjYxMDI1MTExOTM2WjAaMRgwFgYDVQQDDA9zdHMuZXhhbXBsZS5jb20wWTATBgcq\nhkjOPQIBBggqhkjOPQMBBwNCAASOuFqimHrpKSWnQxW11ekLusWj+CuHs2RZaME3\nYHr4kvhujZSjOuxa3yfDaqwn5NOUbo1I4I/klZxlnfVoAPKxo1MwUTAdBgNVHQ4E\nFgQUiRcmCwNopPGNa0bIY7M2R6bDdv0wHwYDVR0jBBgwFoAUiRcmCwNopPGNa0bI\nY7M2R6bDdv0wDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiA/c5oI\n2NuTBTntYz36WlnahxchSX9ywn+P9tkvNoRUkwIhAPI4QXQRsM/NF6Oyz7P1aO2n\nPZvEokqpQgyvXz9DXxCA\n-----END CERTIFICATE-----\n";

        fn temp_file(name: &str, content: &str) -> PathBuf {
            let path = std::env::temp_dir().join(format!("minio-rs-{name}-{}", std::process::id()));
            std::fs::write(&path, content).unwrap();
            path
        }

        fn builder() -> reqwest::ClientBuilder {
            crate::s3::client::http_client_builder()
        }

        const STS_XML: &str = r#"<?xml version="1.0"?>
<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>TLSAK</AccessKeyId><SecretAccessKey>TLSSK</SecretAccessKey>
<SessionToken>TLSTOKEN</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#;

        fn request_complete(buf: &[u8]) -> bool {
            let text = String::from_utf8_lossy(buf);
            let Some(end) = text.find("\r\n\r\n") else {
                return false;
            };
            let content_length = text[..end]
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            buf.len() >= end + 4 + content_length
        }

        /// Starts a one-shot HTTPS server on loopback with a certificate for
        /// `localhost` generated now. Returns its port, the certificate in PEM, and a
        /// channel that reports whether the client completed a request (`true`) or
        /// the connection ended without one (`false`).
        fn start_tls_server() -> (u16, String, std::sync::mpsc::Receiver<bool>) {
            use std::io::{Read, Write};
            use std::sync::Arc;

            let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                .expect("certificate generation");
            let cert_pem = certified.cert.pem();
            let cert_der = certified.cert.der().clone();
            let key_der =
                rustls::pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
                    .expect("private key");
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap();

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let (outcome, outcome_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let Ok((mut tcp, _)) = listener.accept() else {
                    return;
                };
                let Ok(mut conn) = rustls::ServerConnection::new(Arc::new(config)) else {
                    return;
                };
                let mut tls = rustls::Stream::new(&mut conn, &mut tcp);
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                while !request_complete(&request) {
                    match tls.read(&mut chunk) {
                        Ok(0) | Err(_) => {
                            let _ = outcome.send(false);
                            return;
                        }
                        Ok(n) => request.extend_from_slice(&chunk[..n]),
                    }
                }
                let _ = outcome.send(true);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{STS_XML}",
                    STS_XML.len()
                );
                let _ = tls.write_all(response.as_bytes());
                let _ = tls.flush();
            });
            (port, cert_pem, outcome_rx)
        }

        /// Returns a path that does not exist, inside a fresh private directory.
        fn missing_path(name: &str) -> PathBuf {
            let dir = std::env::temp_dir().join(format!("minio-rs-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            dir.join("missing.pem")
        }

        fn token_file(name: &str) -> PathBuf {
            temp_file(name, "jwt-token")
        }

        #[tokio::test]
        async fn t9_bundle_makes_server_certificate_trusted() {
            let (port, cert_pem, _outcome) = start_tls_server();
            let ca = temp_file("ca-server.pem", &cert_pem);
            let token = token_file("tls-ok.jwt");

            let _guard = crate::s3::creds::test_support::ENV_LOCK.lock().await;
            let endpoint = format!("https://localhost:{port}/sts/ns/name");
            let _env = EnvRestore::set(&[
                ("AWS_ENDPOINT_URL_STS", endpoint.as_str()),
                ("AWS_CA_BUNDLE", ca.to_str().unwrap()),
            ]);
            let result = WebIdentityProvider::from_env()
                .role_arn("arn:aws:iam::123456789012:role/test")
                .token_file(token.clone())
                .refresh()
                .await;
            let _ = std::fs::remove_file(&ca);
            let _ = std::fs::remove_file(&token);

            assert_eq!(result.unwrap().access_key, "TLSAK");
        }

        #[tokio::test]
        async fn t10_without_bundle_server_certificate_is_rejected() {
            let (port, _cert_pem, outcome) = start_tls_server();
            let token = token_file("tls-fail.jwt");

            let _guard = crate::s3::creds::test_support::ENV_LOCK.lock().await;
            let endpoint = format!("https://localhost:{port}/sts/ns/name");
            let _env = EnvRestore::set(&[("AWS_ENDPOINT_URL_STS", endpoint.as_str())]);
            let result = WebIdentityProvider::from_env()
                .role_arn("arn:aws:iam::123456789012:role/test")
                .token_file(token.clone())
                .refresh()
                .await;
            let _ = std::fs::remove_file(&token);

            assert!(result.is_err());
            // The client reached the server and dropped the handshake without
            // sending a request, so the failure is the certificate check and not,
            // for example, an unreachable proxy.
            assert_eq!(
                outcome.recv_timeout(std::time::Duration::from_secs(10)),
                Ok(false)
            );
        }

        #[test]
        fn accepts_pem_certificates() {
            let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();
            let path = temp_file("ca-valid.pem", CERT_PEM);
            let result = add_ca_bundle(builder(), &path);
            let _ = std::fs::remove_file(&path);
            assert!(result.unwrap().build().is_ok());
        }

        #[test]
        fn t12_missing_file_is_an_error() {
            let path = missing_path("ca-missing");
            let err = add_ca_bundle(builder(), &path).unwrap_err();
            assert!(err.to_string().contains("unable to read AWS_CA_BUNDLE"));
        }

        #[test]
        fn t13_bundle_without_certificates_is_an_error() {
            let path = temp_file("ca-empty.pem", "not a certificate\n");
            let result = add_ca_bundle(builder(), &path);
            let _ = std::fs::remove_file(&path);
            let err = result.unwrap_err();
            assert!(err.to_string().contains("AWS_CA_BUNDLE"));
        }

        #[test]
        fn directory_is_not_a_regular_file() {
            let err = add_ca_bundle(builder(), &std::env::temp_dir()).unwrap_err();
            let message = err.to_string();
            assert!(message.contains("AWS_CA_BUNDLE"));
            assert!(message.contains("not a regular file"));
        }

        #[test]
        fn t3_bundle_is_not_read_for_explicit_or_default_endpoints() {
            let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();
            let missing = missing_path("ca-ignored");
            let _env = EnvRestore::set(&[
                ("AWS_ENDPOINT_URL_STS", "https://sts.example.com"),
                ("AWS_CA_BUNDLE", missing.to_str().unwrap()),
            ]);
            let explicit = WebIdentityProvider::from_env().endpoint("https://explicit.example.com");
            assert!(explicit.http_client().is_ok());
            assert!(
                WebIdentityProvider::new("https://sts.example.com")
                    .http_client()
                    .is_ok()
            );

            let _env = EnvRestore::set(&[("AWS_CA_BUNDLE", missing.to_str().unwrap())]);
            assert!(WebIdentityProvider::from_env().http_client().is_ok());
        }

        #[cfg(unix)]
        #[tokio::test]
        async fn bundle_path_with_non_utf8_bytes_is_not_ignored() {
            use std::os::unix::ffi::OsStrExt;

            let _guard = crate::s3::creds::test_support::ENV_LOCK.lock().await;
            let _env = EnvRestore::set(&[("AWS_ENDPOINT_URL_STS", "https://sts.example.com")]);
            let path = std::ffi::OsStr::from_bytes(b"/nonexistent/minio-rs-\xff.pem");
            unsafe { env::set_var("AWS_CA_BUNDLE", path) };
            let provider = WebIdentityProvider::from_env();
            let err = provider.http_client().unwrap_err();
            assert!(err.to_string().contains("AWS_CA_BUNDLE"));
        }

        #[tokio::test]
        async fn refresh_fails_on_unreadable_bundle_without_sending_a_request() {
            use crate::s3::creds::mock_http::{self, Request, Responder};
            use std::sync::Arc;
            use std::sync::atomic::{AtomicUsize, Ordering};

            let requests = Arc::new(AtomicUsize::new(0));
            let counter = requests.clone();
            let responder: Responder = Arc::new(move |_: &Request| {
                counter.fetch_add(1, Ordering::SeqCst);
                (404, String::new())
            });
            let server = mock_http::start(responder).await;
            let token = token_file("refresh-bundle.jwt");
            let missing = missing_path("refresh-missing");

            let _guard = crate::s3::creds::test_support::ENV_LOCK.lock().await;
            let _env = EnvRestore::set(&[
                ("AWS_ENDPOINT_URL_STS", server.base_url.as_str()),
                ("AWS_CA_BUNDLE", missing.to_str().unwrap()),
            ]);
            let result = WebIdentityProvider::from_env()
                .role_arn("arn:aws:iam::123456789012:role/test")
                .token_file(token.clone())
                .refresh()
                .await;
            let _ = std::fs::remove_file(&token);

            assert!(result.unwrap_err().to_string().contains("AWS_CA_BUNDLE"));
            assert_eq!(requests.load(Ordering::SeqCst), 0);
        }

        #[cfg(unix)]
        #[test]
        fn env_restore_keeps_non_utf8_values() {
            use std::os::unix::ffi::OsStrExt;

            let _guard = crate::s3::creds::test_support::ENV_LOCK.blocking_lock();
            let original = std::ffi::OsStr::from_bytes(b"/nonexistent/minio-rs-\xff.pem");
            let outer = EnvRestore::set(&[]);
            unsafe { env::set_var("AWS_CA_BUNDLE", original) };
            drop(EnvRestore::set(&[]));
            let restored = env::var_os("AWS_CA_BUNDLE");
            drop(outer);
            assert_eq!(restored.as_deref(), Some(original));
        }
    }
}
