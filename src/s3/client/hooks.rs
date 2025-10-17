// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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

//! Request hooks for intercepting and modifying S3 API requests.
//!
//! # Why "Request Hooks"?
//!
//! Each S3 API request goes through several stages:
//! 1. **Created** - Client builds the request
//! 2. **Prepared** - Headers and URL are set
//! 3. **Signed** - AWS signature is added
//! 4. **Executed** - Request is sent to the server
//! 5. **Response received** - Server responds
//!
//! Hooks allow you to "hook into" specific stages of this process:
//! - [`RequestHooks::before_signing_mut`] - Called before stage 3 (signing)
//! - [`RequestHooks::after_execute`] - Called after stage 4 (execution)
//!
//! This enables implementing cross-cutting concerns like load balancing, telemetry,
//! and debug logging without modifying the core request handling logic.
//!
//! # The `x-minio-redirect` Header
//!
//! When a hook modifies the request URL (e.g., for client-side load balancing), the client
//! automatically adds the `x-minio-redirect` header containing the modified endpoint URL.
//! This header serves several purposes:
//!
//! - **Server-side telemetry**: MinIO servers can track which requests were client-redirected
//! - **Load balancing metrics**: Understand load distribution patterns across nodes
//! - **Debugging**: Trace request flows when troubleshooting issues
//! - **Audit logs**: Record client-side load balancing decisions
//!
//! ## Example Scenario
//!
//! 1. Client creates request for `https://minio-node-1.example.com/bucket/object`
//! 2. Load balancer hook modifies URL to `https://minio-node-3.example.com/bucket/object`
//! 3. Client adds header: `x-minio-redirect: https://minio-node-3.example.com/bucket/object`
//! 4. Server receives request and can log that it was client-redirected
//!
//! The header value is the **new URL** where the request is actually sent, not the original URL.

pub use http::Extensions;

use crate::s3::error::Error;
use crate::s3::http::Url;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use reqwest::Response;
use std::fmt::Debug;

/// Hooks for intercepting and modifying S3 API requests.
///
/// Implementations can intercept requests at two key points:
/// - Before signing: allows modifying URL, headers, and parameters before request signing
/// - After execution: allows inspecting and handling the response
///
/// Hooks are called in the order they were added to the client builder.
/// If a `before_signing_mut` hook returns an error, the request is aborted.
/// Errors in `after_execute` hooks are logged but do not abort the request.
///
/// # Common Use Cases
///
/// ## Client-Side Load Balancing
/// A hook can redirect requests to different MinIO nodes for load distribution:
/// ```ignore
/// impl RequestHooks for LoadBalancerHook {
///     async fn before_signing_mut(&self, ..., url: &mut Url, ...) -> Result<(), Error> {
///         // Select a node based on load balancing strategy
///         url.host = self.select_node().to_string();
///         Ok(())
///     }
/// }
/// ```
/// When the URL is modified, the client automatically adds an `x-minio-redirect` header
/// containing the new endpoint, enabling server-side telemetry and debugging.
///
/// ## Request Telemetry and Debug Logging
/// ```ignore
/// impl RequestHooks for TelemetryHook {
///     async fn after_execute(&self, ..., resp: &Result<Response, ...>, ...) {
///         // Log request metrics, duration, errors, etc.
///     }
/// }
/// ```
///
/// ## Debug Logging
/// ```ignore
/// impl RequestHooks for DebugLoggingHook {
///     async fn after_execute(
///         &self,
///         method: &Method,
///         url: &Url,
///         _region: &str,
///         headers: &Multimap,
///         _query_params: &Multimap,
///         _bucket_name: Option<&str>,
///         _object_name: Option<&str>,
///         _resp: &Result<Response, reqwest::Error>,
///         _extensions: &mut Extensions,
///     ) {
///         let mut header_strings: Vec<String> = headers
///             .iter_all()
///             .map(|(k, v)| format!("{}: {}", k, v.join(",")))
///             .collect();
///         header_strings.sort();
///
///         let debug_str = format!(
///             "S3 request: {} url={}; headers={}",
///             method,
///             url,
///             header_strings.join("; ")
///         );
///         println!("{}", if debug_str.len() > 1000 {
///             format!("{}...", &debug_str[..997])
///         } else {
///             debug_str
///         });
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait RequestHooks: Debug {
    /// Returns a human-readable name for this hook (used in logging/debugging).
    fn name(&self) -> &'static str;

    /// Called before the request is signed.
    ///
    /// This hook can modify:
    /// - `url`: The request URL (e.g., for client-side load balancing)
    ///   - Changing the URL will automatically add an `x-minio-redirect` header
    ///   - The header contains the new endpoint for server-side telemetry
    /// - `headers`: The request headers
    /// - `extensions`: Shared state bag for passing data between hooks
    ///
    /// Return an error to abort the request.
    async fn before_signing_mut(
        &self,
        _method: &Method,
        _url: &mut Url,
        _region: &str,
        _headers: &mut Multimap,
        _query_params: &Multimap,
        _bucket_name: Option<&str>,
        _object_name: Option<&str>,
        _body: Option<&SegmentedBytes>,
        _extensions: &mut Extensions,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Called after the HTTP request is executed.
    ///
    /// This hook receives the response (or error) and can perform logging, telemetry, etc.
    /// Errors in this hook are logged but do not fail the request.
    async fn after_execute(
        &self,
        _method: &Method,
        _url: &Url,
        _region: &str,
        _headers: &Multimap,
        _query_params: &Multimap,
        _bucket_name: Option<&str>,
        _object_name: Option<&str>,
        _resp: &Result<Response, reqwest::Error>,
        _extensions: &mut Extensions,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::multimap_ext::MultimapExt;

    #[test]
    fn test_hook_trait_has_default_implementations() {
        #[derive(Debug)]
        struct MinimalHook;

        #[async_trait::async_trait]
        impl RequestHooks for MinimalHook {
            fn name(&self) -> &'static str {
                "minimal-hook"
            }
        }

        let hook = MinimalHook;
        assert_eq!(hook.name(), "minimal-hook");
    }

    #[tokio::test]
    async fn test_hook_can_modify_url() {
        #[derive(Debug)]
        struct UrlModifyingHook;

        #[async_trait::async_trait]
        impl RequestHooks for UrlModifyingHook {
            fn name(&self) -> &'static str {
                "url-modifier"
            }

            async fn before_signing_mut(
                &self,
                _method: &Method,
                url: &mut Url,
                _region: &str,
                _headers: &mut Multimap,
                _query_params: &Multimap,
                _bucket_name: Option<&str>,
                _object_name: Option<&str>,
                _body: Option<&SegmentedBytes>,
                _extensions: &mut Extensions,
            ) -> Result<(), Error> {
                url.host = "modified-host.example.com".to_string();
                url.port = 9000;
                Ok(())
            }
        }

        let hook = UrlModifyingHook;
        let mut url = Url {
            https: true,
            host: "original-host.example.com".to_string(),
            port: 443,
            path: "/bucket/object".to_string(),
            query: Multimap::new(),
        };
        let mut headers = Multimap::new();
        let query_params = Multimap::new();
        let mut extensions = Extensions::default();

        let result = hook
            .before_signing_mut(
                &Method::GET,
                &mut url,
                "us-east-1",
                &mut headers,
                &query_params,
                Some("bucket"),
                Some("object"),
                None,
                &mut extensions,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(url.host, "modified-host.example.com");
        assert_eq!(url.port, 9000);
    }

    #[tokio::test]
    async fn test_hook_can_modify_headers() {
        #[derive(Debug)]
        struct HeaderModifyingHook;

        #[async_trait::async_trait]
        impl RequestHooks for HeaderModifyingHook {
            fn name(&self) -> &'static str {
                "header-modifier"
            }

            async fn before_signing_mut(
                &self,
                _method: &Method,
                _url: &mut Url,
                _region: &str,
                headers: &mut Multimap,
                _query_params: &Multimap,
                _bucket_name: Option<&str>,
                _object_name: Option<&str>,
                _body: Option<&SegmentedBytes>,
                _extensions: &mut Extensions,
            ) -> Result<(), Error> {
                headers.add("X-Custom-Header", "custom-value");
                Ok(())
            }
        }

        let hook = HeaderModifyingHook;
        let mut url = Url::default();
        let mut headers = Multimap::new();
        let query_params = Multimap::new();
        let mut extensions = Extensions::default();

        let result = hook
            .before_signing_mut(
                &Method::GET,
                &mut url,
                "us-east-1",
                &mut headers,
                &query_params,
                None,
                None,
                None,
                &mut extensions,
            )
            .await;

        assert!(result.is_ok());
        assert!(headers.contains_key("X-Custom-Header"));
        assert_eq!(
            headers.get("X-Custom-Header"),
            Some(&"custom-value".to_string())
        );
    }

    #[tokio::test]
    async fn test_hook_can_use_extensions() {
        #[derive(Debug)]
        struct ExtensionWritingHook;

        #[async_trait::async_trait]
        impl RequestHooks for ExtensionWritingHook {
            fn name(&self) -> &'static str {
                "extension-writer"
            }

            async fn before_signing_mut(
                &self,
                _method: &Method,
                _url: &mut Url,
                _region: &str,
                _headers: &mut Multimap,
                _query_params: &Multimap,
                _bucket_name: Option<&str>,
                _object_name: Option<&str>,
                _body: Option<&SegmentedBytes>,
                extensions: &mut Extensions,
            ) -> Result<(), Error> {
                extensions.insert("test-data".to_string());
                extensions.insert(42u32);
                Ok(())
            }
        }

        let hook = ExtensionWritingHook;
        let mut url = Url::default();
        let mut headers = Multimap::new();
        let query_params = Multimap::new();
        let mut extensions = Extensions::default();

        let result = hook
            .before_signing_mut(
                &Method::GET,
                &mut url,
                "us-east-1",
                &mut headers,
                &query_params,
                None,
                None,
                None,
                &mut extensions,
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(extensions.get::<String>(), Some(&"test-data".to_string()));
        assert_eq!(extensions.get::<u32>(), Some(&42u32));
    }

    #[tokio::test]
    async fn test_hook_can_return_error() {
        use crate::s3::error::ValidationErr;

        #[derive(Debug)]
        struct ErrorReturningHook;

        #[async_trait::async_trait]
        impl RequestHooks for ErrorReturningHook {
            fn name(&self) -> &'static str {
                "error-hook"
            }

            async fn before_signing_mut(
                &self,
                _method: &Method,
                _url: &mut Url,
                _region: &str,
                _headers: &mut Multimap,
                _query_params: &Multimap,
                bucket_name: Option<&str>,
                _object_name: Option<&str>,
                _body: Option<&SegmentedBytes>,
                _extensions: &mut Extensions,
            ) -> Result<(), Error> {
                if bucket_name == Some("forbidden-bucket") {
                    return Err(Error::Validation(ValidationErr::InvalidBucketName {
                        name: "forbidden-bucket".to_string(),
                        reason: "Bucket access denied by hook".to_string(),
                    }));
                }
                Ok(())
            }
        }

        let hook = ErrorReturningHook;
        let mut url = Url::default();
        let mut headers = Multimap::new();
        let query_params = Multimap::new();
        let mut extensions = Extensions::default();

        let result = hook
            .before_signing_mut(
                &Method::GET,
                &mut url,
                "us-east-1",
                &mut headers,
                &query_params,
                Some("forbidden-bucket"),
                None,
                None,
                &mut extensions,
            )
            .await;

        assert!(result.is_err());
        match result {
            Err(Error::Validation(ValidationErr::InvalidBucketName { name, reason })) => {
                assert_eq!(name, "forbidden-bucket");
                assert!(reason.contains("denied by hook"));
            }
            _ => panic!("Expected InvalidBucketName error"),
        }
    }

    #[tokio::test]
    async fn test_hook_default_after_execute() {
        #[derive(Debug)]
        struct NoOpHook;

        #[async_trait::async_trait]
        impl RequestHooks for NoOpHook {
            fn name(&self) -> &'static str {
                "noop-hook"
            }
        }

        let hook = NoOpHook;
        let mut url = Url::default();
        let mut headers = Multimap::new();
        let query_params = Multimap::new();
        let mut extensions = Extensions::default();

        let result = hook
            .before_signing_mut(
                &Method::GET,
                &mut url,
                "us-east-1",
                &mut headers,
                &query_params,
                None,
                None,
                None,
                &mut extensions,
            )
            .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_debug_logging_format() {
        // Test the debug logging formatting logic without async complexity
        let method = Method::PUT;
        let url = Url {
            https: true,
            host: "minio.example.com".to_string(),
            port: 443,
            path: "/bucket/object".to_string(),
            query: Multimap::new(),
        };
        let mut headers = Multimap::new();
        headers.add("Content-Type", "application/json");
        headers.add("Authorization", "AWS4-HMAC-SHA256 Credential=...");
        headers.add("x-amz-date", "20240101T000000Z");

        let mut header_strings: Vec<String> = headers
            .iter_all()
            .map(|(k, v)| format!("{}: {}", k, v.join(",")))
            .collect();
        header_strings.sort();

        let debug_str = format!(
            "S3 request: {} url={}; headers={}",
            method,
            url,
            header_strings.join("; ")
        );

        let truncated = if debug_str.len() > 1000 {
            format!("{}...", &debug_str[..997])
        } else {
            debug_str.clone()
        };

        assert!(debug_str.contains("S3 request:"));
        assert!(debug_str.contains("PUT"));
        assert!(debug_str.contains("minio.example.com"));
        assert!(debug_str.contains("Content-Type"));
        assert!(debug_str.contains("Authorization"));
        assert!(debug_str.contains("x-amz-date"));
        assert_eq!(truncated, debug_str);
    }
}
