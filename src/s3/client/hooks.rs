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
//! Hooks allow you to intercept requests at two key points:
//! - [`RequestHooks::before_signing_mut`] - Modify URL, headers, and parameters before signing
//! - [`RequestHooks::after_execute`] - Inspect responses and track metrics
//!
//! Common use cases: load balancing, telemetry, debug logging, request routing.
//!
//! When a hook modifies the URL, the client adds `x-minio-redirect-from` and `x-minio-redirect-to` headers for server-side tracking.

pub use http::Extensions;

use crate::s3::error::Error;
use crate::s3::http::Url;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use reqwest::Response;
use std::fmt::Debug;

/// Trait for intercepting and modifying S3 API requests.
///
/// Hooks are called in order and can abort requests by returning errors from `before_signing_mut`.
///
/// # Examples
///
/// ## Load Balancing
/// Redirect requests across multiple MinIO nodes:
/// ```no_run
/// use minio::s3::client::RequestHooks;
/// use minio::s3::http::Url;
/// use minio::s3::error::Error;
/// use minio::s3::multimap_ext::Multimap;
/// use minio::s3::segmented_bytes::SegmentedBytes;
/// use http::{Method, Extensions};
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// #[derive(Debug, Clone)]
/// struct LoadBalancerHook {
///     nodes: Vec<String>,
///     counter: Arc<AtomicUsize>,
/// }
///
/// impl LoadBalancerHook {
///     fn new(nodes: Vec<String>) -> Self {
///         Self {
///             nodes,
///             counter: Arc::new(AtomicUsize::new(0)),
///         }
///     }
///
///     fn select_node(&self) -> &str {
///         // Round-robin load balancing
///         let index = self.counter.fetch_add(1, Ordering::SeqCst) % self.nodes.len();
///         &self.nodes[index]
///     }
/// }
///
/// #[async_trait::async_trait]
/// impl RequestHooks for LoadBalancerHook {
///     fn name(&self) -> &'static str {
///         "load-balancer"
///     }
///
///     async fn before_signing_mut(
///         &self,
///         _method: &Method,
///         url: &mut Url,
///         _region: &str,
///         _headers: &mut Multimap,
///         _query_params: &Multimap,
///         _bucket_name: Option<&str>,
///         _object_name: Option<&str>,
///         _body: Option<&SegmentedBytes>,
///         _extensions: &mut Extensions,
///     ) -> Result<(), Error> {
///         // Select a node based on load balancing strategy
///         url.host = self.select_node().to_string();
///         // Note: The client will automatically add x-minio-redirect-from
///         // and x-minio-redirect-to headers when URL is modified
///         Ok(())
///     }
/// }
///
/// # fn main() {}
/// ```
///
/// ## Telemetry & Debug Logging
/// Track timing and log request details:
/// ```no_run
/// use minio::s3::client::RequestHooks;
/// use minio::s3::error::Error;
/// use minio::s3::http::Url;
/// use minio::s3::multimap_ext::Multimap;
/// use minio::s3::segmented_bytes::SegmentedBytes;
/// use http::{Method, Extensions};
/// use reqwest::Response;
/// use std::time::Instant;
///
/// #[derive(Debug)]
/// struct LoggingHook;
///
/// #[async_trait::async_trait]
/// impl RequestHooks for LoggingHook {
///     fn name(&self) -> &'static str { "logger" }
///
///     async fn before_signing_mut(
///         &self,
///         method: &Method,
///         url: &mut Url,
///         _region: &str,
///         _headers: &mut Multimap,
///         _query_params: &Multimap,
///         _bucket_name: Option<&str>,
///         _object_name: Option<&str>,
///         _body: Option<&SegmentedBytes>,
///         extensions: &mut Extensions,
///     ) -> Result<(), Error> {
///         println!("[REQ] {} {}", method, url);
///         extensions.insert(Instant::now());
///         Ok(())
///     }
///
///     async fn after_execute(
///         &self,
///         _method: &Method,
///         _url: &Url,
///         _region: &str,
///         _headers: &Multimap,
///         _query_params: &Multimap,
///         _bucket_name: Option<&str>,
///         _object_name: Option<&str>,
///         resp: &Result<Response, reqwest::Error>,
///         extensions: &mut Extensions,
///     ) {
///         let duration = extensions.get::<Instant>()
///             .map(|start| start.elapsed())
///             .unwrap_or_default();
///
///         match resp {
///             Ok(r) => println!("[RESP] {} in {:?}", r.status(), duration),
///             Err(e) => println!("[ERR] {} in {:?}", e, duration),
///         }
///     }
/// }
///
/// # fn main() {}
/// ```
#[async_trait::async_trait]
pub trait RequestHooks: Debug {
    /// Hook name for logging.
    fn name(&self) -> &'static str;

    /// Called before signing. Modify URL/headers here. Return error to abort.
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

    /// Called after execution. For logging/telemetry. Errors don't fail the request.
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
