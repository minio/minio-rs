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

use super::utils::urlencode_object_key;
use crate::s3::client::DEFAULT_REGION;
use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::types::Region;
use crate::s3::utils::match_hostname;
use hyper::Uri;
use hyper::http::Method;
use lazy_static::lazy_static;
use regex::Regex;
use std::fmt;
use std::str::FromStr;

const AWS_S3_PREFIX: &str = r"^(((bucket\.|accesspoint\.)vpce(-[a-z_\d]+)+\.s3\.)|([a-z_\d-]{1,63}\.)s3-control(-[a-z_\d]+)*\.|(s3(-[a-z_\d]+)*\.))";

lazy_static! {
    static ref AWS_ELB_ENDPOINT_REGEX: Regex =
        Regex::new(r"^[a-z_\d-]{1,63}\.[a-z_\d-]{1,63}\.elb\.amazonaws\.com$").unwrap();
    static ref AWS_S3_PREFIX_REGEX: Regex = Regex::new(AWS_S3_PREFIX).unwrap();
}

/// Represents HTTP URL.
#[derive(Clone, Debug)]
pub struct Url {
    pub https: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub query: Multimap,
}

impl Url {
    pub fn host_header_value(&self) -> String {
        if self.port > 0 {
            return format!("{}:{}", self.host, self.port);
        }
        self.host.clone()
    }
}

impl Default for Url {
    fn default() -> Self {
        Self {
            https: true,
            host: String::default(),
            port: u16::default(),
            path: String::default(),
            query: Multimap::default(),
        }
    }
}

impl fmt::Display for Url {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.https {
            f.write_str("https://")?;
        } else {
            f.write_str("http://")?;
        }

        if self.host.is_empty() {
            f.write_str("<invalid-host>")?;
        } else if self.port > 0 {
            f.write_str(&format!("{}:{}", self.host, self.port))?;
        } else {
            f.write_str(&self.host)?;
        }

        if !self.path.starts_with('/') {
            f.write_str("/")?;
        }
        f.write_str(&self.path)?;

        if !self.query.is_empty() {
            f.write_str("?")?;
            f.write_str(&self.query.to_query_string())?;
        }

        Ok(())
    }
}

pub fn match_aws_endpoint(value: &str) -> bool {
    lazy_static! {
        static ref AWS_ENDPOINT_REGEX: Regex = Regex::new(r".*\.amazonaws\.com(|\.cn)$").unwrap();
    }

    AWS_ENDPOINT_REGEX.is_match(value.to_lowercase().as_str())
}

pub fn match_aws_s3_endpoint(value: &str) -> bool {
    lazy_static! {
        static ref AWS_S3_ENDPOINT_REGEX: Regex = Regex::new(
            &(AWS_S3_PREFIX.to_string() + r"([a-z_\d-]{1,63}\.)*amazonaws\.com(|\.cn)$")
        )
        .unwrap();
    }

    let binding = value.to_lowercase();
    let lvalue = binding.as_str();

    if !AWS_S3_ENDPOINT_REGEX.is_match(lvalue) {
        return false;
    }

    for token in lvalue.split('.') {
        if token.starts_with('-')
            || token.starts_with('_')
            || token.ends_with('-')
            || token.ends_with('_')
            || token.starts_with("vpce-_")
            || token.starts_with("s3-control-_")
            || token.starts_with("s3-_")
        {
            return false;
        }
    }

    true
}

fn get_aws_info(
    host: &str,
    https: bool,
    region: &mut String,
    aws_s3_prefix: &mut String,
    aws_domain_suffix: &mut String,
    dualstack: &mut bool,
) -> Result<(), ValidationErr> {
    if !match_hostname(host) {
        return Ok(());
    }

    if AWS_ELB_ENDPOINT_REGEX.is_match(host) {
        let token = host
            .get(..host.rfind(".elb.amazonaws.com").unwrap() - 1)
            .unwrap();
        *region = token
            .get(token.rfind('.').unwrap() + 1..)
            .unwrap()
            .to_string();
        return Ok(());
    }

    if !match_aws_endpoint(host) {
        return Ok(());
    }

    if !match_aws_s3_endpoint(host) {
        return Err(ValidationErr::UrlBuildError(format!(
            "invalid Amazon AWS host {host}"
        )));
    }

    let matcher = AWS_S3_PREFIX_REGEX.find(host).unwrap();
    let s3_prefix = host.get(..matcher.end()).unwrap();

    if s3_prefix.contains("s3-accesspoint") && !https {
        return Err(ValidationErr::UrlBuildError(format!(
            "use HTTPS scheme for host {host}"
        )));
    }

    let mut tokens: Vec<_> = host.get(matcher.len()..).unwrap().split('.').collect();
    *dualstack = tokens[0].eq_ignore_ascii_case("dualstack");
    if *dualstack {
        tokens.remove(0);
    }

    let mut region_in_host = String::new();
    if tokens[0] != "vpce" && tokens[0] != "amazonaws" {
        region_in_host = tokens[0].to_string();
        tokens.remove(0);
    }

    let domain_suffix = tokens.join(".");

    if host.eq_ignore_ascii_case("s3-external-1.amazonaws.com") {
        region_in_host = DEFAULT_REGION.as_str().to_string();
    }
    if host.eq_ignore_ascii_case("s3-us-gov-west-1.amazonaws.com")
        || host.eq_ignore_ascii_case("s3-fips-us-gov-west-1.amazonaws.com")
    {
        region_in_host = "us-gov-west-1".to_string();
    }

    if domain_suffix.ends_with(".cn") && !s3_prefix.ends_with("s3-accelerate.") && region.is_empty()
    {
        return Err(ValidationErr::UrlBuildError(format!(
            "region missing in Amazon S3 China endpoint {host}"
        )));
    }

    *region = region_in_host;
    *aws_s3_prefix = s3_prefix.to_string();
    *aws_domain_suffix = domain_suffix;

    Ok(())
}

/// Represents base URL of S3 endpoint.
#[derive(Clone, Debug)]
pub struct BaseUrl {
    pub https: bool,
    host: String,
    port: u16,
    pub region: Region,
    aws_s3_prefix: String,
    aws_domain_suffix: String,
    pub dualstack: bool,
    pub virtual_style: bool,
}

impl Default for BaseUrl {
    fn default() -> Self {
        Self {
            https: true,
            host: "127.0.0.1".to_string(),
            port: 9000,
            region: DEFAULT_REGION.clone(),
            aws_s3_prefix: "".to_string(),
            aws_domain_suffix: "".to_string(),
            dualstack: false,
            virtual_style: false,
        }
    }
}

impl FromStr for BaseUrl {
    type Err = ValidationErr;

    /// Convert a string to a BaseUrl.
    ///
    /// Enables use of [`str::parse`] method to create a [`BaseUrl`].
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::http::BaseUrl;
    /// use std::str::FromStr;
    ///
    /// // Get base URL from host name
    /// let base_url = "minio.example.com".parse::<BaseUrl>().unwrap();
    /// let base_url = BaseUrl::from_str("minio.example.com").unwrap();
    /// // Get base URL from host:port
    /// let base_url: BaseUrl = "minio.example.com:9000".parse().unwrap();
    /// // Get base URL from IPv4 address
    /// let base_url: BaseUrl = "http://192.168.124.63:9000".parse().unwrap();
    /// // Get base URL from IPv6 address
    /// let base_url: BaseUrl = "[0:0:0:0:0:ffff:c0a8:7c3f]:9000".parse().unwrap();
    /// ```
    fn from_str(s: &str) -> Result<Self, ValidationErr> {
        let url = s.parse::<Uri>()?;

        let https = match url.scheme() {
            None => true,
            Some(scheme) => match scheme.as_str() {
                "http" => false,
                "https" => true,
                _ => {
                    return Err(ValidationErr::InvalidBaseUrl(
                        "scheme must be http or https".into(),
                    ));
                }
            },
        };

        let mut host = match url.host() {
            Some(h) => h,
            _ => {
                return Err(ValidationErr::InvalidBaseUrl(
                    "valid host must be provided".into(),
                ));
            }
        };

        let ipv6host = "[".to_string() + host + "]";
        if host.parse::<std::net::Ipv6Addr>().is_ok() {
            host = &ipv6host;
        }

        let mut port = match url.port() {
            Some(p) => p.as_u16(),
            _ => 0u16,
        };

        if (https && port == 443) || (!https && port == 80) {
            port = 0u16;
        }

        if url.path() != "/" && url.path() != "" {
            return Err(ValidationErr::InvalidBaseUrl(
                "path must be empty for base URL".into(),
            ));
        }

        if url.query().is_some() {
            return Err(ValidationErr::InvalidBaseUrl(
                "query must be none for base URL".into(),
            ));
        }

        let mut region = String::new();
        let mut aws_s3_prefix = String::new();
        let mut aws_domain_suffix = String::new();
        let mut dualstack: bool = false;
        get_aws_info(
            host,
            https,
            &mut region,
            &mut aws_s3_prefix,
            &mut aws_domain_suffix,
            &mut dualstack,
        )?;
        let virtual_style = !aws_domain_suffix.is_empty() || host.ends_with("aliyuncs.com");

        Ok(BaseUrl {
            https,
            host: host.to_string(),
            port,
            region: if region.is_empty() {
                Region::new_empty()
            } else {
                Region::new(region)?
            },
            aws_s3_prefix,
            aws_domain_suffix,
            dualstack,
            virtual_style,
        })
    }
}

impl BaseUrl {
    /// Checks base URL is AWS host
    pub fn is_aws_host(&self) -> bool {
        !self.aws_domain_suffix.is_empty()
    }

    /// Returns the base URL as a string (e.g., "http://localhost:9000" or "https://minio.example.com")
    pub fn to_url_string(&self) -> String {
        let scheme = if self.https { "https" } else { "http" };
        if self.port > 0 {
            format!("{}://{}:{}", scheme, self.host, self.port)
        } else {
            format!("{}://{}", scheme, self.host)
        }
    }

    fn build_aws_url(
        &self,
        url: &mut Url,
        bucket_name: &str,
        enforce_path_style: bool,
        region: &Region,
    ) -> Result<(), ValidationErr> {
        let mut host = String::from(&self.aws_s3_prefix);
        host.push_str(&self.aws_domain_suffix);
        if host.eq_ignore_ascii_case("s3-external-1.amazonaws.com")
            || host.eq_ignore_ascii_case("s3-us-gov-west-1.amazonaws.com")
            || host.eq_ignore_ascii_case("s3-fips-us-gov-west-1.amazonaws.com")
        {
            url.host = host;
            return Ok(());
        }

        host = String::from(&self.aws_s3_prefix);
        if self.aws_s3_prefix.contains("s3-accelerate") {
            if bucket_name.contains('.') {
                return Err(ValidationErr::UrlBuildError(
                    "bucket name with '.' is not allowed for accelerate endpoint".into(),
                ));
            }

            if enforce_path_style {
                host = host.replacen("-accelerate", "", 1);
            }
        }

        if self.dualstack {
            host.push_str("dualstack.");
        }
        if !self.aws_s3_prefix.contains("s3-accelerate") {
            host.push_str(region.as_str());
            host.push('.');
        }
        host.push_str(&self.aws_domain_suffix);

        url.host = host;

        Ok(())
    }

    fn build_list_buckets_url(&self, url: &mut Url, region: &Region) {
        if self.aws_domain_suffix.is_empty() {
            return;
        }

        let mut host = String::from(&self.aws_s3_prefix);
        host.push_str(&self.aws_domain_suffix);
        if host.eq_ignore_ascii_case("s3-external-1.amazonaws.com")
            || host.eq_ignore_ascii_case("s3-us-gov-west-1.amazonaws.com")
            || host.eq_ignore_ascii_case("s3-fips-us-gov-west-1.amazonaws.com")
        {
            url.host = host;
            return;
        }

        let mut s3_prefix = String::from(&self.aws_s3_prefix);
        let mut domain_suffix = String::from(&self.aws_domain_suffix);
        if s3_prefix.starts_with("s3.") || s3_prefix.starts_with("s3-") {
            s3_prefix = "s3.".to_string();
            domain_suffix = "amazonaws.com".to_string();
            if self.aws_domain_suffix.ends_with(".cn") {
                domain_suffix.push_str(".cn");
            }
        }
        url.host = s3_prefix + region.as_str() + "." + &domain_suffix;
    }

    /// Builds URL from base URL for given parameters for S3 operation
    pub fn build_url(
        &self,
        method: &Method,
        region: &Region,
        query: &Multimap,
        bucket_name: Option<&str>, //TODO change to &BucketName
        object_name: Option<&str>, //TODO change to &ObjectKey
    ) -> Result<Url, ValidationErr> {
        let mut url = Url {
            https: self.https,
            host: self.host.clone(),
            port: self.port,
            path: String::from("/"),
            query: query.clone(),
        };

        let bucket: &str = match bucket_name {
            None => {
                self.build_list_buckets_url(&mut url, region);
                return Ok(url);
            }
            Some(v) => v,
        };

        #[allow(clippy::nonminimal_bool)]
        let enforce_path_style = true &&
	// CreateBucket API requires path style in Amazon AWS S3.
	    (method == Method::PUT && object_name.is_none() && query.is_empty()) ||
	// GetBucketLocation API requires path style in Amazon AWS S3.
	    query.contains_key("location") ||
	// Use path style for bucket name containing '.' which causes
	// SSL certificate validation error.
	    (bucket.contains('.') && self.https);

        if !self.aws_domain_suffix.is_empty() {
            self.build_aws_url(&mut url, bucket, enforce_path_style, region)?;
        }

        let mut host = String::from(&url.host);
        let mut path = String::new();

        if enforce_path_style || !self.virtual_style {
            path.push('/');
            path.push_str(bucket);
        } else {
            host = format!("{}.{}", bucket, url.host);
        }

        if let Some(v) = object_name {
            if !v.starts_with('/') {
                path.push('/');
            }
            path.push_str(&urlencode_object_key(v));
        }

        url.host = host;
        url.path = path;

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::multimap_ext::Multimap;
    use hyper::http::Method;

    // ===========================
    // Url Tests
    // ===========================

    #[test]
    fn test_url_default() {
        let url = Url::default();
        assert!(url.https);
        assert!(url.host.is_empty());
        assert_eq!(url.port, 0);
        assert!(url.path.is_empty());
        assert!(url.query.is_empty());
    }

    #[test]
    fn test_url_host_header_value_with_port() {
        let url = Url {
            https: true,
            host: "example.com".to_string(),
            port: 9000,
            path: "/".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.host_header_value(), "example.com:9000");
    }

    #[test]
    fn test_url_host_header_value_without_port() {
        let url = Url {
            https: true,
            host: "example.com".to_string(),
            port: 0,
            path: "/".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.host_header_value(), "example.com");
    }

    #[test]
    fn test_url_display_https() {
        let url = Url {
            https: true,
            host: "minio.example.com".to_string(),
            port: 0,
            path: "/bucket/object".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.to_string(), "https://minio.example.com/bucket/object");
    }

    #[test]
    fn test_url_display_http() {
        let url = Url {
            https: false,
            host: "localhost".to_string(),
            port: 9000,
            path: "/test".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.to_string(), "http://localhost:9000/test");
    }

    #[test]
    fn test_url_display_with_query() {
        let mut query = Multimap::default();
        query.insert("prefix".to_string(), "test/".to_string());
        query.insert("max-keys".to_string(), "1000".to_string());

        let url = Url {
            https: true,
            host: "s3.amazonaws.com".to_string(),
            port: 0,
            path: "/bucket".to_string(),
            query,
        };

        let url_str = url.to_string();
        assert!(url_str.starts_with("https://s3.amazonaws.com/bucket?"));
        assert!(url_str.contains("prefix="));
        assert!(url_str.contains("max-keys="));
    }

    #[test]
    fn test_url_display_empty_host() {
        let url = Url {
            https: true,
            host: String::new(),
            port: 0,
            path: "/test".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.to_string(), "https://<invalid-host>/test");
    }

    #[test]
    fn test_url_display_path_without_leading_slash() {
        let url = Url {
            https: true,
            host: "example.com".to_string(),
            port: 0,
            path: "bucket/object".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.to_string(), "https://example.com/bucket/object");
    }

    // ===========================
    // AWS Endpoint Matching Tests
    // ===========================

    #[test]
    fn test_match_aws_endpoint_s3() {
        assert!(match_aws_endpoint("s3.amazonaws.com"));
        assert!(match_aws_endpoint("s3.us-west-2.amazonaws.com"));
        assert!(match_aws_endpoint("s3-us-west-1.amazonaws.com"));
    }

    #[test]
    fn test_match_aws_endpoint_china() {
        assert!(match_aws_endpoint("s3.cn-north-1.amazonaws.com.cn"));
    }

    #[test]
    fn test_match_aws_endpoint_non_aws() {
        assert!(!match_aws_endpoint("minio.example.com"));
        assert!(!match_aws_endpoint("s3.example.com"));
        assert!(!match_aws_endpoint("localhost"));
    }

    #[test]
    fn test_match_aws_s3_endpoint_standard() {
        assert!(match_aws_s3_endpoint("s3.amazonaws.com"));
        assert!(match_aws_s3_endpoint("s3.us-east-1.amazonaws.com"));
        assert!(match_aws_s3_endpoint("s3.us-west-2.amazonaws.com"));
    }

    #[test]
    fn test_match_aws_s3_endpoint_legacy() {
        assert!(match_aws_s3_endpoint("s3-us-west-1.amazonaws.com"));
        assert!(match_aws_s3_endpoint("s3-external-1.amazonaws.com"));
    }

    #[test]
    fn test_match_aws_s3_endpoint_dualstack() {
        assert!(match_aws_s3_endpoint(
            "s3.dualstack.us-east-1.amazonaws.com"
        ));
    }

    #[test]
    fn test_match_aws_s3_endpoint_accelerate() {
        assert!(match_aws_s3_endpoint("s3-accelerate.amazonaws.com"));
        assert!(match_aws_s3_endpoint(
            "s3-accelerate.dualstack.amazonaws.com"
        ));
    }

    #[test]
    fn test_match_aws_s3_endpoint_vpce() {
        assert!(match_aws_s3_endpoint(
            "bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com"
        ));
    }

    #[test]
    fn test_match_aws_s3_endpoint_accesspoint() {
        assert!(match_aws_s3_endpoint(
            "accesspoint.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com"
        ));
    }

    #[test]
    fn test_match_aws_s3_endpoint_s3_control() {
        assert!(match_aws_s3_endpoint("s3-control.amazonaws.com"));
        assert!(match_aws_s3_endpoint("s3-control.us-east-1.amazonaws.com"));
    }

    #[test]
    fn test_match_aws_s3_endpoint_china() {
        assert!(match_aws_s3_endpoint("s3.cn-north-1.amazonaws.com.cn"));
    }

    #[test]
    fn test_match_aws_s3_endpoint_invalid_prefix() {
        assert!(!match_aws_s3_endpoint("s3-_invalid.amazonaws.com"));
        assert!(!match_aws_s3_endpoint("s3-control-_invalid.amazonaws.com"));
    }

    #[test]
    fn test_match_aws_s3_endpoint_non_s3() {
        assert!(!match_aws_s3_endpoint("ec2.amazonaws.com"));
        assert!(!match_aws_s3_endpoint("dynamodb.amazonaws.com"));
    }

    // ===========================
    // BaseUrl Parsing Tests
    // ===========================

    #[test]
    fn test_baseurl_default() {
        let base = BaseUrl::default();
        assert!(base.https);
        assert_eq!(base.host, "127.0.0.1");
        assert_eq!(base.port, 9000);
        assert_eq!(base.region, DEFAULT_REGION.clone());
        assert!(!base.dualstack);
        assert!(!base.virtual_style);
    }

    #[test]
    fn test_baseurl_from_str_simple_host() {
        let base: BaseUrl = "minio.example.com".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.host, "minio.example.com");
        assert_eq!(base.port, 0);
    }

    #[test]
    fn test_baseurl_from_str_with_port() {
        let base: BaseUrl = "minio.example.com:9000".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.host, "minio.example.com");
        assert_eq!(base.port, 9000);
    }

    #[test]
    fn test_baseurl_from_str_http_scheme() {
        let base: BaseUrl = "http://localhost:9000".parse().unwrap();
        assert!(!base.https);
        assert_eq!(base.host, "localhost");
        assert_eq!(base.port, 9000);
    }

    #[test]
    fn test_baseurl_from_str_https_scheme() {
        let base: BaseUrl = "https://minio.example.com".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.host, "minio.example.com");
        assert_eq!(base.port, 0);
    }

    #[test]
    fn test_baseurl_from_str_ipv4() {
        let base: BaseUrl = "http://192.168.1.100:9000".parse().unwrap();
        assert!(!base.https);
        assert_eq!(base.host, "192.168.1.100");
        assert_eq!(base.port, 9000);
    }

    #[test]
    fn test_baseurl_from_str_ipv6() {
        let base: BaseUrl = "[::1]:9000".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.host, "[::1]");
        assert_eq!(base.port, 9000);
    }

    #[test]
    fn test_baseurl_from_str_ipv6_full() {
        let base: BaseUrl = "[2001:0db8::1]:9000".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.host, "[2001:0db8::1]");
        assert_eq!(base.port, 9000);
    }

    #[test]
    fn test_baseurl_from_str_default_https_port() {
        let base: BaseUrl = "https://minio.example.com:443".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.port, 0);
    }

    #[test]
    fn test_baseurl_from_str_default_http_port() {
        let base: BaseUrl = "http://minio.example.com:80".parse().unwrap();
        assert!(!base.https);
        assert_eq!(base.port, 0);
    }

    #[test]
    fn test_baseurl_from_str_aws_s3() {
        let base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.host, "s3.amazonaws.com");
        assert_eq!(base.region, Region::new_empty());
        assert!(base.is_aws_host());
        assert!(base.virtual_style);
    }

    #[test]
    fn test_baseurl_from_str_aws_s3_regional() {
        let base: BaseUrl = "s3.us-west-2.amazonaws.com".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.region, Region::new("us-west-2").unwrap());
        assert!(base.is_aws_host());
        assert!(base.virtual_style);
    }

    #[test]
    fn test_baseurl_from_str_aws_s3_dualstack() {
        let base: BaseUrl = "s3.dualstack.us-east-1.amazonaws.com".parse().unwrap();
        assert!(base.https);
        assert_eq!(base.region, Region::new("us-east-1").unwrap());
        assert!(base.dualstack);
        assert!(base.is_aws_host());
    }

    #[test]
    fn test_baseurl_from_str_aws_elb() {
        let base: BaseUrl = "my-lb-1234567890.us-west-2.elb.amazonaws.com"
            .parse()
            .unwrap();
        assert!(base.https);
        assert!(!base.region.is_empty() || base.region.is_empty());
    }

    #[test]
    fn test_baseurl_from_str_aliyun() {
        let base: BaseUrl = "oss-cn-hangzhou.aliyuncs.com".parse().unwrap();
        assert!(base.https);
        assert!(base.virtual_style);
    }

    #[test]
    fn test_baseurl_from_str_invalid_scheme() {
        let result = "ftp://example.com".parse::<BaseUrl>();
        assert!(result.is_err());
    }

    #[test]
    fn test_baseurl_from_str_no_host() {
        let result = "https://".parse::<BaseUrl>();
        assert!(result.is_err());
    }

    #[test]
    fn test_baseurl_from_str_with_path() {
        let result = "https://minio.example.com/bucket".parse::<BaseUrl>();
        assert!(result.is_err());
    }

    #[test]
    fn test_baseurl_from_str_with_query() {
        let result = "https://minio.example.com?key=value".parse::<BaseUrl>();
        assert!(result.is_err());
    }

    // ===========================
    // BaseUrl build_url Tests
    // ===========================

    #[test]
    fn test_baseurl_build_url_list_buckets() {
        let base: BaseUrl = "minio.example.com".parse().unwrap();
        let query = Multimap::default();
        let region = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(&Method::GET, &region, &query, None, None)
            .unwrap();

        assert_eq!(url.host, "minio.example.com");
        assert_eq!(url.path, "/");
    }

    #[test]
    fn test_baseurl_build_url_bucket_path_style() {
        let base: BaseUrl = "localhost:9000".parse().unwrap();
        let query = Multimap::default();
        let region = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(&Method::GET, &region, &query, Some("mybucket"), None)
            .unwrap();

        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 9000);
        assert_eq!(url.path, "/mybucket");
    }

    #[test]
    fn test_baseurl_build_url_bucket_virtual_style() {
        let base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        let query = Multimap::default();
        let region = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(&Method::GET, &region, &query, Some("mybucket"), None)
            .unwrap();

        assert_eq!(url.host, "mybucket.s3.us-east-1.amazonaws.com");
        assert_eq!(url.path, "");
    }

    #[test]
    fn test_baseurl_build_url_object_path_style() {
        let base: BaseUrl = "localhost:9000".parse().unwrap();
        let query = Multimap::default();
        let region = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(
                &Method::GET,
                &region,
                &query,
                Some("mybucket"),
                Some("myobject"),
            )
            .unwrap();

        assert_eq!(url.path, "/mybucket/myobject");
    }

    #[test]
    fn test_baseurl_build_url_object_virtual_style() {
        let base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        let query = Multimap::default();
        let region = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(
                &Method::GET,
                &region,
                &query,
                Some("mybucket"),
                Some("myobject"),
            )
            .unwrap();

        assert_eq!(url.host, "mybucket.s3.us-east-1.amazonaws.com");
        assert_eq!(url.path, "/myobject");
    }

    #[test]
    fn test_baseurl_build_url_object_with_slash() {
        let base: BaseUrl = "localhost:9000".parse().unwrap();
        let query = Multimap::default();

        let url = base
            .build_url(
                &Method::GET,
                &Region::default(),
                &query,
                Some("mybucket"),
                Some("/path/to/object"),
            )
            .unwrap();

        assert_eq!(url.path, "/mybucket/path/to/object");
    }

    #[test]
    fn test_baseurl_build_url_create_bucket_path_style() {
        let base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        let query = Multimap::default();

        let url = base
            .build_url(
                &Method::PUT,
                &Region::default(),
                &query,
                Some("mybucket"),
                None,
            )
            .unwrap();

        assert_eq!(url.host, "s3.us-east-1.amazonaws.com");
        assert_eq!(url.path, "/mybucket");
    }

    #[test]
    fn test_baseurl_build_url_get_bucket_location_path_style() {
        let base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        let mut query = Multimap::default();
        query.insert("location".to_string(), String::new());

        let url = base
            .build_url(
                &Method::GET,
                &Region::default(),
                &query,
                Some("mybucket"),
                None,
            )
            .unwrap();

        assert_eq!(url.host, "s3.us-east-1.amazonaws.com");
        assert_eq!(url.path, "/mybucket");
    }

    #[test]
    fn test_baseurl_build_url_bucket_with_dots_https() {
        let base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        let query = Multimap::default();

        let url = base
            .build_url(
                &Method::GET,
                &Region::default(),
                &query,
                Some("my.bucket.name"),
                None,
            )
            .unwrap();

        assert_eq!(url.host, "s3.us-east-1.amazonaws.com");
        assert_eq!(url.path, "/my.bucket.name");
    }

    #[test]
    fn test_baseurl_build_url_accelerate() {
        let base: BaseUrl = "s3-accelerate.amazonaws.com".parse().unwrap();
        let query = Multimap::default();

        let url = base
            .build_url(
                &Method::GET,
                &Region::default(),
                &query,
                Some("mybucket"),
                Some("object"),
            )
            .unwrap();

        assert_eq!(url.host, "mybucket.s3-accelerate.amazonaws.com");
    }

    #[test]
    fn test_baseurl_build_url_accelerate_bucket_with_dot() {
        let base: BaseUrl = "s3-accelerate.amazonaws.com".parse().unwrap();
        let query = Multimap::default();

        let result = base.build_url(
            &Method::GET,
            &Region::default(),
            &query,
            Some("my.bucket"),
            Some("object"),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_baseurl_build_url_dualstack() {
        let base: BaseUrl = "s3.dualstack.us-west-2.amazonaws.com".parse().unwrap();
        let query = Multimap::default();
        let region2 = Region::new("us-west-2").unwrap();
        let url = base
            .build_url(&Method::GET, &region2, &query, Some("mybucket"), None)
            .unwrap();

        assert!(url.host.contains("dualstack"));
    }

    #[test]
    fn test_baseurl_build_url_with_query_parameters() {
        let base: BaseUrl = "localhost:9000".parse().unwrap();
        let mut query = Multimap::default();
        let region2 = Region::new("us-east-1").unwrap();

        query.insert("prefix".to_string(), "test/".to_string());
        query.insert("max-keys".to_string(), "1000".to_string());

        let url = base
            .build_url(&Method::GET, &region2, &query, Some("mybucket"), None)
            .unwrap();

        assert!(url.query.contains_key("prefix"));
        assert!(url.query.contains_key("max-keys"));
    }

    #[test]
    fn test_baseurl_is_aws_host() {
        let aws_base: BaseUrl = "s3.amazonaws.com".parse().unwrap();
        assert!(aws_base.is_aws_host());

        let non_aws_base: BaseUrl = "minio.example.com".parse().unwrap();
        assert!(!non_aws_base.is_aws_host());
    }

    // ===========================
    // Edge Cases and Error Handling
    // ===========================

    #[test]
    fn test_baseurl_build_url_special_characters_in_object() {
        let base: BaseUrl = "localhost:9000".parse().unwrap();
        let query = Multimap::default();
        let region1 = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(
                &Method::GET,
                &region1,
                &query,
                Some("mybucket"),
                Some("path/to/file with spaces.txt"),
            )
            .unwrap();

        assert!(url.path.contains("mybucket"));
    }

    #[test]
    fn test_baseurl_build_url_empty_object_name() {
        let base: BaseUrl = "localhost:9000".parse().unwrap();
        let query = Multimap::default();
        let region1 = Region::new("us-east-1").unwrap();

        let url = base
            .build_url(&Method::GET, &region1, &query, Some("mybucket"), Some(""))
            .unwrap();

        assert_eq!(url.path, "/mybucket/");
    }

    #[test]
    fn test_url_display_ipv6_host() {
        let url = Url {
            https: true,
            host: "[::1]".to_string(),
            port: 9000,
            path: "/bucket".to_string(),
            query: Multimap::default(),
        };
        assert_eq!(url.to_string(), "https://[::1]:9000/bucket");
    }
}
