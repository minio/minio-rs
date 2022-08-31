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

use crate::s3::error::Error;
use crate::s3::utils::{to_query_string, Multimap};
use derivative::Derivative;
use hyper::http::Method;
use hyper::Uri;
use std::fmt;

#[derive(Derivative)]
#[derivative(Clone, Debug, Default)]
pub struct Url {
    #[derivative(Default(value = "true"))]
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
        return self.host.clone();
    }
}

impl fmt::Display for Url {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.host.is_empty() {
            return Err(std::fmt::Error);
        }

        if self.https {
            f.write_str("https://")?;
        } else {
            f.write_str("http://")?;
        }

        if self.port > 0 {
            f.write_str(format!("{}:{}", self.host, self.port).as_str())?;
        } else {
            f.write_str(&self.host)?;
        }

        if !self.path.starts_with("/") {
            f.write_str("/")?;
        }
        f.write_str(&self.path)?;

        if !self.query.is_empty() {
            f.write_str("?")?;
            f.write_str(&to_query_string(&self.query))?;
        }

        Ok(())
    }
}

fn extract_region(host: &str) -> String {
    let tokens: Vec<&str> = host.split('.').collect();
    let region = match tokens.get(1) {
        Some(r) => match *r {
            "dualstack" => match tokens.get(2) {
                Some(t) => t,
                _ => "",
            },
            "amazonaws" => "",
            _ => r,
        },
        _ => "",
    };
    return region.to_string();
}

#[derive(Derivative)]
#[derivative(Clone, Debug, Default)]
pub struct BaseUrl {
    #[derivative(Default(value = "true"))]
    pub https: bool,
    host: String,
    port: u16,
    pub region: String,
    pub aws_host: bool,
    accelerate_host: bool,
    dualstack_host: bool,
    virtual_style: bool,
}

impl BaseUrl {
    pub fn build_url(
        &self,
        method: &Method,
        region: &String,
        query: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
    ) -> Result<Url, Error> {
        if !object_name.map_or(true, |v| v.is_empty()) {
            if bucket_name.map_or(true, |v| v.is_empty()) {
                return Err(Error::UrlBuildError(String::from(
                    "empty bucket name provided for object name",
                )));
            }
        }

        let mut url = Url::default();
        url.https = self.https;
        url.host = self.host.clone();
        url.port = self.port;
        url.query = query.clone();

        if bucket_name.is_none() {
            url.path.push_str("/");
            if self.aws_host {
                url.host = format!("s3.{}.{}", region, self.host);
            }
            return Ok(url);
        }

        let bucket = bucket_name.unwrap();

        let enforce_path_style = true &&
	// CreateBucket API requires path style in Amazon AWS S3.
	    (method == Method::PUT && object_name.is_none() && query.is_empty()) ||
	// GetBucketLocation API requires path style in Amazon AWS S3.
	    query.contains_key("location") ||
	// Use path style for bucket name containing '.' which causes
	// SSL certificate validation error.
	    (bucket.contains('.') && self.https);

        if self.aws_host {
            let mut s3_domain = "s3.".to_string();
            if self.accelerate_host {
                if bucket.contains('.') {
                    return Err(Error::UrlBuildError(String::from(
                        "bucket name with '.' is not allowed for accelerate endpoint",
                    )));
                }

                if !enforce_path_style {
                    s3_domain = "s3-accelerate.".to_string();
                }
            }

            if self.dualstack_host {
                s3_domain.push_str("dualstack.");
            }
            if enforce_path_style || !self.accelerate_host {
                s3_domain.push_str(region);
                s3_domain.push_str(".");
            }
            url.host = s3_domain + &url.host;
        }

        if enforce_path_style || !self.virtual_style {
            url.path.push_str("/");
            url.path.push_str(bucket);
        } else {
            url.host = format!("{}.{}", bucket, url.host);
        }

        if object_name.is_some() {
            if object_name.unwrap().chars().nth(0) != Some('/') {
                url.path.push_str("/");
            }
            // FIXME: urlencode path
            url.path.push_str(object_name.unwrap());
        }

        return Ok(url);
    }

    pub fn from_string(s: String) -> Result<BaseUrl, Error> {
        let url = s.parse::<Uri>()?;

        let https = match url.scheme() {
            None => true,
            Some(scheme) => match scheme.as_str() {
                "http" => false,
                "https" => true,
                _ => {
                    return Err(Error::InvalidBaseUrl(String::from(
                        "scheme must be http or https",
                    )))
                }
            },
        };

        let mut host = match url.host() {
            Some(h) => h,
            _ => {
                return Err(Error::InvalidBaseUrl(String::from(
                    "valid host must be provided",
                )))
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
            return Err(Error::InvalidBaseUrl(String::from(
                "path must be empty for base URL",
            )));
        }

        if !url.query().is_none() {
            return Err(Error::InvalidBaseUrl(String::from(
                "query must be none for base URL",
            )));
        }

        let mut accelerate_host = host.starts_with("s3-accelerate.");
        let aws_host = (host.starts_with("s3.") || accelerate_host)
            && (host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn"));
        let virtual_style = aws_host || host.ends_with("aliyuncs.com");

        let mut region = String::new();
        let mut dualstack_host = false;

        if aws_host {
            let mut aws_domain = "amazonaws.com";
            region = extract_region(host);

            let is_aws_china_host = host.ends_with(".cn");
            if is_aws_china_host {
                aws_domain = "amazonaws.com.cn";
                if region.is_empty() {
                    return Err(Error::InvalidBaseUrl(String::from(
                        "region must be provided in Amazon S3 China endpoint",
                    )));
                }
            }

            dualstack_host = host.contains(".dualstack.");
            host = aws_domain;
        } else {
            accelerate_host = false;
        }

        return Ok(BaseUrl {
            https: https,
            host: host.to_string(),
            port: port,
            region: region,
            aws_host: aws_host,
            accelerate_host: accelerate_host,
            dualstack_host: dualstack_host,
            virtual_style: virtual_style,
        });
    }
}
