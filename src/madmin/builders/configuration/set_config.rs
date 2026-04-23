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

use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::SetConfigResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

const MAX_CONFIG_SIZE: usize = 256 * 1024; // 256 KiB

/// Argument builder for the Set Config admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::set_config`](crate::madmin::madmin_client::MadminClient::set_config) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SetConfig {
    #[builder(!default)]
    client: MadminClient,
    #[builder(
        default,
        setter(into, doc = "Optional extra HTTP headers to include in the request")
    )]
    extra_headers: Option<Multimap>,
    #[builder(
        default,
        setter(
            into,
            doc = "Optional extra query parameters to include in the request"
        )
    )]
    extra_query_params: Option<Multimap>,
    #[builder(setter(into, doc = "Config bytes"))]
    config_bytes: Vec<u8>,
}

/// Builder type for [`SetConfig`].
pub type SetConfigBldr = SetConfigBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for SetConfig {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        if self.config_bytes.len() > MAX_CONFIG_SIZE {
            return Err(Error::Validation(ValidationErr::StrError {
                message: format!(
                    "Configuration size {} exceeds maximum allowed size of {} bytes",
                    self.config_bytes.len(),
                    MAX_CONFIG_SIZE
                ),
                source: None,
            }));
        }

        let password = self
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for SetConfig".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let encrypted_data = crate::madmin::encrypt::encrypt_data(&password, &self.config_bytes)?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/config")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for SetConfig {
    type MadminResponse = SetConfigResponse;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_config_size_validation() {
        let oversized_config = vec![0u8; MAX_CONFIG_SIZE + 1];
        assert!(oversized_config.len() > MAX_CONFIG_SIZE);
    }

    #[test]
    fn test_valid_config_size() {
        let valid_config = vec![0u8; MAX_CONFIG_SIZE];
        assert_eq!(valid_config.len(), MAX_CONFIG_SIZE);
    }
}
