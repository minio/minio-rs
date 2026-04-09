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
use crate::madmin::response::SetLogConfigResponse;
use crate::madmin::types::log_config::LogConfig;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SetLogConfig {
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
    #[builder(setter(into, doc = "Configuration data"))]
    config: LogConfig,
}

/// Builder type for [`SetLogConfig`].
pub type SetLogConfigBldr = SetLogConfigBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for SetLogConfig {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let config_json = serde_json::to_vec(&self.config)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        let password = self
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for SetLogConfig".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let encrypted_data = crate::madmin::encrypt::encrypt_data(&password, &config_json)?;

        let body = Some(Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data))));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/log-config")
            .api_version(3)
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}

impl MadminApi for SetLogConfig {
    type MadminResponse = SetLogConfigResponse;
}
