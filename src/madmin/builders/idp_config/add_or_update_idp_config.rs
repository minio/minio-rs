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
use crate::madmin::response::AddOrUpdateIdpConfigResponse;
use crate::madmin::types::idp_config::IdpType;
use crate::madmin::types::typed_parameters::IdpConfigName;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct AddOrUpdateIdpConfig {
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
    #[builder(setter(into, doc = "Type of identity provider (LDAP, OpenID, etc.)"))]
    idp_type: IdpType,
    #[builder(!default, setter(into, doc = "Configuration name/identifier for the IDP"))]
    name: IdpConfigName,
    #[builder(setter(into, doc = "IDP configuration data as a string"))]
    config_data: String,
    #[builder(
        default = false,
        setter(
            doc = "Set to true to update existing configuration, false to create new (default: false)"
        )
    )]
    update: bool,
}

/// Builder type for [`AddOrUpdateIdpConfig`].
pub type AddOrUpdateIdpConfigBldr =
    AddOrUpdateIdpConfigBuilder<((MadminClient,), (), (), (), (), (), ())>;

impl ToMadminRequest for AddOrUpdateIdpConfig {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let method = if self.update {
            Method::POST
        } else {
            Method::PUT
        };

        let path = format!(
            "/idp-config/{}/{}",
            self.idp_type.as_str(),
            urlencoding::encode(self.name.as_str())
        );

        let body = Some(Arc::new(SegmentedBytes::from(Bytes::from(
            self.config_data,
        ))));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(method)
            .path(path)
            .api_version(4)
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}

impl MadminApi for AddOrUpdateIdpConfig {
    type MadminResponse = AddOrUpdateIdpConfigResponse;
}
