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
use crate::madmin::response::InspectResponse;
use crate::madmin::types::inspect::InspectOptions;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct Inspect {
    #[builder(!default)]
    client: MadminClient,
    #[builder(default, setter(into, doc = "Optional extra HTTP headers"))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into, doc = "Optional extra query parameters"))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(doc = "Inspect options"))]
    opts: InspectOptions,
}

pub type InspectBldr = InspectBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for Inspect {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        // Add volume and file as query parameters
        if let Some(volume) = &self.opts.volume {
            query_params.add("volume", volume);
        }
        if let Some(file) = &self.opts.file {
            query_params.add("file", file);
        }

        // Determine HTTP method based on whether public key is provided
        let method = if self.opts.public_key.is_some() {
            // If public key provided, use POST with base64-encoded key
            if let Some(key) = &self.opts.public_key {
                let encoded =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key);
                query_params.add("public-key", &encoded);
            }
            Method::POST
        } else {
            Method::GET
        };

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(method)
            .path("/inspect-data")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for Inspect {
    type MadminResponse = InspectResponse;
}
