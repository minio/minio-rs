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
use crate::madmin::response::GenerateBatchJobV2Response;
use crate::madmin::types::batch::GenerateBatchJobOpts;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for generating a batch job template from server.
///
/// This struct constructs the parameters required for the [`MadminClient::generate_batch_job_v2`](crate::madmin::madmin_client::MadminClient::generate_batch_job_v2) method.
///
/// Requests a YAML template from the MinIO server.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct GenerateBatchJobV2 {
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
    #[builder(!default)]
    opts: GenerateBatchJobOpts,
}

impl ToMadminRequest for GenerateBatchJobV2 {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        let job_type_str = match self.opts.job_type {
            crate::madmin::types::batch::BatchJobType::Replicate => "replicate",
            crate::madmin::types::batch::BatchJobType::KeyRotate => "keyrotate",
            crate::madmin::types::batch::BatchJobType::Expire => "expire",
            crate::madmin::types::batch::BatchJobType::Catalog => "catalog",
        };
        query_params.add("jobType", job_type_str.to_string());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/generate-job")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for GenerateBatchJobV2 {
    type MadminResponse = GenerateBatchJobV2Response;
}
