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
use crate::madmin::response::StartBatchJobResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for starting a batch job.
///
/// This struct constructs the parameters required for the [`MadminClient::start_batch_job`](crate::madmin::madmin_client::MadminClient::start_batch_job) method.
///
/// Starts a new batch job using the provided YAML configuration.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct StartBatchJob {
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
    job_yaml: String,
}

impl ToMadminRequest for StartBatchJob {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/start-job")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(Arc::new(SegmentedBytes::from(self.job_yaml))))
            .build())
    }
}

impl MadminApi for StartBatchJob {
    type MadminResponse = StartBatchJobResponse;
}
