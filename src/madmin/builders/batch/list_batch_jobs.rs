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
use crate::madmin::response::ListBatchJobsResponse;
use crate::madmin::types::batch::ListBatchJobsFilter;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for listing batch jobs.
///
/// This struct constructs the parameters required for the [`MadminClient::list_batch_jobs`](crate::madmin::madmin_client::MadminClient::list_batch_jobs) method.
///
/// Returns a list of all batch jobs with optional filtering.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ListBatchJobs {
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
    #[builder(default)]
    filter: Option<ListBatchJobsFilter>,
}

impl ToMadminRequest for ListBatchJobs {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        if let Some(filter) = self.filter {
            if let Some(job_type) = filter.by_job_type {
                query_params.add("jobType", job_type);
            }
            if let Some(bucket) = filter.by_bucket {
                query_params.add("bucket", bucket);
            }
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/list-jobs")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for ListBatchJobs {
    type MadminResponse = ListBatchJobsResponse;
}
