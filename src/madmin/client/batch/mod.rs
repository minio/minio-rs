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

use crate::madmin::builders::batch::*;
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::{
    BatchJobStatusResponse, CancelBatchJobResponse, DescribeBatchJobResponse,
    GenerateBatchJobResponse, GenerateBatchJobV2Response, GetSupportedBatchJobTypesResponse,
    ListBatchJobsResponse, StartBatchJobResponse,
};
use crate::madmin::types::batch::{GenerateBatchJobOpts, ListBatchJobsFilter};
use crate::madmin::types::{JobId, MadminApi};
use crate::s3::error::{Error, ValidationErr};

impl MadminClient {
    /// Starts a new batch job with the provided YAML configuration
    pub async fn start_batch_job(
        &self,
        job_yaml: impl Into<String>,
    ) -> Result<StartBatchJobResponse, Error> {
        let args = StartBatchJob::builder()
            .client(self.clone())
            .job_yaml(job_yaml.into())
            .build();
        args.send().await
    }

    /// Gets the status of a batch job
    pub async fn batch_job_status<J>(&self, job_id: J) -> Result<BatchJobStatusResponse, Error>
    where
        J: TryInto<JobId>,
        J::Error: Into<ValidationErr>,
    {
        let args = BatchJobStatus::builder()
            .client(self.clone())
            .job_id(job_id.try_into().map_err(Into::into)?)
            .build();
        args.send().await
    }

    /// Describes a currently running batch job, returning its YAML configuration
    pub async fn describe_batch_job<J>(&self, job_id: J) -> Result<DescribeBatchJobResponse, Error>
    where
        J: TryInto<JobId>,
        J::Error: Into<ValidationErr>,
    {
        let args = DescribeBatchJob::builder()
            .client(self.clone())
            .job_id(job_id.try_into().map_err(Into::into)?)
            .build();
        args.send().await
    }

    /// Generates a batch job template (local, no server call)
    pub fn generate_batch_job(&self, opts: GenerateBatchJobOpts) -> GenerateBatchJobResponse {
        GenerateBatchJobResponse::from_opts(&opts)
    }

    /// Gets the list of server-supported batch job types
    pub async fn get_supported_batch_job_types(
        &self,
    ) -> Result<GetSupportedBatchJobTypesResponse, Error> {
        let args = GetSupportedBatchJobTypes::builder()
            .client(self.clone())
            .build();
        args.send().await
    }

    /// Generates a batch job template from the server (v2 API)
    pub async fn generate_batch_job_v2(
        &self,
        opts: GenerateBatchJobOpts,
    ) -> Result<GenerateBatchJobV2Response, Error> {
        let args = GenerateBatchJobV2::builder()
            .client(self.clone())
            .opts(opts)
            .build();
        args.send().await
    }

    /// Lists all batch jobs with optional filtering
    pub async fn list_batch_jobs(
        &self,
        filter: Option<ListBatchJobsFilter>,
    ) -> Result<ListBatchJobsResponse, Error> {
        let args = ListBatchJobs::builder()
            .client(self.clone())
            .filter(filter)
            .build();
        args.send().await
    }

    /// Cancels an ongoing batch job
    pub async fn cancel_batch_job<J>(&self, job_id: J) -> Result<CancelBatchJobResponse, Error>
    where
        J: TryInto<JobId>,
        J::Error: Into<ValidationErr>,
    {
        let args = CancelBatchJob::builder()
            .client(self.clone())
            .job_id(job_id.try_into().map_err(Into::into)?)
            .build();
        args.send().await
    }
}
