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

use crate::madmin::types::FromMadminResponse;
use crate::madmin::types::MadminRequest;
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use bytes::Bytes;

/// Response for the GetAPILogs API operation.
///
/// **Note:** The MinIO server returns API logs encoded in MessagePack format.
/// This response contains the raw MessagePack bytes. To decode individual log
/// entries, you'll need to use a MessagePack decoder like `rmp-serde`.
///
/// # Example Decoding (requires rmp-serde crate)
///
/// ```ignore
/// use rmp_serde::Deserializer;
/// use serde::Deserialize;
/// use minio::madmin::types::api_logs::APILog;
///
/// let mut de = Deserializer::new(&response.data[..]);
/// while let Ok(log) = APILog::deserialize(&mut de) {
///     println!("API: {}, Status: {}", log.api, log.status_code);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct GetAPILogsResponse {
    /// Raw MessagePack-encoded log data
    pub data: Bytes,
}

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for GetAPILogsResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let data = resp.bytes().await.map_err(ValidationErr::HttpError)?;
        Ok(GetAPILogsResponse { data })
    }
}
