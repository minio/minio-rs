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
use crate::madmin::types::inspect::{InspectData, InspectDataFormat};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;

/// Response for the Inspect API operation.
#[derive(Debug, Clone)]
pub struct InspectResponse {
    /// Parsed inspect data with format information
    pub data: InspectData,
}

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for InspectResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let bytes = resp.bytes().await.map_err(ValidationErr::HttpError)?;

        if bytes.is_empty() {
            return Err(ValidationErr::StrError {
                message: "Empty response from inspect API".to_string(),
                source: None,
            }
            .into());
        }

        // Read format byte
        let format_byte = bytes[0];
        let format = match format_byte {
            1 => InspectDataFormat::WithKey,
            2 => InspectDataFormat::DataOnly,
            _ => {
                return Err(ValidationErr::StrError {
                    message: format!("Unknown inspect data format: {}", format_byte),
                    source: None,
                }
                .into());
            }
        };

        let (encryption_key, data) = match format {
            InspectDataFormat::WithKey => {
                // Format 1: first byte is format, next 32 bytes are key, rest is data
                if bytes.len() < 33 {
                    return Err(ValidationErr::StrError {
                        message: "Insufficient data for format 1 (need at least 33 bytes)"
                            .to_string(),
                        source: None,
                    }
                    .into());
                }
                let key = bytes[1..33].to_vec();
                let data = bytes[33..].to_vec();
                (Some(key), data)
            }
            InspectDataFormat::DataOnly => {
                // Format 2: first byte is format, rest is data
                let data = bytes[1..].to_vec();
                (None, data)
            }
        };

        Ok(InspectResponse {
            data: InspectData {
                format,
                encryption_key,
                data,
            },
        })
    }
}
