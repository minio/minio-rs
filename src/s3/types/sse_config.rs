// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Server-side encryption configuration

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SseConfig {
    pub sse_algorithm: String,
    pub kms_master_key_id: Option<String>,
}

impl SseConfig {
    pub fn s3() -> SseConfig {
        SseConfig {
            sse_algorithm: String::from("AES256"),
            kms_master_key_id: None,
        }
    }

    pub fn kms(kms_master_key_id: Option<String>) -> SseConfig {
        SseConfig {
            sse_algorithm: String::from("aws:kms"),
            kms_master_key_id,
        }
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from(
            "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>",
        );
        data.push_str("<SSEAlgorithm>");
        data.push_str(&self.sse_algorithm);
        data.push_str("</SSEAlgorithm>");
        if let Some(v) = &self.kms_master_key_id {
            data.push_str("<KMSMasterKeyID>");
            data.push_str(v);
            data.push_str("</KMSMasterKeyID>");
        }

        data.push_str(
            "</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>",
        );
        data
    }
}
