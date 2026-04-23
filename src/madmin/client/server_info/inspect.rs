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

use crate::madmin::builders::{Inspect, InspectBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a builder for inspecting server internal state.
    ///
    /// **Note:** This API returns binary data with a custom protocol.
    /// The response includes a format byte indicating the data structure:
    /// - Format 1: 32-byte encryption key + data
    /// - Format 2: Data only
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::types::inspect::InspectOptions;
    /// # async fn example(client: minio::madmin::madmin_client::MadminClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let opts = InspectOptions {
    ///     volume: Some("data1".to_string()),
    ///     file: Some("xl.meta".to_string()),
    ///     public_key: None,
    /// };
    ///
    /// let response = client
    ///     .inspect()
    ///     .opts(opts)
    ///     .send()
    ///     .await?;
    ///
    /// match response.data.format {
    ///     minio::madmin::types::inspect::InspectDataFormat::WithKey => {
    ///         println!("Key: {:?}", response.data.encryption_key);
    ///     }
    ///     minio::madmin::types::inspect::InspectDataFormat::DataOnly => {
    ///         println!("No key returned");
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn inspect(&self) -> InspectBldr {
        Inspect::builder().client(self.clone())
    }
}
