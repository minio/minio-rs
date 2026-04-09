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

use crate::madmin::builders::{Uncordon, UncordonBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::NodeAddress;
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Mark node as schedulable (uncordon).
    ///
    /// Allows requests to be routed to the specified node again.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let result = madmin.uncordon("node1:9000")?.send().await?;
    /// println!("Uncordoned node: {}", result.node);
    /// # Ok(())
    /// # }
    /// ```
    pub fn uncordon<N>(&self, node: N) -> Result<UncordonBldr, ValidationErr>
    where
        N: TryInto<NodeAddress>,
        N::Error: Into<ValidationErr>,
    {
        Ok(Uncordon::builder()
            .client(self.clone())
            .node(node.try_into().map_err(Into::into)?))
    }
}
