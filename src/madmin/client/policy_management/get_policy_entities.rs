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

use crate::madmin::builders::{GetPolicyEntities, GetPolicyEntitiesBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get entities (users and groups) associated with policies
    ///
    /// Returns policy entity associations based on the provided query criteria.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::{MadminApi, policy::PolicyEntitiesQuery};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url: BaseUrl = "http://localhost:9000".parse()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let madmin_client = MadminClient::new(base_url, Some(provider));
    ///
    ///     // Query policy entities for a specific policy
    ///     let query = PolicyEntitiesQuery {
    ///         users: vec![],
    ///         groups: vec![],
    ///         policy: vec!["readwrite".to_string()],
    ///     };
    ///
    ///     let response = madmin_client
    ///         .get_policy_entities()
    ///         .query(query)
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Policy entities: {:?}", response.entities());
    ///     Ok(())
    /// }
    /// ```
    pub fn get_policy_entities(&self) -> GetPolicyEntitiesBldr {
        GetPolicyEntities::builder().client(self.clone())
    }
}
