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

use crate::madmin::builders::{AddServiceAccountLDAP, AddServiceAccountLDAPBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a service account for an LDAP user.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure the service account details and send the request.
    /// The response contains the generated credentials (access key and secret key).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::service_account::AddServiceAccountReq;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use serde_json::json;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000".parse().unwrap();
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let madmin_client = MadminClient::new(base_url, Some(provider));
    ///
    ///     let policy = json!({
    ///         "Version": "2012-10-17",
    ///         "Statement": [{
    ///             "Effect": "Allow",
    ///             "Action": ["s3:GetObject"],
    ///             "Resource": ["arn:aws:s3:::mybucket/*"]
    ///         }]
    ///     });
    ///
    ///     let req = AddServiceAccountReq {
    ///         policy: Some(policy),
    ///         access_key: None,
    ///         secret_key: None,
    ///         name: Some("LDAP Service Account".to_string()),
    ///         description: Some("Read-only access".to_string()),
    ///         expiration: None,
    ///         target_user: Some("ldap-username".to_string()),
    ///     };
    ///
    ///     let response = madmin_client
    ///         .add_service_account_ldap()
    ///         .request(req)
    ///         .build()
    ///         .send()
    ///         .await
    ///         .unwrap();
    ///
    ///     println!("Access Key: {}", response.creds.access_key);
    ///     println!("Secret Key: {}", response.creds.secret_key);
    /// }
    /// ```
    pub fn add_service_account_ldap(&self) -> AddServiceAccountLDAPBldr {
        AddServiceAccountLDAP::builder().client(self.clone())
    }
}
