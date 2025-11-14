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

//! Builder for GetNamespace operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetNamespaceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for GetNamespace operation
///
/// Retrieves metadata and properties for a specific namespace.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let response = tables
///     .get_namespace("my-warehouse", vec!["analytics".to_string()])
///     .build()
///     .send()
///     .await?;
///
/// println!("Namespace: {:?}", response.parsed_namespace()?);
/// println!("Properties: {:?}", response.properties()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetNamespace {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
}

impl TablesApi for GetNamespace {
    type TablesResponse = GetNamespaceResponse;
}

/// Builder type for GetNamespace
pub type GetNamespaceBldr = GetNamespaceBuilder<((TablesClient,), (String,), (Vec<String>,))>;

impl ToTablesRequest for GetNamespace {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        if self.namespace.is_empty() {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace cannot be empty".to_string(),
            ));
        }

        for level in &self.namespace {
            if level.is_empty() {
                return Err(ValidationErr::InvalidNamespaceName(
                    "namespace levels cannot be empty".to_string(),
                ));
            }
        }

        let namespace_path = self.namespace.join("\u{001F}");

        Ok(TablesRequest {
            client: self.client,
            method: Method::GET,
            path: format!("/{}/namespaces/{}", self.warehouse_name, namespace_path),
            query_params: Default::default(),
            headers: Default::default(),
            body: None,
        })
    }
}
