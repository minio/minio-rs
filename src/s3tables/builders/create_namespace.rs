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

//! Builder for CreateNamespace operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::CreateNamespaceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// Argument builder for CreateNamespace operation
///
/// Creates a namespace within a warehouse for organizing tables.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3::types::S3Api;
/// use std::collections::HashMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let mut properties = HashMap::new();
/// properties.insert("owner".to_string(), "analytics-team".to_string());
///
/// let response = tables
///     .create_namespace("my-warehouse", vec!["analytics".to_string()])
///     .properties(properties)
///     .build()
///     .send()
///     .await?;
///
/// println!("Created namespace: {:?}", response.parsed_namespace()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct CreateNamespace {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
    #[builder(default, setter(into))]
    properties: HashMap<String, String>,
}

/// Request body for CreateNamespace
#[derive(Serialize)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    properties: HashMap<String, String>,
}

impl TablesApi for CreateNamespace {
    type TablesResponse = CreateNamespaceResponse;
}

/// Builder type for CreateNamespace
pub type CreateNamespaceBldr =
    CreateNamespaceBuilder<((TablesClient,), (String,), (Vec<String>,), ())>;

impl ToTablesRequest for CreateNamespace {
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

        let request_body = CreateNamespaceRequest {
            namespace: self.namespace,
            properties: self.properties,
        };

        let body = serde_json::to_vec(&request_body).map_err(|e| {
            ValidationErr::InvalidNamespaceName(format!("JSON serialization failed: {e}"))
        })?;

        Ok(TablesRequest {
            client: self.client,
            method: Method::POST,
            path: format!("/{}/namespaces", self.warehouse_name),
            query_params: Default::default(),
            headers: Default::default(),
            body: Some(body),
        })
    }
}
