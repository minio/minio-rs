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
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L237>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::CreateNamespaceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, WarehouseName};
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
/// use minio::s3tables::{TablesClient, TablesApi, HasNamespace};
/// use minio::s3tables::utils::{Namespace, WarehouseName};
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
///     .create_namespace(
///         WarehouseName::try_from("my-warehouse")?,
///         Namespace::single("analytics")?,
///     )
///     .properties(properties)
///     .build()
///     .send()
///     .await?;
///
/// println!("Created namespace: {:?}", response.namespace()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct CreateNamespace {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(default, setter(into))]
    properties: HashMap<String, String>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
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
    CreateNamespaceBuilder<((TablesClient,), (WarehouseName,), (Namespace,), (), ())>;

impl ToTablesRequest for CreateNamespace {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let request_body = CreateNamespaceRequest {
            namespace: self.namespace.into_inner(), //TODO investigate if this needs cloning while later request_body is made into a vector
            properties: self.properties,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!("/{}/namespaces", self.warehouse_name.as_str()))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
