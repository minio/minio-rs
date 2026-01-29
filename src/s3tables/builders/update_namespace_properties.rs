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

//! Builder for UpdateNamespaceProperties operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/properties`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L382>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::UpdateNamespacePropertiesResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, WarehouseName, encode_namespace};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// TypedBuilder handles required fields, then converts to our custom builder
#[derive(TypedBuilder)]
#[builder(build_method(into = UpdateNamespacePropertiesBldr))]
pub struct UpdateNamespacePropertiesRequired {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
}

/// Builder for UpdateNamespaceProperties operation
///
/// Sets or removes properties on a namespace.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
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
/// let mut updates = HashMap::new();
/// updates.insert("owner".to_string(), "analytics-team".to_string());
///
/// let response = tables
///     .update_namespace_properties(
///         WarehouseName::try_from("my-warehouse")?,
///         Namespace::single("my-namespace")?,
///     )?
///     .updates(updates)
///     .removals(vec!["old-property".to_string()])
///     .build()?
///     .send()
///     .await?;
///
/// println!("Updated: {:?}", response.updated()?);
/// println!("Removed: {:?}", response.removed()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct UpdateNamespacePropertiesBldr {
    client: TablesClient,
    warehouse: WarehouseName,
    namespace: Namespace,
    removals: Vec<String>,
    updates: HashMap<String, String>,
    idempotency_key: Option<String>,
}

impl From<UpdateNamespacePropertiesRequired> for UpdateNamespacePropertiesBldr {
    fn from(req: UpdateNamespacePropertiesRequired) -> Self {
        Self {
            client: req.client,
            warehouse: req.warehouse,
            namespace: req.namespace,
            removals: Vec::new(),
            updates: HashMap::new(),
            idempotency_key: None,
        }
    }
}

impl UpdateNamespacePropertiesBldr {
    /// Properties to remove from the namespace
    pub fn removals(mut self, removals: impl Into<Vec<String>>) -> Self {
        self.removals = removals.into();
        self
    }

    /// Properties to add or update on the namespace
    pub fn updates(mut self, updates: impl Into<HashMap<String, String>>) -> Self {
        self.updates = updates.into();
        self
    }

    /// Idempotency key for safe request retries (UUID format)
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Builds the request, validating that at least one of `updates` or `removals` is provided
    pub fn build(self) -> Result<UpdateNamespaceProperties, ValidationErr> {
        if self.removals.is_empty() && self.updates.is_empty() {
            return Err(ValidationErr::InvalidNamespaceName(
                "at least one of removals or updates must be provided".to_string(),
            ));
        }
        Ok(UpdateNamespaceProperties {
            client: self.client,
            warehouse: self.warehouse,
            namespace: self.namespace,
            removals: self.removals,
            updates: self.updates,
            idempotency_key: self.idempotency_key,
        })
    }
}

/// Validated UpdateNamespaceProperties request
#[derive(Clone, Debug)]
pub struct UpdateNamespaceProperties {
    client: TablesClient,
    warehouse: WarehouseName,
    namespace: Namespace,
    removals: Vec<String>,
    updates: HashMap<String, String>,
    idempotency_key: Option<String>,
}

/// Request body for UpdateNamespaceProperties
#[derive(Serialize)]
struct UpdateNamespacePropertiesRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    removals: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    updates: HashMap<String, String>,
}

impl TablesApi for UpdateNamespaceProperties {
    type TablesResponse = UpdateNamespacePropertiesResponse;
}

impl ToTablesRequest for UpdateNamespaceProperties {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        // Validation already done in UpdateNamespacePropertiesBldr::build()
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let request_body = UpdateNamespacePropertiesRequest {
            removals: self.removals,
            updates: self.updates,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!(
                "/{}/namespaces/{}/properties",
                self.warehouse,
                encode_namespace(&self.namespace)
            ))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
