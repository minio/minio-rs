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

//! Builder for ReplaceView operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/views/{view}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L950>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ReplaceViewResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName, view_path};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// Argument builder for ReplaceView operation
///
/// Replaces an existing view with a new version.
#[derive(Clone, Debug, TypedBuilder)]
pub struct ReplaceView {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    view_name: ViewName,
    #[builder(default, setter(into))]
    requirements: Vec<ViewRequirement>,
    #[builder(default, setter(into))]
    updates: Vec<ViewUpdate>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

/// View requirement for optimistic concurrency control
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ViewRequirement {
    AssertViewUuid { uuid: String },
}

/// View update operation
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ViewUpdate {
    AssignUuid {
        uuid: String,
    },
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
    AddSchema {
        schema: crate::s3tables::iceberg::Schema,
        last_column_id: Option<i32>,
    },
    AddViewVersion {
        #[serde(rename = "view-version")]
        view_version: ViewVersionUpdate,
    },
    SetCurrentViewVersion {
        #[serde(rename = "view-version-id")]
        view_version_id: i32,
    },
}

/// View version for update operations
#[derive(Clone, Debug, Serialize)]
pub struct ViewVersionUpdate {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    pub summary: HashMap<String, String>,
    #[serde(rename = "default-namespace")]
    pub default_namespace: Vec<String>,
    #[serde(rename = "default-catalog", skip_serializing_if = "Option::is_none")]
    pub default_catalog: Option<String>,
    pub representations: Vec<SqlViewRepresentation>,
}

/// SQL view representation
#[derive(Clone, Debug, Serialize)]
pub struct SqlViewRepresentation {
    pub r#type: String,
    pub sql: String,
    pub dialect: String,
}

/// Request body for ReplaceView
#[derive(Serialize)]
struct CommitViewRequest {
    identifier: ViewIdentifier,
    requirements: Vec<ViewRequirement>,
    updates: Vec<ViewUpdate>,
}

#[derive(Serialize)]
struct ViewIdentifier {
    namespace: Vec<String>,
    name: String,
}

impl TablesApi for ReplaceView {
    type TablesResponse = ReplaceViewResponse;
}

/// Builder type for ReplaceView
pub type ReplaceViewBldr = ReplaceViewBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (ViewName,),
    (),
    (),
    (),
)>;

impl ToTablesRequest for ReplaceView {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let namespace_vec = self.namespace.as_slice().to_vec();
        let view_name_str = self.view_name.as_str().to_string();

        let request_body = CommitViewRequest {
            identifier: ViewIdentifier {
                namespace: namespace_vec,
                name: view_name_str,
            },
            requirements: self.requirements,
            updates: self.updates,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(view_path(
                &self.warehouse_name,
                &self.namespace,
                &self.view_name,
            ))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
