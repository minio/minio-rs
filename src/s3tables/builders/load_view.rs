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

//! Builder for LoadView operation
//!
//! Iceberg REST API: `GET /v1/{prefix}/namespaces/{namespace}/views/{view}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L920>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::LoadViewResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName, view_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for LoadView operation
///
/// Loads a view's metadata from the catalog.
#[derive(Clone, Debug, TypedBuilder)]
pub struct LoadView {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    view_name: ViewName,
}

impl TablesApi for LoadView {
    type TablesResponse = LoadViewResponse;
}

/// Builder type for LoadView
pub type LoadViewBldr =
    LoadViewBuilder<((TablesClient,), (WarehouseName,), (Namespace,), (ViewName,))>;

impl ToTablesRequest for LoadView {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(view_path(
                &self.warehouse_name,
                &self.namespace,
                &self.view_name,
            ))
            .body(None)
            .build())
    }
}
