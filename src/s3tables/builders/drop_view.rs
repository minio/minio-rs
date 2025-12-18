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

//! Builder for DropView operation
//!
//! Iceberg REST API: `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L980>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::DropViewResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName, view_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for DropView operation
///
/// Deletes a view from the catalog.
#[derive(Clone, Debug, TypedBuilder)]
pub struct DropView {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    view_name: ViewName,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

impl TablesApi for DropView {
    type TablesResponse = DropViewResponse;
}

/// Builder type for DropView
pub type DropViewBldr = DropViewBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (ViewName,),
    (),
)>;

impl ToTablesRequest for DropView {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
            .path(view_path(
                &self.warehouse_name,
                &self.namespace,
                &self.view_name,
            ))
            .headers(headers)
            .body(None)
            .build())
    }
}
