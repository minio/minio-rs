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

//! Builder for RenameView operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/views/rename`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L1020>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::RenameViewResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, ViewName, WarehouseName};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for RenameView operation
///
/// Renames or moves a view to a different namespace.
#[derive(Clone, Debug, TypedBuilder)]
pub struct RenameView {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    source_namespace: Namespace,
    #[builder(!default)]
    source_view_name: ViewName,
    #[builder(!default)]
    dest_namespace: Namespace,
    #[builder(!default)]
    dest_view_name: ViewName,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

/// Request body for RenameView
#[derive(Serialize)]
struct RenameViewRequest {
    source: ViewRef,
    destination: ViewRef,
}

#[derive(Serialize)]
struct ViewRef {
    namespace: Vec<String>,
    name: String,
}

impl TablesApi for RenameView {
    type TablesResponse = RenameViewResponse;
}

/// Builder type for RenameView
pub type RenameViewBldr = RenameViewBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (ViewName,),
    (Namespace,),
    (ViewName,),
    (),
)>;

impl ToTablesRequest for RenameView {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let request_body = RenameViewRequest {
            source: ViewRef {
                namespace: self.source_namespace.into_inner(),
                name: self.source_view_name.into_inner(),
            },
            destination: ViewRef {
                namespace: self.dest_namespace.into_inner(),
                name: self.dest_view_name.into_inner(),
            },
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!("/{}/views/rename", self.warehouse_name.as_str()))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
