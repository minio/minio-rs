// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Builder for GetWarehouseMetrics operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetWarehouseMetricsResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
pub struct GetWarehouseMetrics {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
}

impl TablesApi for GetWarehouseMetrics {
    type TablesResponse = GetWarehouseMetricsResponse;
}

pub type GetWarehouseMetricsBldr = GetWarehouseMetricsBuilder<((TablesClient,), (WarehouseName,))>;

impl ToTablesRequest for GetWarehouseMetrics {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!("/warehouses/{}/metrics", self.warehouse))
            .build())
    }
}
