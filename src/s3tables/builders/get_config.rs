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

//! Builder for GetConfig operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetConfigResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for GetConfig operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetConfig {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
}

impl TablesApi for GetConfig {
    type TablesResponse = GetConfigResponse;
}

/// Builder type for GetConfig
pub type GetConfigBldr = GetConfigBuilder<((TablesClient,), (String,))>;

impl ToTablesRequest for GetConfig {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        let mut query_params = crate::s3::multimap_ext::Multimap::new();
        query_params.insert("warehouse".to_string(), self.warehouse_name);

        Ok(TablesRequest {
            client: self.client,
            method: Method::GET,
            path: "/config".to_string(),
            query_params,
            headers: Default::default(),
            body: None,
        })
    }
}
