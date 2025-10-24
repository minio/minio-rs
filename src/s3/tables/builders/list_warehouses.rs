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

//! Builder for ListWarehouses operation

use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::tables::client::TablesClient;
use crate::s3::tables::response::ListWarehousesResponse;
use crate::s3::tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for ListWarehouses operation
///
/// Lists all warehouses (table buckets) in the Tables catalog.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3::tables::TablesClient;
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let response = tables
///     .list_warehouses()
///     .max_list(100)
///     .build()
///     .send()
///     .await?;
///
/// for warehouse in response.warehouses {
///     println!("Warehouse: {}", warehouse.name);
/// }
/// # Ok(())
/// # }\
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListWarehouses {
    #[builder(!default)]
    client: TablesClient,
    #[builder(default, setter(into, strip_option))]
    max_list: Option<i32>,
    #[builder(default, setter(into, strip_option))]
    page_token: Option<String>,
}

impl TablesApi for ListWarehouses {
    type TablesResponse = ListWarehousesResponse;
}

/// Builder type for ListWarehouses
pub type ListWarehousesBldr = ListWarehousesBuilder<((TablesClient,), (), ())>;

impl ToTablesRequest for ListWarehouses {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();

        if let Some(max) = self.max_list {
            if max <= 0 {
                return Err(ValidationErr::InvalidWarehouseName(
                    "max-list must be positive".to_string(),
                ));
            }
            query_params.add("max-list", max.to_string());
        }

        if let Some(token) = self.page_token {
            query_params.add("page-token", token);
        }

        Ok(TablesRequest {
            client: self.client,
            method: Method::GET,
            path: "/warehouses".to_string(),
            query_params,
            headers: Default::default(),
            body: None,
        })
    }
}
