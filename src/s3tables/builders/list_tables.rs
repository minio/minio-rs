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

//! Builder for ListTables operation

use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ListTablesResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for ListTables operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListTables {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
    #[builder(default, setter(into, strip_option))]
    max_list: Option<i32>,
    #[builder(default, setter(into, strip_option))]
    page_token: Option<String>,
}

impl TablesApi for ListTables {
    type TablesResponse = ListTablesResponse;
}

/// Builder type for ListTables
pub type ListTablesBldr = ListTablesBuilder<((TablesClient,), (String,), (Vec<String>,), (), ())>;

impl ToTablesRequest for ListTables {
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

        let namespace_path = self.namespace.join("\u{001F}");

        let mut query_params = Multimap::new();

        if let Some(max) = self.max_list {
            if max <= 0 {
                return Err(ValidationErr::InvalidTableName(
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
            path: format!(
                "/{}/namespaces/{}/tables",
                self.warehouse_name, namespace_path
            ),
            query_params,
            headers: Default::default(),
            body: None,
        })
    }
}
