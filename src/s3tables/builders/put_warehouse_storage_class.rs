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

//! Builder for PutWarehouseStorageClass operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutWarehouseStorageClassResponse;
use crate::s3tables::types::{StorageClass, TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
pub struct PutWarehouseStorageClass {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    storage_class: StorageClass,
}

#[derive(Serialize)]
struct PutWarehouseStorageClassRequest {
    #[serde(rename = "storageClass")]
    storage_class: StorageClass,
}

impl TablesApi for PutWarehouseStorageClass {
    type TablesResponse = PutWarehouseStorageClassResponse;
}

pub type PutWarehouseStorageClassBldr =
    PutWarehouseStorageClassBuilder<((TablesClient,), (WarehouseName,), (StorageClass,))>;

impl ToTablesRequest for PutWarehouseStorageClass {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let body = PutWarehouseStorageClassRequest {
            storage_class: self.storage_class,
        };
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(format!("/warehouses/{}/storageclass", self.warehouse))
            .body(Some(serde_json::to_vec(&body)?))
            .build())
    }
}
