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

//! Response types for inventory operations.

mod delete_inventory_config;
mod generate_inventory_config;
mod get_inventory_config;
mod get_inventory_job_status;
mod list_inventory_configs;
mod put_inventory_config;

pub use delete_inventory_config::DeleteInventoryConfigResponse;
pub use generate_inventory_config::GenerateInventoryConfigResponse;
pub use get_inventory_config::{GetInventoryConfigJson, GetInventoryConfigResponse};
pub use get_inventory_job_status::GetInventoryJobStatusResponse;
pub use list_inventory_configs::{ListInventoryConfigsJson, ListInventoryConfigsResponse};
pub use put_inventory_config::PutInventoryConfigResponse;
