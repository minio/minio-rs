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

//! Request builders for S3 Inventory operations

pub mod delete_inventory_config;
pub mod generate_inventory_config;
pub mod get_inventory_config;
pub mod get_inventory_job_status;
pub mod list_inventory_configs;
pub mod put_inventory_config;

pub use delete_inventory_config::{DeleteInventoryConfig, DeleteInventoryConfigBldr};
pub use generate_inventory_config::{GenerateInventoryConfig, GenerateInventoryConfigBldr};
pub use get_inventory_config::{GetInventoryConfig, GetInventoryConfigBldr};
pub use get_inventory_job_status::{GetInventoryJobStatus, GetInventoryJobStatusBldr};
pub use list_inventory_configs::{ListInventoryConfigs, ListInventoryConfigsBldr};
pub use put_inventory_config::{PutInventoryConfig, PutInventoryConfigBldr};
