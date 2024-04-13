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

//! Argument builders for [minio::s3::client::Client](crate::s3::client::Client) APIs

mod buckets;
mod get_object;
mod list_objects;
mod listen_bucket_notification;
mod object_content;
mod put_object;
mod remove_objects;

pub use buckets::*;
pub use get_object::*;
pub use list_objects::*;
pub use listen_bucket_notification::*;
pub use object_content::*;
pub use put_object::*;
pub use remove_objects::*;
