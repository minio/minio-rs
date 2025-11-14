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

use serde::{Deserialize, Serialize};

/// Lock entry containing information about a lock held on a resource
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockEntry {
    /// When the lock was first granted
    #[serde(rename = "time")]
    pub timestamp: String,

    /// Duration for which lock has been held (in nanoseconds)
    #[serde(rename = "elapsed")]
    pub elapsed: i64,

    /// Resource contains info like bucket+object
    #[serde(rename = "resource")]
    pub resource: String,

    /// Type indicates if 'Write' or 'Read' lock
    #[serde(rename = "type")]
    pub lock_type: String,

    /// Source at which lock was granted
    #[serde(rename = "source")]
    pub source: String,

    /// List of servers participating in the lock
    #[serde(rename = "serverlist")]
    pub server_list: Vec<String>,

    /// Owner UUID indicates server owns the lock
    #[serde(rename = "owner")]
    pub owner: String,

    /// UID to uniquely identify request of client
    #[serde(rename = "id")]
    pub id: String,

    /// Quorum number of servers required to hold this lock, used to look for stale locks
    #[serde(rename = "quorum")]
    pub quorum: i32,
}

/// Options for the TopLocks API
#[derive(Debug, Clone)]
pub struct TopLockOpts {
    /// Number of locks to return
    pub count: i32,

    /// Whether to include stale locks
    pub stale: bool,
}

impl Default for TopLockOpts {
    fn default() -> Self {
        Self {
            count: 10,
            stale: false,
        }
    }
}
