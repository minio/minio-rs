// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Quote fields types

use std::fmt;

#[derive(Clone, Debug)]
pub enum QuoteFields {
    ALWAYS,
    ASNEEDED,
}

impl fmt::Display for QuoteFields {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QuoteFields::ALWAYS => write!(f, "ALWAYS"),
            QuoteFields::ASNEEDED => write!(f, "ASNEEDED"),
        }
    }
}
