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

#[derive(Clone, Debug, Default)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

pub trait Provider: std::fmt::Debug {
    fn fetch(&self) -> Credentials;
}

#[derive(Clone, Debug)]
pub struct StaticProvider {
    creds: Credentials,
}

impl StaticProvider {
    pub fn new(access_key: &str, secret_key: &str, session_token: Option<&str>) -> StaticProvider {
        StaticProvider {
            creds: Credentials {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                session_token: session_token.map(|v| v.to_string()),
            },
        }
    }
}

impl Provider for StaticProvider {
    fn fetch(&self) -> Credentials {
        self.creds.clone()
    }
}
