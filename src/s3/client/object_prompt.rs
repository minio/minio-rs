// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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

//! S3 APIs for downloading objects.

use crate::s3::builders::ObjectPrompt;

use super::Client;

impl Client {
    /// Creates a [`ObjectPrompt`] request builder. Prompt an object using natural language.
    ///
    /// To execute the request, call [`ObjectPrompt::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`ObjectPromptResponse`](crate::s3::response::ObjectPromptResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::ObjectPromptResponse;
    /// use minio::s3::types::S3Api;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: ObjectPromptResponse = client
    ///         .object_prompt("bucket-name", "object-name", "What is it about?")
    ///         .send().await.unwrap();
    ///     println!("the prompt response is: '{}'", resp.prompt_response);
    /// }
    /// ```
    pub fn object_prompt<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        prompt: S3,
    ) -> ObjectPrompt {
        ObjectPrompt::new(self.clone(), bucket.into(), object.into(), prompt.into())
    }
}
