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

use crate::s3::builders::{DeleteObjectAnnotation, DeleteObjectAnnotationBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{AnnotationName, BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`DeleteObjectAnnotation`] request builder that permanently
    /// removes a single named annotation (irreversible).
    ///
    /// To execute, call [`S3Api::send()`](crate::s3::types::S3Api::send), which
    /// returns a [`DeleteObjectAnnotationResponse`](crate::s3::response::DeleteObjectAnnotationResponse).
    ///
    /// 🛈 This is an AIStor extension and is not part of the S3 API.
    pub fn delete_object_annotation<B, O, N>(
        &self,
        bucket: B,
        object: O,
        annotation_name: N,
    ) -> Result<DeleteObjectAnnotationBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
        N: TryInto<AnnotationName>,
        N::Error: Into<ValidationErr>,
    {
        Ok(DeleteObjectAnnotation::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?)
            .annotation_name(annotation_name.try_into().map_err(Into::into)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::s3::MinioClient;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;
    use crate::s3::types::ToS3Request;

    fn test_client() -> MinioClient {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        MinioClient::new(base_url, Some(provider), None, None).unwrap()
    }

    #[test]
    fn client_method_wires_the_request() {
        let req = test_client()
            .delete_object_annotation("bucket", "object", "review")
            .unwrap()
            .build()
            .to_s3request()
            .unwrap();
        assert!(req.query_params.contains_key("annotation"));
        assert_eq!(
            req.query_params.get("annotationName").map(String::as_str),
            Some("review")
        );
    }

    #[test]
    fn client_method_rejects_empty_name() {
        assert!(
            test_client()
                .delete_object_annotation("bucket", "object", "")
                .is_err()
        );
    }
}
