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

use crate::s3::error::ValidationErr;
use crate::s3::response_traits::HasS3Fields;
use crate::s3::types::{Bucket, BucketName, S3Request};
use crate::s3::utils::{from_iso8601utc, get_text_result};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use xmltree::Element;

/// Response of [list_buckets()](crate::s3::client::MinioClient::list_buckets) API
#[derive(Debug, Clone)]
pub struct ListBucketsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(ListBucketsResponse);
impl_has_s3fields!(ListBucketsResponse);

impl ListBucketsResponse {
    /// Returns the list of buckets in the account.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the body is not the expected XML, or when
    /// a listed name does not satisfy the [`BucketName`] rules.
    pub fn buckets(&self) -> Result<Vec<Bucket>, ValidationErr> {
        parse_buckets(self.body().clone())
    }
}

/// Parses the `<Buckets>` element of a `ListAllMyBucketsResult` body.
///
/// Each listed name goes through [`BucketName::new`], so a name that reaches a
/// caller carries the same invariant as one written in code and can be passed
/// straight back to any operation.
fn parse_buckets(body: Bytes) -> Result<Vec<Bucket>, ValidationErr> {
    let mut root = Element::parse(body.reader())?;
    let buckets_xml = root
        .get_mut_child("Buckets")
        .ok_or(ValidationErr::xml_error("<Buckets> tag not found"))?;

    let mut buckets: Vec<Bucket> = Vec::new();
    while let Some(bucket) = buckets_xml.take_child("Bucket") {
        buckets.push(Bucket {
            name: BucketName::new(get_text_result(&bucket, "Name")?)?,
            creation_date: from_iso8601utc(&get_text_result(&bucket, "CreationDate")?)?,
        })
    }
    Ok(buckets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::client::MinioClient;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;

    fn listing(name: &str) -> Bytes {
        Bytes::from(format!(
            r#"<ListAllMyBucketsResult><Buckets><Bucket>
                <Name>{name}</Name>
                <CreationDate>2024-01-01T00:00:00.000Z</CreationDate>
            </Bucket></Buckets></ListAllMyBucketsResult>"#
        ))
    }

    #[test]
    fn a_listed_name_is_validated() {
        let buckets = parse_buckets(listing("my-bucket")).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name.as_str(), "my-bucket");
    }

    /// A name the server sends is the one path that could hand out a `BucketName`
    /// the strict rules never saw, which every request builder now relies on.
    #[test]
    fn a_listed_name_that_breaks_the_rules_is_rejected() {
        for name in ["My_Bucket", "my_bucket", "ab", "192.168.1.1", "xn--bucket"] {
            assert!(
                matches!(
                    parse_buckets(listing(name)),
                    Err(ValidationErr::InvalidBucketName { .. })
                ),
                "{name} must not become a BucketName"
            );
        }
    }

    /// The same names cannot reach a request through a client method either.
    #[test]
    fn a_name_that_breaks_the_rules_cannot_build_a_request() {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        let client = MinioClient::new(base_url, Some(provider), None, None).unwrap();

        assert!(client.get_object("my-bucket", "key").is_ok());
        for name in ["My_Bucket", "my_bucket", "ab", "192.168.1.1", "xn--bucket"] {
            assert!(
                client.get_object(name, "key").is_err(),
                "{name} must not build a request"
            );
        }
    }
}
