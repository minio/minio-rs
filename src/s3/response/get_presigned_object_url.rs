use crate::s3::types::{BucketName, ObjectKey, Region, VersionId};

#[derive(Clone, Debug)]
/// Response of [get_presigned_object_url()](crate::s3::client::MinioClient::get_presigned_object_url) API
pub struct GetPresignedObjectUrlResponse {
    /// The AWS region where the bucket resides.
    pub region: Region,

    /// Name of the bucket containing the object.
    pub bucket: BucketName,

    /// Key (path) identifying the object within the bucket.
    pub object: ObjectKey,

    /// The version ID of the object, if versioning is enabled.
    pub version_id: Option<VersionId>,

    /// The presigned URL for the object.
    pub url: String,
}
