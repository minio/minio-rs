#[derive(Clone, Debug)]
/// Response of [get_presigned_object_url()](crate::s3::client::MinioClient::get_presigned_object_url) API
pub struct GetPresignedObjectUrlResponse {
    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// TODO
    pub version_id: Option<String>,

    /// The presigned URL for the object.
    pub url: String,
}
