#[derive(Clone, Debug)]
/// Response of [get_presigned_object_url()](crate::s3::client::Client::get_presigned_object_url) API
pub struct GetPresignedObjectUrlResponse {
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub url: String,
}
