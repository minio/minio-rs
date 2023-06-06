#[derive(Debug, Clone)]
pub struct ProcessResponse {
    pub cmd: String,
    pub output: std::process::Output,
}
