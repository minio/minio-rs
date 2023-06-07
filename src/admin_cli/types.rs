#[derive(Debug, Clone)]
pub struct ProcessResponse {
    pub cmd: String,
    pub output: std::process::Output,
}

#[derive(Debug, Clone)]
pub enum UserStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
    pub status: UserStatus,
}
