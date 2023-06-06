use crate::admin_cli::ProcessResponse;
use std::fmt::Display;

#[derive(Clone, Debug, Default)]
pub struct ErrorResponse {
    pub code: String,
    pub stdout: String,
    pub stderr: String,
    pub cmd: String,
    pub target_id: Option<String>,
}

impl ErrorResponse {
    pub fn parse_output(
        process_response: &ProcessResponse,
        target_id: Option<String>,
    ) -> Result<Self, Error> {
        Ok(ErrorResponse {
            code: process_response.output.status.to_string(),

            stdout: std::str::from_utf8(process_response.output.stdout.as_slice())
                .map_err(|x| Error::Utf8ParsingError(x.to_string()))?
                .to_owned(),

            stderr: std::str::from_utf8(process_response.output.stderr.as_slice())
                .map_err(|x| Error::Utf8ParsingError(x.to_string()))?
                .to_owned(),

            cmd: process_response.cmd.clone(),
            target_id,
        })
    }
}

#[derive(Debug)]
pub enum Error {
    InitializationError,
    ExecutionError(String),
    Utf8ParsingError(String),
    CmdFailed(ErrorResponse),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InitializationError => write!(f, "Could not initialize AdminCliClient"),
            Error::ExecutionError(e) => write!(f, "Error could not be run: {}", e),
            Error::Utf8ParsingError(e) => write!(f, "Message could not be parsed: {}", e),
            Error::CmdFailed(c) => write!(f, "Command returned non zero code: {:?}", c),
        }
    }
}

impl std::error::Error for Error {}

impl From<ErrorResponse> for Error {
    fn from(value: ErrorResponse) -> Self {
        Self::CmdFailed(value)
    }
}
