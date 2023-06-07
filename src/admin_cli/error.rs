use crate::admin_cli::ProcessResponse;
use std::fmt::Display;
use std::str::Utf8Error;

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
            stdout: std::str::from_utf8(process_response.output.stdout.as_slice())?.to_owned(),
            stderr: std::str::from_utf8(process_response.output.stderr.as_slice())?.to_owned(),
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
    AdminParsingError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InitializationError => write!(f, "Could not initialize AdminCliClient"),
            Error::ExecutionError(e) => write!(f, "Error could not be run: {}", e),
            Error::Utf8ParsingError(e) => write!(f, "Message could not be parsed: {}", e),
            Error::CmdFailed(c) => write!(f, "Command returned non zero code: {:?}", c),
            Error::AdminParsingError(p) => write!(f, "Could not parse cli command output: {}", p),
        }
    }
}

impl std::error::Error for Error {}

impl From<ErrorResponse> for Error {
    fn from(value: ErrorResponse) -> Self {
        Self::CmdFailed(value)
    }
}

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::Utf8ParsingError(value.to_string())
    }
}
