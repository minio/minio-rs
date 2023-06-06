pub mod args;
pub mod error;
pub mod response;
pub mod types;

use crate::admin_cli::{
    args::*,
    error::{Error, ErrorResponse},
    response::*,
};
use crate::s3::client::Client;
use crate::s3::creds::Provider;
use crate::s3::http::BaseUrl;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::ffi::OsStr;
use tokio::process::Command;
use types::ProcessResponse;

pub struct AdminCliClient {
    command: String,
    client_id: String,
    mc_host: String,
}

impl AdminCliClient {
    fn set_mc_host(
        base_url: &BaseUrl,
        provider: &(dyn Provider + Send + Sync),
    ) -> (String, String) {
        let client_id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        let creds = provider.fetch();

        let mc_host = format!(
            "{}://{}:{}@{}",
            if base_url.https { "https" } else { "http" },
            creds.access_key,
            creds.secret_key,
            base_url.host_with_port()
        );

        println!("{}", mc_host);

        (client_id, mc_host)
    }

    pub fn new(base_url: &BaseUrl, provider: &(dyn Provider + Send + Sync)) -> AdminCliClient {
        let (client_id, mc_host) = AdminCliClient::set_mc_host(base_url, provider);

        Self {
            client_id,
            mc_host,
            command: "mc".into(),
        }
    }

    pub fn set_command(&mut self, cmd: &str) -> &mut Self {
        self.command = cmd.into();
        self
    }

    async fn command<I1, S1, I2, S2>(
        &self,
        cmd_path: I1,
        args: I2,
    ) -> Result<ProcessResponse, Error>
    where
        I1: IntoIterator<Item = S1> + Clone,
        S1: AsRef<OsStr>,
        I2: IntoIterator<Item = S2>,
        S2: AsRef<OsStr>,
    {
        let cmd_string = cmd_path
            .clone()
            .into_iter()
            .fold(format!("{} admin", self.command.clone()), |acc, x| {
                format!("{} {}", acc, x.as_ref().to_string_lossy())
            });

        let output = Command::new(&self.command)
            .env(&format!("MC_HOST_{}", self.client_id), &self.mc_host)
            .arg("admin")
            .args(cmd_path)
            .arg(&self.client_id)
            .args(args)
            .output()
            .await
            .map_err(|x| Error::ExecutionError(format!("{}: {}", cmd_string, x)))?;

        Ok(ProcessResponse {
            cmd: cmd_string,
            output,
        })
    }

    pub async fn add_user(&self, args: &mut AddUserArgs<'_>) -> Result<AddUserResponse, Error> {
        let process_response = self
            .command(["user", "add"], [args.access_key, args.secret_key])
            .await?;

        if process_response.output.status.success() {
            Ok(AddUserResponse {
                acess_key: args.access_key.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.access_key.into()))?
                    .into(),
            )
        }
    }
}

impl std::convert::TryFrom<&Client<'_>> for AdminCliClient {
    type Error = Error;

    fn try_from(value: &Client<'_>) -> Result<Self, Self::Error> {
        let values = value.base_url_with_provider();
        if let Some(provider) = values.1 {
            Ok(AdminCliClient::new(values.0, provider))
        } else {
            Err(Error::InitializationError)
        }
    }
}