pub mod args;
pub mod error;
pub mod pbac;
pub mod response;
pub mod types;
pub mod utils;

use crate::admin_cli::{
    args::*,
    error::{Error, ErrorResponse},
    response::*,
    types::User,
};
use crate::s3::client::Client;
use crate::s3::creds::Provider;
use crate::s3::http::BaseUrl;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::ffi::OsStr;
use std::io::Write;
use tokio::process::Command;
use types::ProcessResponse;

#[derive(Clone, Debug, Default)]
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

        (client_id, mc_host)
    }

    fn list_of_jsons_to_json(list_of_jsons: &str) -> String {
        let mut single_json = list_of_jsons.replace('\n', ",").trim_end().to_string();
        single_json.pop();
        format!("[{}]", single_json)
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
                access_key: args.access_key.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.access_key.into()))?
                    .into(),
            )
        }
    }

    pub async fn remove_user(
        &self,
        args: &mut RemoveUserArgs<'_>,
    ) -> Result<RemoveUserResponse, Error> {
        let process_response = self.command(["user", "remove"], [args.access_key]).await?;

        if process_response.output.status.success() {
            Ok(RemoveUserResponse {
                access_key: args.access_key.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.access_key.into()))?
                    .into(),
            )
        }
    }

    pub async fn enable_user(
        &self,
        args: &mut EnableUserArgs<'_>,
    ) -> Result<EnableUserResponse, Error> {
        let process_response = self.command(["user", "enable"], [args.access_key]).await?;

        if process_response.output.status.success() {
            Ok(EnableUserResponse {
                access_key: args.access_key.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.access_key.into()))?
                    .into(),
            )
        }
    }

    pub async fn disable_user(
        &self,
        args: &mut DisableUserArgs<'_>,
    ) -> Result<DisableUserResponse, Error> {
        let process_response = self.command(["user", "disable"], [args.access_key]).await?;

        if process_response.output.status.success() {
            Ok(DisableUserResponse {
                access_key: args.access_key.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.access_key.into()))?
                    .into(),
            )
        }
    }

    pub async fn list_users(&self, _args: &mut ListUsersArgs) -> Result<ListUsersResponse, Error> {
        let process_response = self.command(["user", "list"], ["--json", "-q"]).await?;

        if process_response.output.status.success() {
            let result_content = Self::list_of_jsons_to_json(std::str::from_utf8(
                process_response.output.stdout.as_slice(),
            )?);

            let users: Vec<User> = serde_json::from_str(&result_content)?;
            Ok(ListUsersResponse { users })
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn create_policy(
        &self,
        args: &mut CreatePolicyArgs<'_>,
    ) -> Result<CreatePolicyResponse, Error> {
        let mut tempfile = tempfile::NamedTempFile::new()?;
        write!(tempfile, "{}", serde_json::to_string(args.policy)?)?;
        let tempfile_path = tempfile.into_temp_path();

        let process_response = self
            .command(
                ["policy", "create"],
                [
                    args.policy_name,
                    tempfile_path
                        .to_str()
                        .ok_or(Error::SystemIOError("Could not get tempfile path".into()))?,
                    "-q",
                ],
            )
            .await?;

        if process_response.output.status.success() {
            Ok(CreatePolicyResponse {
                policy_name: args.policy_name.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.policy_name.into()))?
                    .into(),
            )
        }
    }

    pub async fn list_policies(
        &self,
        _args: &mut ListPoliciesArgs,
    ) -> Result<ListPoliciesResponse, Error> {
        let process_response = self.command(["policy", "ls"], ["--json", "-q"]).await?;

        if process_response.output.status.success() {
            let result_content = Self::list_of_jsons_to_json(std::str::from_utf8(
                process_response.output.stdout.as_slice(),
            )?);

            let policies: Vec<types::Policy> = serde_json::from_str(&result_content)?;
            Ok(ListPoliciesResponse { policies })
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn remove_policy(
        &self,
        args: &mut RemovePolicyArgs<'_>,
    ) -> Result<RemovePolicyResponse, Error> {
        let process_response = self
            .command(["policy", "rm"], [args.policy_name, "-q"])
            .await?;

        if process_response.output.status.success() {
            Ok(RemovePolicyResponse {
                policy_name: args.policy_name.into(),
            })
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn attach_policy(
        &self,
        args: &mut AttachPolicyArgs<'_>,
    ) -> Result<AttachPolicyResponse, Error> {
        let mut commnds_args = args.policy_names.to_vec();
        commnds_args.push("-q");

        let attach_to = match args.attaching_to {
            UserGroup::User(u) => {
                commnds_args.push("--user");
                commnds_args.push(u);
                u
            }
            UserGroup::Group(g) => {
                commnds_args.push("--group");
                commnds_args.push(g);
                g
            }
        };

        let process_response = self.command(["policy", "attach"], commnds_args).await?;

        if process_response.output.status.success() {
            Ok(AttachPolicyResponse {
                attaching_to: attach_to.into(),
            })
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn detach_policy(
        &self,
        args: &mut DetachPolicyArgs<'_>,
    ) -> Result<DetachPolicyResponse, Error> {
        let mut commnds_args = args.policy_names.to_vec();
        commnds_args.push("-q");

        let detach_from = match args.detaching_from {
            UserGroup::User(u) => {
                commnds_args.push("--user");
                commnds_args.push(u);
                u
            }
            UserGroup::Group(g) => {
                commnds_args.push("--group");
                commnds_args.push(g);
                g
            }
        };

        let process_response = self.command(["policy", "detach"], commnds_args).await?;

        if process_response.output.status.success() {
            Ok(DetachPolicyResponse {
                detaching_from: detach_from.into(),
            })
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn get_policy(
        &self,
        args: &mut GetPolicyArgs<'_>,
    ) -> Result<GetPolicyResponse, Error> {
        let process_response = self
            .command(["policy", "info"], [args.policy_name, "--json", "-q"])
            .await?;

        if process_response.output.status.success() {
            let policy_reponse: types::Policy =
                serde_json::from_slice(&process_response.output.stdout)?;
            Ok(policy_reponse.policy_info)
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn add_svcacct(
        &self,
        args: &mut AddSvcacctArgs<'_>,
    ) -> Result<AddSvcacctResponse, Error> {
        let mut commands_args: Vec<&str> = [
            args.account,
            "-q",
            "--json",
            "--access-key",
            args.access_key,
            "--secret-key",
            args.secret_key,
        ]
        .into();

        let tempfile_path;
        if let Some(policy) = args.policy {
            let mut tempfile = tempfile::NamedTempFile::new()?;
            write!(tempfile, "{}", serde_json::to_string(policy)?)?;
            tempfile_path = tempfile.into_temp_path();
            commands_args.push("--policy");
            commands_args.push(
                tempfile_path
                    .to_str()
                    .ok_or(Error::SystemIOError("Could not get tempfile path".into()))?,
            );
        }

        let expiry_str;
        if let Some(expiry) = args.expiry {
            expiry_str = utils::mc_date_format::format_serialize(expiry);
            commands_args.push("--expiry");
            commands_args.push(&expiry_str);
        }

        if let Some(name) = args.name {
            commands_args.push("--name");
            commands_args.push(name);
        }

        if let Some(description) = args.description {
            commands_args.push("--description");
            commands_args.push(description);
        }

        let process_response = self
            .command(["user", "svcacct", "add"], commands_args)
            .await?;

        if process_response.output.status.success() {
            Ok(serde_json::from_slice(
                process_response.output.stdout.as_slice(),
            )?)
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.access_key.into()))?
                    .into(),
            )
        }
    }

    pub async fn remove_svcacct(
        &self,
        args: &mut RemoveSvcacctArgs<'_>,
    ) -> Result<RemoveSvcacctResponse, Error> {
        let process_response = self
            .command(["user", "svcacct", "remove"], [args.service_account])
            .await?;

        if process_response.output.status.success() {
            Ok(RemoveSvcacctResponse {
                service_account: args.service_account.into(),
            })
        } else {
            Err(
                ErrorResponse::parse_output(&process_response, Some(args.service_account.into()))?
                    .into(),
            )
        }
    }

    pub async fn list_groups(&self, _args: &mut ListGroupsArgs) -> Result<ListGroupsResponse, Error> {
        let process_response = self.command(["group", "list"], ["--json", "-q"]).await?;

        if process_response.output.status.success() {
            let result_content = Self::list_of_jsons_to_json(std::str::from_utf8(
                process_response.output.stdout.as_slice(),
            )?);

            Ok(serde_json::from_str(&result_content)?)
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    pub async fn get_group(&self, args: &mut GetGroupArgs<'_>) -> Result<GetGroupResponse, Error> {
        let process_response = self.command(["group", "info"], [args.group_name, "--json", "-q"]).await?;

        if process_response.output.status.success() {
            let result_content = Self::list_of_jsons_to_json(std::str::from_utf8(
                process_response.output.stdout.as_slice(),
            )?);

            Ok(serde_json::from_str(&result_content)?)
        } else {
            Err(ErrorResponse::parse_output(&process_response, None)?.into())
        }
    }

    // Here should go implementation for list_svcacct but its current implementation
    // is absolutely not stable


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
