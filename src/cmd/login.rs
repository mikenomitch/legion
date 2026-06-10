//! `legion login` / `legion logout` — exchange credentials for an ACL token
//! and persist it for subsequent CLI calls.
//!
//! Methods:
//! - `userpass` (default): username/password against the manager's user store.
//! - any configured JWT/OIDC auth method: pass the externally issued JWT with
//!   `--jwt` or `--jwt-file` and the manager exchanges it for an ACL token.
//!
//! The minted secret is written to `~/.legion/token` (override with
//! `LEGION_TOKEN_FILE`); `LEGION_TOKEN` always takes precedence when set.

use crate::cmd::helpers::{cli_token_path, connect_manager_client, json_flag, save_cli_token};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::LoginRequest;

pub fn login_config() -> Command {
    Command::new("login")
        .about("Log in to the manager and store the resulting ACL token")
        .arg(
            Arg::new("method")
                .long("method")
                .short('m')
                .value_name("METHOD")
                .default_value("userpass")
                .help("Auth method: userpass or a configured JWT/OIDC method name"),
        )
        .arg(
            Arg::new("username")
                .long("username")
                .short('u')
                .value_name("USERNAME")
                .help("Username (userpass method)"),
        )
        .arg(
            Arg::new("password")
                .long("password")
                .short('p')
                .value_name("PASSWORD")
                .help("Password (userpass method; or set LEGION_PASSWORD)"),
        )
        .arg(
            Arg::new("jwt")
                .long("jwt")
                .value_name("JWT")
                .help("Externally issued JWT/OIDC token (JWT methods)"),
        )
        .arg(
            Arg::new("jwt-file")
                .long("jwt-file")
                .value_name("FILE")
                .value_hint(ValueHint::FilePath)
                .help("Read the JWT from a file instead of the command line"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("TTL")
                .help("Requested token lifetime (capped by the server)"),
        )
        .arg(
            Arg::new("no-store")
                .long("no-store")
                .action(ArgAction::SetTrue)
                .help("Print the token instead of writing the token file"),
        )
        .arg(json_flag())
}

pub fn logout_config() -> Command {
    Command::new("logout")
        .about("Remove the stored ACL token")
        .arg(json_flag())
}

#[instrument(err, skip_all)]
pub async fn handle_login(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let method = arg_matches
        .get_one::<String>("method")
        .cloned()
        .unwrap_or_else(|| "userpass".to_string());
    let username = arg_matches.get_one::<String>("username").cloned();
    let password = arg_matches
        .get_one::<String>("password")
        .cloned()
        .or_else(|| std::env::var("LEGION_PASSWORD").ok());
    let jwt = match (
        arg_matches.get_one::<String>("jwt"),
        arg_matches.get_one::<String>("jwt-file"),
    ) {
        (Some(_), Some(_)) => {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    "provide either --jwt or --jwt-file, not both",
                    None,
                ),
            ))
        }
        (Some(jwt), None) => Some(jwt.clone()),
        (None, Some(path)) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("failed to read JWT from {path}"))?
                .trim()
                .to_string(),
        ),
        (None, None) => None,
    };
    let ttl = arg_matches.get_one::<String>("ttl").cloned();

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Login failed",
        client
            .login(tonic::Request::new(LoginRequest {
                method,
                username,
                password,
                jwt,
                ttl,
            }))
            .await,
    )?;
    let reply = response.into_inner();
    let token = reply.token.unwrap_or_default();

    let stored_path = if arg_matches.get_flag("no-store") {
        None
    } else {
        Some(save_cli_token(&reply.secret)?)
    };

    if format.is_json() {
        output::print_success(&json!({
            "accessorId": token.accessor_id,
            "secret": reply.secret,
            "name": token.name,
            "policies": token.policies,
            "expiresAt": token.expires_at,
            "storedAt": stored_path.as_ref().map(|p| p.display().to_string()),
        }));
    } else {
        println!("Logged in as `{}`", token.name);
        if !token.policies.is_empty() {
            println!("Policies: {}", token.policies.join(", "));
        }
        if let Some(expires) = &token.expires_at {
            println!("Expires:  {expires}");
        }
        match stored_path {
            Some(path) => println!("Token stored at {}", path.display()),
            None => println!("Token: {}", reply.secret),
        }
    }
    Ok(())
}

#[instrument(err, skip_all)]
pub async fn handle_logout(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let path = cli_token_path();
    let removed = match std::fs::remove_file(&path) {
        Ok(()) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => {
            return Err(err).with_context(|| format!("failed to remove {}", path.display()))
        }
    };

    if format.is_json() {
        output::print_success(&json!({ "removed": removed, "path": path.display().to_string() }));
    } else if removed {
        println!("Removed stored token at {}", path.display());
    } else {
        println!("No stored token at {}", path.display());
    }
    Ok(())
}
