//! `legion login` / `legion logout` — exchange credentials for an ACL token
//! and persist it for subsequent CLI calls.
//!
//! Methods:
//! - `userpass` (default): username/password against the manager's user store.
//! - any configured `oidc` auth method: opens the browser for the standard
//!   authorization-code + PKCE flow on a loopback callback, then exchanges
//!   the resulting ID token with the manager (see `crate::cmd::oidc`).
//! - any configured JWT method: pass the externally issued JWT with `--jwt`
//!   or `--jwt-file` and the manager exchanges it for an ACL token.
//!
//! The minted secret is written to `~/.legion/token` (override with
//! `LEGION_TOKEN_FILE`); `LEGION_TOKEN` always takes precedence when set.

use crate::cmd::helpers::{cli_token_path, connect_manager_client, json_flag, save_cli_token};
use crate::cmd::oidc;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{GetAuthMethodsRequest, LoginRequest};

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
            Arg::new("callback-port")
                .long("callback-port")
                .value_name("PORT")
                .help("Loopback port for the OIDC browser callback (default: 4749)"),
        )
        .arg(
            Arg::new("no-browser")
                .long("no-browser")
                .action(ArgAction::SetTrue)
                .help("Print the OIDC authorization URL instead of opening a browser"),
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

    // OIDC methods without an explicit JWT run the browser redirect flow to
    // obtain an ID token first.
    let jwt = match jwt {
        Some(jwt) => Some(jwt),
        None if method != "userpass" => {
            let methods = output::resolve(
                format,
                "Failed to look up auth methods",
                client
                    .get_auth_methods(tonic::Request::new(GetAuthMethodsRequest {}))
                    .await,
            )?
            .into_inner()
            .methods;
            let info = methods.iter().find(|m| m.name == method);
            match info {
                Some(info) if info.kind == "oidc" => {
                    let issuer = info.oidc_issuer.clone().ok_or_else(|| {
                        anyhow!("auth method `{method}` has no issuer configured")
                    })?;
                    let client_id = info.oidc_client_id.clone().ok_or_else(|| {
                        anyhow!(
                            "auth method `{method}` has no oidcClientId configured; \
                             browser login requires one (or pass --jwt)"
                        )
                    })?;
                    let port = arg_matches
                        .get_one::<String>("callback-port")
                        .map(|raw| raw.parse::<u16>())
                        .transpose()
                        .context("invalid --callback-port")?
                        .unwrap_or(oidc::DEFAULT_CALLBACK_PORT);
                    Some(
                        browser_login(&issuer, &client_id, &info.oidc_scopes, port, arg_matches)
                            .await?,
                    )
                }
                _ => None,
            }
        }
        None => None,
    };

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

/// Runs the OIDC authorization-code + PKCE flow and returns the ID token.
async fn browser_login(
    issuer: &str,
    client_id: &str,
    scopes: &[String],
    port: u16,
    arg_matches: &ArgMatches,
) -> R<String> {
    let endpoints = oidc::discover(issuer).await?;
    let pkce = oidc::PkceFlow::generate();
    let url = oidc::authorize_url(
        &endpoints.authorization_endpoint,
        client_id,
        scopes,
        &pkce,
        port,
    )?;

    eprintln!(
        "Opening your browser to complete the login. If it does not open, visit:\n\n  {url}\n"
    );
    if !arg_matches.get_flag("no-browser") {
        oidc::open_browser(&url);
    }
    eprintln!("Waiting for the identity provider callback on 127.0.0.1:{port} ...");

    let code = oidc::wait_for_callback(port, &pkce.state).await?;
    oidc::exchange_code(
        &endpoints.token_endpoint,
        client_id,
        &code,
        &pkce.verifier,
        port,
    )
    .await
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
