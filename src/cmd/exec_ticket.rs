//! `legion exec-ticket` — mint exec tickets for direct exec
//! (docs/exec-and-actions.md, "Direct exec").
//!
//! An exec ticket lets a client open one session on a worker directly, with
//! no manager on the data path. `mint` runs the authorization `legion exec`
//! runs and prints the ticket with the worker address, the node name to
//! verify, and the cluster CA to verify it with. `legion exec --direct`
//! mints and redeems in one step.

use crate::cmd::helpers::{connect_manager_client, json_flag, AuthedManagerClient};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::worker_client::WorkerClient;
use crate::structs::{MintExecTicketReply, MintExecTicketRequest};
use tonic::transport::Channel;

pub fn config() -> Command {
    let mint_cmd = Command::new("mint")
        .about("Mint an exec ticket for one direct session")
        .arg(
            Arg::new("allocation")
                .value_name("ALLOCATION")
                .help("Allocation id the session runs in (or use --instance)"),
        )
        .arg(
            Arg::new("instance")
                .long("instance")
                .value_name("ID")
                .conflicts_with("allocation")
                .help("Instance the session runs in, resolved to its current allocation"),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .help("Task within the allocation (for multi-task allocations)"),
        )
        .arg(
            Arg::new("action")
                .long("action")
                .value_name("ACTION")
                .help("Mint for a declared action; its argv is rendered by the manager"),
        )
        .arg(
            Arg::new("param")
                .long("param")
                .value_name("KEY=VALUE")
                .action(ArgAction::Append)
                .requires("action")
                .help("Action parameter (repeatable)"),
        )
        .arg(
            Arg::new("tty")
                .short('t')
                .long("tty")
                .action(ArgAction::SetTrue)
                .help("The session is interactive (a pseudo-terminal)"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("DURATION")
                .help("How long the ticket can be redeemed (default 60s, at most 10m)"),
        )
        .arg(
            Arg::new("max-session")
                .long("max-session")
                .value_name("DURATION")
                .help("How long the session may run once opened (default and ceiling 1h)"),
        )
        .arg(
            Arg::new("command")
                .value_name("COMMAND")
                .num_args(1..)
                .last(true)
                .conflicts_with("action")
                .help("The command the ticket allows (after --); omit to allow any command"),
        )
        .arg(json_flag());

    let sign_cmd = Command::new("sign")
        .about("Sign an exec ticket offline with a delegated exec key's private key")
        .arg(
            Arg::new("key")
                .long("key")
                .value_name("FILE")
                .required(true)
                .value_hint(ValueHint::FilePath)
                .help("The P-256 private key, PKCS#8 PEM"),
        )
        .arg(
            Arg::new("key-id")
                .long("key-id")
                .value_name("ID")
                .required(true)
                .help("The key's id from `legion exec-key create` (xk_...)"),
        )
        .arg(
            Arg::new("node")
                .long("node")
                .value_name("NODE")
                .required(true)
                .help("The node the session runs on"),
        )
        .arg(
            Arg::new("alloc")
                .long("alloc")
                .value_name("ID")
                .required(true)
                .help("The allocation the session runs in"),
        )
        .arg(
            Arg::new("job")
                .long("job")
                .value_name("JOB")
                .required(true)
                .help("The allocation's job (must match the key's glob)"),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .required(true)
                .help("The allocation's task"),
        )
        .arg(
            Arg::new("instance")
                .long("instance")
                .value_name("ID")
                .help("The instance, for the audit record"),
        )
        .arg(
            Arg::new("tty")
                .short('t')
                .long("tty")
                .action(ArgAction::SetTrue)
                .help("The session is interactive"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("DURATION")
                .default_value("60s")
                .help("Redeem window (at most the key's maxTtl)"),
        )
        .arg(
            Arg::new("max-session")
                .long("max-session")
                .value_name("DURATION")
                .default_value("1h")
                .help("Session length (at most the key's maxSession)"),
        )
        .arg(
            Arg::new("command")
                .value_name("COMMAND")
                .num_args(1..)
                .last(true)
                .help("The command the ticket allows (after --); omit to allow any command"),
        )
        .arg(json_flag());

    Command::new("exec-ticket")
        .about("Mint exec tickets for direct exec")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(mint_cmd)
        .subcommand(sign_cmd)
}

/// What `exec-ticket sign` puts in a ticket.
pub struct SignRequest {
    pub key_pem: String,
    pub key_id: String,
    pub node: String,
    pub allocation_id: i64,
    pub job: String,
    pub task: String,
    pub instance: Option<String>,
    pub argv: Option<Vec<String>>,
    pub tty: bool,
    pub ttl: std::time::Duration,
    pub max_session: std::time::Duration,
}

/// Signs an exec ticket with a delegated key's private half: the JWT a
/// platform's own code would produce. Returns `(ticket, jti, exp)`.
pub fn sign_delegated(request: &SignRequest, now_unix: i64) -> R<(String, String, i64)> {
    let jti = crate::structs::exec_ticket::new_exec_ticket_id();
    let exp = now_unix + request.ttl.as_secs() as i64;
    let claims = crate::structs::exec_ticket::ExecTicketClaims {
        sub: format!("key:{}", request.key_id),
        jti: jti.clone(),
        iat: now_unix,
        exp,
        node: request.node.clone(),
        job: request.job.clone(),
        task: request.task.clone(),
        allocation_id: request.allocation_id,
        instance: request.instance.clone(),
        argv: request.argv.clone(),
        action: None,
        tty: request.tty,
        max_session: request.max_session.as_secs(),
        uses: 1,
    };
    let mut body = claims.to_claims();
    body.insert(
        "aud".into(),
        json!([crate::structs::exec_ticket::EXEC_TICKET_AUDIENCE]),
    );
    body.insert("nbf".into(), json!(now_unix));
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    header.kid = Some(request.key_id.clone());
    let key = jsonwebtoken::EncodingKey::from_ec_pem(request.key_pem.as_bytes())
        .context("the signing key must be a P-256 private key in PKCS#8 PEM")?;
    let ticket = jsonwebtoken::encode(&header, &body, &key).context("sign the exec ticket")?;
    Ok((ticket, jti, exp))
}

async fn sign_cmd(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let arg = |name: &str| matches.get_one::<String>(name).cloned().unwrap_or_default();
    let key_path = arg("key");
    let request = SignRequest {
        key_pem: std::fs::read_to_string(&key_path)
            .with_context(|| format!("could not read {key_path}"))?,
        key_id: arg("key-id"),
        node: arg("node"),
        allocation_id: arg("alloc")
            .parse()
            .context("--alloc must be an allocation id")?,
        job: arg("job"),
        task: arg("task"),
        instance: matches.get_one::<String>("instance").cloned(),
        argv: matches
            .get_many::<String>("command")
            .map(|values| values.cloned().collect()),
        tty: matches.get_flag("tty"),
        ttl: humantime::parse_duration(&arg("ttl")).context("--ttl is not a duration")?,
        max_session: humantime::parse_duration(&arg("max-session"))
            .context("--max-session is not a duration")?,
    };
    let (ticket, jti, exp) =
        sign_delegated(&request, time::OffsetDateTime::now_utc().unix_timestamp())?;
    let expires_at = time::OffsetDateTime::from_unix_timestamp(exp)
        .ok()
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default();
    if format.is_json() {
        output::print_success(&json!({
            "ticket": ticket,
            "jti": jti,
            "node": request.node,
            "allocationId": request.allocation_id,
            "expiresAt": expires_at,
        }));
    } else {
        println!(
            "Exec ticket {jti}, redeemable on {} until {expires_at}:\n",
            request.node
        );
        println!("  {ticket}");
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("mint", sub)) => mint_cmd(sub).await,
        Some(("sign", sub)) => sign_cmd(sub).await,
        _ => bail!("unknown exec-ticket subcommand"),
    }
}

async fn mint_cmd(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let allocation_id = match matches.get_one::<String>("allocation") {
        Some(raw) => raw
            .parse::<i64>()
            .with_context(|| format!("invalid allocation id `{raw}`"))?,
        None => 0,
    };
    let instance_id = matches
        .get_one::<String>("instance")
        .cloned()
        .unwrap_or_default();
    if allocation_id == 0 && instance_id.is_empty() {
        bail!("name the session's target: an allocation id or --instance <id>");
    }
    let mut action_params = std::collections::HashMap::new();
    for raw in matches.get_many::<String>("param").into_iter().flatten() {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| anyhow!("--param must be key=value, got `{raw}`"))?;
        action_params.insert(key.to_string(), value.to_string());
    }
    let request = MintExecTicketRequest {
        allocation_id,
        instance_id,
        task_name: matches
            .get_one::<String>("task")
            .cloned()
            .unwrap_or_default(),
        command: matches
            .get_many::<String>("command")
            .map(|values| values.cloned().collect())
            .unwrap_or_default(),
        action: matches
            .get_one::<String>("action")
            .cloned()
            .unwrap_or_default(),
        action_params,
        tty: matches.get_flag("tty"),
        ttl: matches
            .get_one::<String>("ttl")
            .cloned()
            .unwrap_or_default(),
        max_session: matches
            .get_one::<String>("max-session")
            .cloned()
            .unwrap_or_default(),
    };

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to mint an exec ticket",
        mint(&mut client, request).await,
    )?;

    if format.is_json() {
        output::print_success(&json!({
            "ticket": reply.ticket,
            "worker": reply.worker_address,
            "node": reply.node,
            "expiresAt": reply.expires_at,
            "allocationId": reply.allocation_id,
            "jti": reply.jti,
            "maxSessionSeconds": reply.max_session_seconds,
            "caPem": reply.ca_pem,
        }));
    } else {
        println!(
            "Exec ticket {} for allocation {}.\n",
            reply.jti, reply.allocation_id
        );
        println!("  {}\n", reply.ticket);
        println!("Worker:      {}", reply.worker_address);
        println!(
            "Verify as:   {}",
            crate::manager::mesh_ca::node_dns_name(&reply.node)
        );
        println!("Redeem by:   {}", reply.expires_at);
        println!("Session cap: {}s", reply.max_session_seconds);
        println!(
            "\nPresent it once, in ExecStart.exec_ticket on Worker/ExecTask, over TLS that \
             trusts only the cluster CA (`legion net ca show`)."
        );
    }
    Ok(())
}

/// Calls `MintExecTicket`.
pub async fn mint(
    client: &mut AuthedManagerClient,
    request: MintExecTicketRequest,
) -> Result<MintExecTicketReply, tonic::Status> {
    Ok(client.mint_exec_ticket(request).await?.into_inner())
}

/// A worker client for the node a ticket names: TLS that trusts only the
/// cluster CA from the mint reply and accepts only that node's certificate
/// (`<node>.node.legion`), with no client certificate (the ticket is the
/// credential).
pub async fn worker_client(reply: &MintExecTicketReply) -> R<WorkerClient<Channel>> {
    if reply.ca_pem.trim().is_empty() {
        bail!("the manager returned no cluster CA to verify the worker with");
    }
    let name = crate::manager::mesh_ca::node_dns_name(&reply.node);
    let roots = crate::cluster_tls::SwappableRoots::from_pem(&reply.ca_pem)?;
    let verifier = crate::cluster_tls::NamedServerVerifier::new(roots, &name)?;
    let config = crate::cluster_tls::client_config(verifier, None)?;
    let channel = crate::cluster_tls::tls_connect(&reply.worker_address, config, &name)
        .await
        .with_context(|| {
            format!(
                "could not reach node {} at {} for direct exec\nHint: direct exec needs a \
                 route to the worker's port; drop --direct to go through the manager",
                reply.node, reply.worker_address
            )
        })?;
    Ok(WorkerClient::new(channel))
}

#[cfg(test)]
mod exec_ticket_cmd_test {
    use super::*;

    #[test]
    fn mint_parses_its_target_and_command_test() {
        let matches = config()
            .try_get_matches_from([
                "exec-ticket",
                "mint",
                "--instance",
                "user-42",
                "--tty",
                "--ttl",
                "30s",
                "--",
                "/bin/bash",
                "-l",
            ])
            .expect("parses");
        let (_, mint) = matches.subcommand().expect("mint");
        assert_eq!(mint.get_one::<String>("instance").unwrap(), "user-42");
        assert!(mint.get_flag("tty"));
        let command: Vec<_> = mint.get_many::<String>("command").unwrap().collect();
        assert_eq!(command, ["/bin/bash", "-l"]);
        // An action ticket names no command: the manager renders it.
        assert!(config()
            .try_get_matches_from(["exec-ticket", "mint", "7", "--action", "a", "--", "sh"])
            .is_err());
    }
}
