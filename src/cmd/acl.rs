//! `legion acl` — administer the ACL system: policies, tokens, and
//! username/password users. The initial management token is minted by the
//! manager at first start (docs/auth-and-acls.md, "Quick start").
//!
//! See docs/auth-and-acls.md for the policy schema and workflows.

use crate::cmd::helpers::{
    connect_manager_client, job_arg, json_flag, namespace_flag, print_table, required_job_arg,
};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    AclPolicy, AclTokenInfo, AclUser, CreateAclTokenRequest, DeleteAclPolicyRequest,
    DeleteAclTokenRequest, DeleteAclUserRequest, GetAclPolicyRequest, JobGrant,
    ListAclPoliciesRequest, ListAclTokensRequest, ListAclUsersRequest, ListGrantsRequest,
    RevokeGrantRequest, UpsertAclPolicyRequest, UpsertAclUserRequest, WhoAmIRequest,
};
use tabled::Tabled;

pub fn config() -> Command {
    let whoami_cmd = Command::new("whoami")
        .about("Show how the manager authenticates you and which policies apply")
        .arg(json_flag());

    let policy_cmd = Command::new("policy")
        .about("Manage ACL policies")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("apply")
                .about("Create or replace a policy from a rules file (JSON or Nickel)")
                .arg(Arg::new("name").value_name("NAME").required(true))
                .arg(
                    Arg::new("file")
                        .value_name("FILE")
                        .help("Policy file: {description?, rules: [...]} or a bare rules array")
                        .value_hint(ValueHint::FilePath),
                )
                .arg(
                    Arg::new("rules")
                        .long("rules")
                        .value_name("JSON")
                        .help("Inline JSON rules array (alternative to FILE)"),
                )
                .arg(
                    Arg::new("description")
                        .long("description")
                        .value_name("TEXT")
                        .help("Policy description"),
                )
                .arg(json_flag()),
        )
        .subcommand(Command::new("list").about("List policies").arg(json_flag()))
        .subcommand(
            Command::new("get")
                .about("Show one policy, including its rules")
                .arg(Arg::new("name").value_name("NAME").required(true))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a policy")
                .arg(Arg::new("name").value_name("NAME").required(true))
                .arg(json_flag()),
        );

    let token_cmd = Command::new("token")
        .about("Manage ACL tokens")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create a token attached to existing policies")
                .arg(
                    Arg::new("name")
                        .long("name")
                        .short('n')
                        .value_name("NAME")
                        .required(true)
                        .help("Human-readable token name (for audit logs)"),
                )
                .arg(
                    Arg::new("policy")
                        .long("policy")
                        .short('p')
                        .value_name("POLICY")
                        .action(ArgAction::Append)
                        .help("Policy to attach (repeatable)"),
                )
                .arg(
                    Arg::new("type")
                        .long("type")
                        .value_name("TYPE")
                        .default_value("client")
                        .help("Token type: client or management"),
                )
                .arg(
                    Arg::new("ttl")
                        .long("ttl")
                        .value_name("TTL")
                        .help("Lifetime (for example 8h, 30d). Omit for a non-expiring token"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("list")
                .about("List tokens (secrets are never shown)")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Revoke a token by accessor ID")
                .arg(
                    Arg::new("accessor-id")
                        .value_name("ACCESSOR_ID")
                        .required(true),
                )
                .arg(json_flag()),
        );

    let user_cmd = Command::new("user")
        .about("Manage username/password users for `legion login`")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("upsert")
                .about("Create or update a user")
                .arg(Arg::new("username").value_name("USERNAME").required(true))
                .arg(
                    Arg::new("password")
                        .long("password")
                        .value_name("PASSWORD")
                        .help("Password (omit to keep the existing one when updating)"),
                )
                .arg(
                    Arg::new("policy")
                        .long("policy")
                        .short('p')
                        .value_name("POLICY")
                        .action(ArgAction::Append)
                        .help("Policy granted on login (repeatable)"),
                )
                .arg(json_flag()),
        )
        .subcommand(Command::new("list").about("List users").arg(json_flag()))
        .subcommand(
            Command::new("delete")
                .about("Delete a user")
                .arg(Arg::new("username").value_name("USERNAME").required(true))
                .arg(json_flag()),
        );

    let grants_cmd = Command::new("grants")
        .about(
            "List and revoke job grants: what jobs' `legion` identities were granted at submit \
             (docs/workload-identity.md, \"Permissions\")",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List job grants and who made them")
                .arg(
                    Arg::new("job")
                        .long("job")
                        .value_name("JOB")
                        .help("Only this job's grants (`name`, or `namespace/name`)"),
                )
                .arg(namespace_flag())
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .action(ArgAction::SetTrue)
                        .help("Include revoked grants"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("revoke")
                // `--version` is the job config version here, as on
                // `legion template`, not the binary's.
                .disable_version_flag(true)
                .about(
                    "Revoke a job's grants; its tokens lose them at their next request. The \
                     next submit by someone who holds the access grants them again",
                )
                .arg(Arg::new("job").value_name("JOB").required(true))
                .arg(namespace_flag())
                .arg(
                    Arg::new("version")
                        .long("version")
                        .value_name("VERSION")
                        .value_parser(clap::value_parser!(i64))
                        .help("Revoke only this job config version's grant (default: every version's)"),
                )
                .arg(
                    Arg::new("idempotency-key")
                        .long("idempotency-key")
                        .value_name("KEY")
                        .help("Dedupe retries: a repeat with the same key returns the first reply"),
                )
                .arg(json_flag()),
        );

    Command::new("acl")
        .about("Administer authentication and ACLs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(whoami_cmd)
        .subcommand(policy_cmd)
        .subcommand(token_cmd)
        .subcommand(user_cmd)
        .subcommand(grants_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("whoami", arg_matches)) => whoami(OutputFormat::from_matches(arg_matches)).await,
        Some(("policy", policy_matches)) => match policy_matches.subcommand() {
            Some(("apply", arg_matches)) => policy_apply(arg_matches).await,
            Some(("list", arg_matches)) => {
                policy_list(OutputFormat::from_matches(arg_matches)).await
            }
            Some(("get", arg_matches)) => {
                let name = arg_matches
                    .get_one::<String>("name")
                    .context("Must provide name")?;
                policy_get(name, OutputFormat::from_matches(arg_matches)).await
            }
            Some(("delete", arg_matches)) => {
                let name = arg_matches
                    .get_one::<String>("name")
                    .context("Must provide name")?;
                policy_delete(name, OutputFormat::from_matches(arg_matches)).await
            }
            _ => unreachable!(),
        },
        Some(("token", token_matches)) => match token_matches.subcommand() {
            Some(("create", arg_matches)) => token_create(arg_matches).await,
            Some(("list", arg_matches)) => {
                token_list(OutputFormat::from_matches(arg_matches)).await
            }
            Some(("delete", arg_matches)) => {
                let accessor = arg_matches
                    .get_one::<String>("accessor-id")
                    .context("Must provide accessor ID")?;
                token_delete(accessor, OutputFormat::from_matches(arg_matches)).await
            }
            _ => unreachable!(),
        },
        Some(("grants", grants_matches)) => match grants_matches.subcommand() {
            Some(("list", arg_matches)) => grants_list(arg_matches).await,
            Some(("revoke", arg_matches)) => grants_revoke(arg_matches).await,
            _ => unreachable!(),
        },
        Some(("user", user_matches)) => match user_matches.subcommand() {
            Some(("upsert", arg_matches)) => user_upsert(arg_matches).await,
            Some(("list", arg_matches)) => user_list(OutputFormat::from_matches(arg_matches)).await,
            Some(("delete", arg_matches)) => {
                let username = arg_matches
                    .get_one::<String>("username")
                    .context("Must provide username")?;
                user_delete(username, OutputFormat::from_matches(arg_matches)).await
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

async fn whoami(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to query identity",
        client.who_am_i(tonic::Request::new(WhoAmIRequest {})).await,
    )?;
    let reply = response.into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "aclEnabled": reply.acl_enabled,
            "auditOnly": reply.audit_only,
            "source": reply.source,
            "principal": reply.principal,
            "tokenType": reply.token_type,
            "policies": reply.policies,
            "expiresAt": reply.expires_at,
            "accessorId": reply.accessor_id,
        }));
    } else if !reply.acl_enabled {
        println!(
            "ACLs: disabled by the manager config (acl.enabled = false); every caller, \
             including any workload that can reach the manager, has full access"
        );
    } else {
        println!(
            "ACLs:       enabled{}",
            if reply.audit_only {
                " (audit-only)"
            } else {
                ""
            }
        );
        println!("Principal:  {} (via {})", reply.principal, reply.source);
        println!("Token type: {}", reply.token_type);
        if let Some(accessor) = reply.accessor_id {
            println!("Accessor:   {accessor}");
        }
        if !reply.policies.is_empty() {
            println!("Policies:   {}", reply.policies.join(", "));
        }
        if let Some(expires) = reply.expires_at {
            println!("Expires:    {expires}");
        }
    }
    Ok(())
}

/// Reads policy rules from a file (JSON or Nickel) or an inline string.
/// Accepts either a bare rules array or `{description?, rules: [...]}`.
fn read_policy_source(
    file: Option<&String>,
    inline: Option<&String>,
    description_flag: Option<&String>,
) -> R<(String, String)> {
    let raw = match (file, inline) {
        // Both languages go through the one loader: `.json` is parsed, and
        // anything else is evaluated as Nickel.
        (Some(path), None) => crate::config_lang::eval_file(path)
            .with_context(|| format!("Error rendering policy {path}"))?,
        (None, Some(inline)) => inline.clone(),
        (Some(_), Some(_)) => bail!("provide either FILE or --rules, not both"),
        (None, None) => bail!("provide a policy FILE or --rules <json>"),
    };

    let value: Value = serde_json::from_str(&raw).context("policy is not valid JSON")?;
    let (rules, file_description) = match &value {
        Value::Array(_) => (value.clone(), None),
        Value::Object(object) => {
            let rules = object
                .get("rules")
                .context("policy object is missing a `rules` array")?
                .clone();
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string);
            (rules, description)
        }
        _ => bail!("policy must be a rules array or an object with a `rules` array"),
    };
    let description = description_flag
        .cloned()
        .or(file_description)
        .unwrap_or_default();
    Ok((serde_json::to_string(&rules)?, description))
}

async fn policy_apply(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let name = arg_matches
        .get_one::<String>("name")
        .context("Must provide name")?;
    let (rules_json, description) = match read_policy_source(
        arg_matches.get_one::<String>("file"),
        arg_matches.get_one::<String>("rules"),
        arg_matches.get_one::<String>("description"),
    ) {
        Ok(parsed) => parsed,
        Err(e) => {
            return Err(output::fail_local(
                format,
                CliError::local("InvalidArgument", e.to_string(), None),
            ))
        }
    };

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to apply ACL policy",
        client
            .upsert_acl_policy(tonic::Request::new(UpsertAclPolicyRequest {
                name: name.clone(),
                description,
                rules_json,
            }))
            .await,
    )?;
    let reply = response.into_inner();
    let policy = reply.policy.unwrap_or_default();

    if format.is_json() {
        let mut data = policy_json(&policy);
        data["warnings"] = json!(reply.warnings);
        output::print_success(&data);
    } else {
        for warning in &reply.warnings {
            eprintln!("warning: {warning}");
        }
        println!("Applied ACL policy `{}`", policy.name);
    }
    Ok(())
}

fn policy_json(policy: &AclPolicy) -> Value {
    json!({
        "name": policy.name,
        "description": policy.description,
        "rules": serde_json::from_str::<Value>(&policy.rules_json).unwrap_or(Value::Null),
        "createdAt": policy.created_at,
        "updatedAt": policy.updated_at,
    })
}

#[derive(Tabled)]
struct PolicyPrint {
    name: String,
    description: String,
    updated_at: String,
}

async fn policy_list(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to list ACL policies",
        client
            .list_acl_policies(tonic::Request::new(ListAclPoliciesRequest {}))
            .await,
    )?;
    let policies = response.into_inner().policies;

    if format.is_json() {
        output::print_success(&policies.iter().map(policy_json).collect::<Vec<_>>());
    } else if policies.is_empty() {
        println!("No ACL policies");
    } else {
        print_table(
            policies
                .into_iter()
                .map(|policy| PolicyPrint {
                    name: policy.name,
                    description: policy.description,
                    updated_at: policy.updated_at,
                })
                .collect(),
        );
    }
    Ok(())
}

async fn policy_get(name: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to get ACL policy",
        client
            .get_acl_policy(tonic::Request::new(GetAclPolicyRequest {
                name: name.to_string(),
            }))
            .await,
    )?;
    let policy = response.into_inner().policy.unwrap_or_default();

    if format.is_json() {
        output::print_success(&policy_json(&policy));
    } else {
        println!("Name:        {}", policy.name);
        if !policy.description.is_empty() {
            println!("Description: {}", policy.description);
        }
        println!("Updated:     {}", policy.updated_at);
        let pretty = serde_json::from_str::<Value>(&policy.rules_json)
            .and_then(|value| serde_json::to_string_pretty(&value))
            .unwrap_or(policy.rules_json);
        println!("Rules:\n{pretty}");
    }
    Ok(())
}

async fn policy_delete(name: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to delete ACL policy",
        client
            .delete_acl_policy(tonic::Request::new(DeleteAclPolicyRequest {
                name: name.to_string(),
            }))
            .await,
    )?;
    let message = response.into_inner().message;
    if format.is_json() {
        output::print_success(&json!({ "message": message }));
    } else {
        println!("{message}");
    }
    Ok(())
}

async fn token_create(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let name = arg_matches
        .get_one::<String>("name")
        .context("Must provide --name")?;
    let token_type = arg_matches
        .get_one::<String>("type")
        .map(String::as_str)
        .unwrap_or("client");
    let policies: Vec<String> = arg_matches
        .get_many::<String>("policy")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    let ttl = arg_matches.get_one::<String>("ttl").cloned();

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to create ACL token",
        client
            .create_acl_token(tonic::Request::new(CreateAclTokenRequest {
                name: name.clone(),
                token_type: token_type.to_string(),
                policies,
                ttl,
            }))
            .await,
    )?;
    let reply = response.into_inner();
    let token = reply.token.unwrap_or_default();

    if format.is_json() {
        output::print_success(&json!({
            "accessorId": token.accessor_id,
            "secret": reply.secret,
            "name": token.name,
            "tokenType": token.token_type,
            "policies": token.policies,
            "expiresAt": token.expires_at,
        }));
    } else {
        println!(
            "Created ACL token `{}`. The secret is shown only once.\n",
            token.name
        );
        println!("  Accessor ID: {}", token.accessor_id);
        println!("  Secret:      {}", reply.secret);
        if !token.policies.is_empty() {
            println!("  Policies:    {}", token.policies.join(", "));
        }
        if let Some(expires) = token.expires_at {
            println!("  Expires:     {expires}");
        }
    }
    Ok(())
}

#[derive(Tabled)]
struct TokenPrint {
    accessor_id: String,
    name: String,
    r#type: String,
    policies: String,
    expires_at: String,
}

fn token_json(token: &AclTokenInfo) -> Value {
    json!({
        "accessorId": token.accessor_id,
        "name": token.name,
        "tokenType": token.token_type,
        "policies": token.policies,
        "createdAt": token.created_at,
        "expiresAt": token.expires_at,
    })
}

async fn token_list(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to list ACL tokens",
        client
            .list_acl_tokens(tonic::Request::new(ListAclTokensRequest {}))
            .await,
    )?;
    let tokens = response.into_inner().tokens;

    if format.is_json() {
        output::print_success(&tokens.iter().map(token_json).collect::<Vec<_>>());
    } else if tokens.is_empty() {
        println!("No ACL tokens");
    } else {
        print_table(
            tokens
                .into_iter()
                .map(|token| TokenPrint {
                    accessor_id: token.accessor_id,
                    name: token.name,
                    r#type: token.token_type,
                    policies: token.policies.join(", "),
                    expires_at: token.expires_at.unwrap_or_else(|| "never".to_string()),
                })
                .collect(),
        );
    }
    Ok(())
}

async fn token_delete(accessor_id: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to delete ACL token",
        client
            .delete_acl_token(tonic::Request::new(DeleteAclTokenRequest {
                accessor_id: accessor_id.to_string(),
            }))
            .await,
    )?;
    let message = response.into_inner().message;
    if format.is_json() {
        output::print_success(&json!({ "message": message }));
    } else {
        println!("{message}");
    }
    Ok(())
}

async fn user_upsert(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let username = arg_matches
        .get_one::<String>("username")
        .context("Must provide username")?;
    let password = arg_matches
        .get_one::<String>("password")
        .cloned()
        .unwrap_or_default();
    let policies: Vec<String> = arg_matches
        .get_many::<String>("policy")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();

    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to upsert ACL user",
        client
            .upsert_acl_user(tonic::Request::new(UpsertAclUserRequest {
                username: username.clone(),
                password,
                policies,
            }))
            .await,
    )?;
    let user = response.into_inner().user.unwrap_or_default();

    if format.is_json() {
        output::print_success(&user_json(&user));
    } else {
        println!(
            "Saved ACL user `{}` (policies: {})",
            user.username,
            user.policies.join(", ")
        );
    }
    Ok(())
}

#[derive(Tabled)]
struct UserPrint {
    username: String,
    policies: String,
    updated_at: String,
}

fn user_json(user: &AclUser) -> Value {
    json!({
        "username": user.username,
        "policies": user.policies,
        "createdAt": user.created_at,
        "updatedAt": user.updated_at,
    })
}

async fn user_list(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to list ACL users",
        client
            .list_acl_users(tonic::Request::new(ListAclUsersRequest {}))
            .await,
    )?;
    let users = response.into_inner().users;

    if format.is_json() {
        output::print_success(&users.iter().map(user_json).collect::<Vec<_>>());
    } else if users.is_empty() {
        println!("No ACL users");
    } else {
        print_table(
            users
                .into_iter()
                .map(|user| UserPrint {
                    username: user.username,
                    policies: user.policies.join(", "),
                    updated_at: user.updated_at,
                })
                .collect(),
        );
    }
    Ok(())
}

async fn user_delete(username: &str, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to delete ACL user",
        client
            .delete_acl_user(tonic::Request::new(DeleteAclUserRequest {
                username: username.to_string(),
            }))
            .await,
    )?;
    let message = response.into_inner().message;
    if format.is_json() {
        output::print_success(&json!({ "message": message }));
    } else {
        println!("{message}");
    }
    Ok(())
}

fn grant_json(grant: &JobGrant) -> Value {
    json!({
        "jobId": grant.job_id,
        "version": grant.version,
        "jobConfigId": grant.job_config_id,
        "tasks": grant.tasks.iter().map(|task| json!({
            "group": task.group,
            "task": task.task,
            "permissions": serde_json::from_str::<Value>(&task.permissions_json)
                .unwrap_or(Value::Null),
            "policies": task.policies,
        })).collect::<Vec<_>>(),
        "grantedBy": grant.granted_by,
        "grantedAt": grant.granted_at,
        "revokedAt": (!grant.revoked_at.is_empty()).then_some(&grant.revoked_at),
        "revokedBy": (!grant.revoked_by.is_empty()).then_some(&grant.revoked_by),
    })
}

/// One line per granted rule or policy, for the table: `kv:read,write on
/// certs/*`, `policy cert-writer`.
fn grant_summary(grant: &JobGrant) -> String {
    let mut lines = Vec::new();
    for task in &grant.tasks {
        let rules: Vec<crate::structs::acl::AclRule> =
            serde_json::from_str(&task.permissions_json).unwrap_or_default();
        for rule in rules {
            let mut line = format!(
                "{}{}:{}",
                if rule.is_deny() { "deny " } else { "" },
                rule.resources.join(","),
                rule.actions.join(",")
            );
            if let Some(paths) = &rule.paths {
                line.push_str(&format!(" on {}", paths.join(",")));
            }
            if let Some(names) = &rule.names {
                line.push_str(&format!(" on jobs {}", names.join(",")));
            }
            if let Some(namespaces) = &rule.namespaces {
                line.push_str(&format!(" in {}", namespaces.join(",")));
            }
            // Usable only when the app acts for one of its objects.
            if rule.is_via_objects() {
                line.push_str(" (via objects)");
            }
            lines.push(if grant.tasks.len() > 1 {
                format!("{}: {line}", task.task)
            } else {
                line
            });
        }
        for policy in &task.policies {
            lines.push(if grant.tasks.len() > 1 {
                format!("{}: policy {policy}", task.task)
            } else {
                format!("policy {policy}")
            });
        }
    }
    lines.join("\n")
}

#[derive(Tabled)]
struct GrantPrint {
    job: String,
    version: i64,
    grants: String,
    granted_by: String,
    granted_at: String,
    revoked: String,
}

async fn grants_list(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = job_arg(arg_matches, "job")?;
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to list grants",
        client
            .list_grants(tonic::Request::new(ListGrantsRequest {
                job,
                include_revoked: arg_matches.get_flag("all"),
            }))
            .await,
    )?;
    let grants = response.into_inner().grants;
    if format.is_json() {
        output::print_success(&grants.iter().map(grant_json).collect::<Vec<_>>());
    } else if grants.is_empty() {
        println!("No grants");
    } else {
        print_table(
            grants
                .iter()
                .map(|grant| GrantPrint {
                    job: grant.job_id.clone(),
                    version: grant.version,
                    grants: grant_summary(grant),
                    granted_by: grant.granted_by.clone(),
                    granted_at: grant.granted_at.clone(),
                    revoked: if grant.revoked_at.is_empty() {
                        "-".to_string()
                    } else {
                        format!("{} by {}", grant.revoked_at, grant.revoked_by)
                    },
                })
                .collect(),
        );
    }
    Ok(())
}

async fn grants_revoke(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = required_job_arg(arg_matches, "job")?;
    let mut client = connect_manager_client().await?;
    let response = output::resolve(
        format,
        "Failed to revoke grants",
        client
            .revoke_grant(tonic::Request::new(RevokeGrantRequest {
                job,
                version: arg_matches.get_one::<i64>("version").copied(),
                idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?;
    let reply = response.into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "revoked": reply.revoked,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}
