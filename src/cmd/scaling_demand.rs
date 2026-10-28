//! `legion scaling demand`: how many nodes of a node-group template the
//! pending work needs (`GetCapacityDemand`, docs/autoscaling.md, "Capacity
//! demand").
//!
//! One template from flags, or any number from a file (`-f`): Nickel or
//! JSON holding a node-group policy (its `name` and `target.nodes` are the
//! template, so the file `legion scaling apply` will take works as is), a
//! bare template `{ name, pool, workerType, shape, likeNode, upcoming }`,
//! or a list of either.

use crate::autoscaler::nodes::{NodeGroupPolicy, NodeShape};
use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetCapacityDemandReply, GetCapacityDemandRequest, NodeGroupShape, NodeGroupTemplate,
};
use std::collections::BTreeMap;

/// A template as a file writes it.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TemplateFile {
    name: String,
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    worker_type: Option<String>,
    #[serde(default)]
    shape: Option<NodeShape>,
    #[serde(default)]
    like_node: Option<String>,
    #[serde(default)]
    upcoming: u32,
}

/// The `demand` subcommand.
pub fn demand_command() -> Command {
    Command::new("demand")
        .about("How many nodes of a node-group shape the pending work needs")
        .long_about(
            "\
        How many nodes of a node-group shape the pending work needs.\n\
        \n\
        Packs every replica the scheduler could not place onto copies of the\n\
        template, with the scheduler's own rules (permit and pool, node\n\
        namespaces, constraints, distinct, static ports, resources, gangs),\n\
        and reports the nodes needed and the replicas no template can host,\n\
        with the rule that fails. Read-only. One template from the flags, or\n\
        several from -f (a node-group policy, a template, or a list).\n\
        \n\
        Examples:\n\
        \x20 legion scaling demand --pool gpu --cpu 16000 --memory 65536 --attr gpu=a100\n\
        \x20 legion scaling demand -f nodes.ncl -j\
        ",
        )
        .arg(
            Arg::new("file")
                .short('f')
                .long("file")
                .value_name("PATH")
                .value_hint(ValueHint::FilePath)
                .help("Nickel or JSON: a node-group policy, a template, or a list of either")
                .conflicts_with_all([
                    "name",
                    "pool",
                    "worker-type",
                    "cpu",
                    "memory",
                    "cores",
                    "attr",
                    "resources",
                    "max-allocations",
                    "like-node",
                    "upcoming",
                ]),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("The node group's name (default: nodes)"),
        )
        .arg(
            Arg::new("pool")
                .long("pool")
                .value_name("POOL")
                .help("The pool members join (default: the worker type's)"),
        )
        .arg(
            Arg::new("worker-type")
                .long("worker-type")
                .value_name("TYPE")
                .help("The worker type members join as (default: default)"),
        )
        .arg(
            Arg::new("cpu")
                .long("cpu")
                .value_name("MHZ")
                .value_parser(clap::value_parser!(u64).range(1..))
                .help("CPU of one member, MHz"),
        )
        .arg(
            Arg::new("memory")
                .long("memory")
                .value_name("MIB")
                .value_parser(clap::value_parser!(u64).range(1..))
                .help("Memory of one member, MiB"),
        )
        .arg(
            Arg::new("cores")
                .long("cores")
                .value_name("N")
                .value_parser(clap::value_parser!(u32).range(1..))
                .help("Whole cores of one member"),
        )
        .arg(
            Arg::new("attr")
                .long("attr")
                .value_name("KEY=VALUE")
                .action(ArgAction::Append)
                .help("A node attribute every member has, repeatable"),
        )
        .arg(
            Arg::new("resources")
                .long("resources")
                .value_name("JSON")
                .help("Custom resources of one member: '{\"gpu\": 2}'"),
        )
        .arg(
            Arg::new("max-allocations")
                .long("max-allocations")
                .value_name("N")
                .value_parser(clap::value_parser!(u32))
                .help("The most allocations one member takes"),
        )
        .arg(
            Arg::new("like-node")
                .long("like-node")
                .value_name("NODE")
                .help("Copy an existing member's capacity and attributes"),
        )
        .arg(
            Arg::new("upcoming")
                .long("upcoming")
                .value_name("N")
                .value_parser(clap::value_parser!(u32))
                .help("Members already asked for that have not joined"),
        )
        .arg(json_flag())
}

fn proto_shape(shape: &NodeShape) -> R<NodeGroupShape> {
    Ok(NodeGroupShape {
        cpu: shape.cpu,
        memory: shape.memory,
        cores: shape.cores,
        attributes: shape
            .attributes
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
        resources_json: match &shape.resources {
            Some(resources) => serde_json::to_string(resources)?,
            None => String::new(),
        },
        max_allocations: shape.max_allocations,
    })
}

fn template_from_value(value: Value) -> R<NodeGroupTemplate> {
    if value.get("target").is_some() {
        let policy: NodeGroupPolicy =
            serde_json::from_value(value).context("not a node-group policy")?;
        let policy_name = policy.name.clone();
        let effective = policy.resolve().map_err(|err| anyhow!(err))?;
        return Ok(NodeGroupTemplate {
            name: policy_name,
            worker_type: effective.worker_type.clone(),
            pool: effective.pool.clone(),
            shape: Some(proto_shape(&effective.shape)?),
            like_node: None,
            upcoming: 0,
        });
    }
    let template: TemplateFile =
        serde_json::from_value(value).context("not a node-group template")?;
    let shape = template.shape.unwrap_or_default();
    shape.validate("shape").map_err(|err| anyhow!(err))?;
    Ok(NodeGroupTemplate {
        name: template.name,
        worker_type: template.worker_type.unwrap_or_default(),
        pool: template.pool,
        shape: Some(proto_shape(&shape)?),
        like_node: template.like_node,
        upcoming: template.upcoming,
    })
}

/// The templates a file holds.
fn templates_from_file(path: &str) -> R<Vec<NodeGroupTemplate>> {
    let rendered = crate::config_lang::eval_file(path)
        .with_context(|| format!("failed to evaluate {path}"))?;
    let value: Value = serde_json::from_str(&rendered)?;
    let items = match value {
        Value::Array(items) => items,
        other => vec![other],
    };
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            template_from_value(item).with_context(|| format!("{path}: entry {index}"))
        })
        .collect()
}

fn template_from_flags(matches: &ArgMatches) -> R<NodeGroupTemplate> {
    let mut attributes = BTreeMap::new();
    for raw in matches.get_many::<String>("attr").into_iter().flatten() {
        let (key, value) = raw
            .split_once('=')
            .with_context(|| format!("--attr {raw:?} is not KEY=VALUE"))?;
        attributes.insert(key.to_string(), value.to_string());
    }
    let resources = match matches.get_one::<String>("resources") {
        Some(raw) => Some(
            serde_json::from_str::<BTreeMap<String, Value>>(raw)
                .with_context(|| format!("--resources {raw:?} is not a JSON object"))?,
        ),
        None => None,
    };
    let shape = NodeShape {
        cpu: matches.get_one::<u64>("cpu").copied(),
        memory: matches.get_one::<u64>("memory").copied(),
        cores: matches.get_one::<u32>("cores").copied(),
        attributes: (!attributes.is_empty()).then_some(attributes),
        resources,
        max_allocations: matches.get_one::<u32>("max-allocations").copied(),
    };
    shape.validate("shape").map_err(|err| anyhow!(err))?;
    Ok(NodeGroupTemplate {
        name: matches
            .get_one::<String>("name")
            .cloned()
            .unwrap_or_else(|| "nodes".to_string()),
        worker_type: matches
            .get_one::<String>("worker-type")
            .cloned()
            .unwrap_or_default(),
        pool: matches.get_one::<String>("pool").cloned(),
        shape: Some(proto_shape(&shape)?),
        like_node: matches.get_one::<String>("like-node").cloned(),
        upcoming: matches.get_one::<u32>("upcoming").copied().unwrap_or(0),
    })
}

/// Run `legion scaling demand`.
pub async fn handle_demand(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let templates = match matches.get_one::<String>("file") {
        Some(path) => templates_from_file(path),
        None => template_from_flags(matches).map(|template| vec![template]),
    }
    .map_err(|err| {
        output::fail_local(
            format,
            output::CliError::local("InvalidArgument", format!("{err:#}"), None),
        )
    })?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read capacity demand",
        client
            .get_capacity_demand(tonic::Request::new(GetCapacityDemandRequest { templates }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
    } else {
        print!("{}", render(&reply));
    }
    Ok(())
}

fn amount(requested: f64, capacity: f64, unit: &str) -> String {
    if capacity > 0.0 {
        format!("{requested:.0}/{capacity:.0} {unit}")
    } else {
        format!("{requested:.0} {unit}")
    }
}

/// The human form of a reply.
fn render(reply: &GetCapacityDemandReply) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<20} {:>6} {:>9} {:>9}  {:<22} {}\n",
        "GROUP", "NODES", "REPLICAS", "ABSORBED", "CPU (req/cap)", "MEMORY (req/cap)"
    ));
    for group in &reply.groups {
        out.push_str(&format!(
            "{:<20} {:>6} {:>9} {:>9}  {:<22} {}\n",
            group.name,
            group.nodes,
            group.replicas,
            group.absorbed,
            amount(group.cpu_requested_mhz, group.cpu_capacity_mhz, "MHz"),
            amount(group.memory_requested_mib, group.memory_capacity_mib, "MiB"),
        ));
    }
    let label = |index: u32| {
        reply
            .pending
            .get(index as usize)
            .map(|pending| {
                if pending.redacted {
                    format!("<a job in {} you cannot read>", pending.namespace)
                } else {
                    format!("{} ({})", pending.job_id, pending.tasks.join(", "))
                }
            })
            .unwrap_or_else(|| format!("#{index}"))
    };
    let total: u32 = reply.pending.iter().map(|p| p.pending).sum();
    out.push_str(&format!(
        "\nPending work: {} replica(s) in {} task group(s)\n",
        total,
        reply.pending.len()
    ));
    for (index, pending) in reply.pending.iter().enumerate() {
        let gang = pending
            .gang_quorum
            .map(|quorum| format!(", gang quorum {quorum}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "  {}: {} pending ({}/{} placed{gang}), each {}\n",
            label(index as u32),
            pending.pending,
            pending.placed,
            pending.desired,
            pending.resources_json
        ));
    }
    if !reply.unfittable.is_empty() {
        out.push_str("\nUnfittable (no template can host them):\n");
        for entry in &reply.unfittable {
            out.push_str(&format!(
                "  {}: {} replica(s)\n",
                label(entry.pending_index),
                entry.replicas
            ));
            for reason in &entry.reasons {
                out.push_str(&format!(
                    "    {}: {}: {}\n",
                    reason.template, reason.rule, reason.detail
                ));
            }
        }
    }
    for group in &reply.groups {
        let partial: Vec<_> = group
            .refusals
            .iter()
            .filter(|refusal| {
                !reply
                    .unfittable
                    .iter()
                    .any(|entry| entry.pending_index == refusal.pending_index)
            })
            .collect();
        if partial.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "\n{} cannot host (another template can):\n",
            group.name
        ));
        for refusal in partial {
            out.push_str(&format!(
                "  {}: {} replica(s): {}: {}\n",
                label(refusal.pending_index),
                refusal.replicas,
                refusal.rule,
                refusal.detail
            ));
        }
    }
    if !reply.awaiting_scheduler.is_empty() || reply.awaiting_scheduler_redacted > 0 {
        let mut jobs = reply.awaiting_scheduler.clone();
        if reply.awaiting_scheduler_redacted > 0 {
            jobs.push(format!(
                "{} you cannot read",
                reply.awaiting_scheduler_redacted
            ));
        }
        out.push_str(&format!(
            "\nNot counted yet (the scheduler has not tried them): {}\n",
            jobs.join(", ")
        ));
    }
    if reply.truncated {
        out.push_str("\nTruncated: only the first 20000 pending replicas were packed.\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{
        CapacityRefusal, NodeGroupDemand, PendingCapacity, TemplateRefusal, UnfittableCapacity,
    };

    fn parse(args: &[&str]) -> R<ArgMatches> {
        let mut argv = vec!["legion", "scaling"];
        argv.extend(args);
        let matches = crate::cmd::cli().try_get_matches_from(argv)?;
        let (_, scaling) = matches.subcommand().context("scaling")?;
        let (_, demand) = scaling.subcommand().context("demand")?;
        Ok(demand.clone())
    }

    #[test]
    fn flags_build_one_template_test() -> R<()> {
        let matches = parse(&[
            "demand",
            "--name",
            "gpu",
            "--pool",
            "gpu",
            "--cpu",
            "16000",
            "--memory",
            "65536",
            "--attr",
            "gpu=a100",
            "--resources",
            r#"{"gpu": 2}"#,
            "--upcoming",
            "1",
        ])?;
        let template = template_from_flags(&matches)?;
        assert_eq!(template.name, "gpu");
        assert_eq!(template.pool.as_deref(), Some("gpu"));
        assert_eq!(template.upcoming, 1);
        let shape = template.shape.context("shape")?;
        assert_eq!(shape.cpu, Some(16000));
        assert_eq!(shape.attributes["gpu"], "a100");
        assert_eq!(shape.resources_json, r#"{"gpu":2}"#);
        assert!(parse(&["demand", "-f", "x.ncl", "--cpu", "1"]).is_err());
        Ok(())
    }

    #[test]
    fn a_file_takes_policies_templates_and_lists_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("nodes.json");
        std::fs::write(
            &path,
            serde_json::to_string(&json!([
                {
                    "name": "workers-general",
                    "target": { "nodes": {
                        "workerType": "default",
                        "shape": { "cpu": 8000, "memory": 16384 },
                        "plugin": "aws-asg",
                    } },
                    "max": 20,
                },
                { "name": "gpu", "pool": "gpu", "shape": { "cpu": 16000 }, "upcoming": 2 },
            ]))?,
        )?;
        let templates = templates_from_file(path.to_str().context("utf-8")?)?;
        assert_eq!(templates.len(), 2);
        assert_eq!(templates[0].name, "workers-general");
        assert_eq!(templates[0].worker_type, "default");
        assert_eq!(
            templates[0].shape.as_ref().and_then(|s| s.memory),
            Some(16384)
        );
        assert_eq!(templates[1].pool.as_deref(), Some("gpu"));
        assert_eq!(templates[1].upcoming, 2);

        std::fs::write(
            &path,
            r#"{ "name": "x", "max": 1, "target": { "nodes": {} } }"#,
        )?;
        let err = templates_from_file(path.to_str().context("utf-8")?).unwrap_err();
        assert!(format!("{err:#}").contains("entry 0"), "{err:#}");
        Ok(())
    }

    #[test]
    fn render_names_groups_pending_and_unfittable_work_test() {
        let reply = GetCapacityDemandReply {
            groups: vec![NodeGroupDemand {
                name: "workers".into(),
                nodes: 2,
                replicas: 5,
                cpu_requested_mhz: 2500.0,
                cpu_capacity_mhz: 16000.0,
                refusals: vec![CapacityRefusal {
                    pending_index: 1,
                    replicas: 1,
                    rule: "resources".into(),
                    detail: "one replica needs memory 65536 > 16384 per node".into(),
                }],
                ..Default::default()
            }],
            pending: vec![
                PendingCapacity {
                    job_id: "web".into(),
                    namespace: "default".into(),
                    tasks: vec!["web".into()],
                    desired: 6,
                    placed: 1,
                    pending: 5,
                    resources_json: r#"{"cpu":500}"#.into(),
                    ..Default::default()
                },
                PendingCapacity {
                    namespace: "ml".into(),
                    pending: 1,
                    redacted: true,
                    ..Default::default()
                },
            ],
            unfittable: vec![UnfittableCapacity {
                pending_index: 1,
                replicas: 1,
                reasons: vec![TemplateRefusal {
                    template: "workers".into(),
                    rule: "resources".into(),
                    detail: "one replica needs memory 65536 > 16384 per node".into(),
                }],
            }],
            awaiting_scheduler: vec!["batch".into()],
            ..Default::default()
        };
        let text = render(&reply);
        let row: Vec<&str> = text
            .lines()
            .find(|line| line.starts_with("workers"))
            .map(|line| line.split_whitespace().take(4).collect())
            .unwrap_or_default();
        assert_eq!(row, vec!["workers", "2", "5", "0"], "{text}");
        assert!(text.contains("2500/16000 MHz"), "{text}");
        assert!(text.contains("web (web): 5 pending (1/6 placed)"), "{text}");
        assert!(
            text.contains("<a job in ml you cannot read>: 1 replica(s)"),
            "{text}"
        );
        assert!(
            text.contains("workers: resources: one replica needs memory"),
            "{text}"
        );
        assert!(
            text.contains("the scheduler has not tried them): batch"),
            "{text}"
        );
    }
}
