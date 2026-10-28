//! `legion app view`: an app's declarative views in the terminal
//! (docs/apps.md, "Views and actions"; apps plan §6.5, "The terminal renders
//! the same views").
//!
//! `legion app view <app>` lists what the app shows you, where. With a slot
//! (and the slot's context) it renders one view through `GetAppView`, the
//! same RPC the SPA's panels and the `legion_app_view` MCP tool call: stats,
//! series (as sparklines), tables, timelines, key-values, notices, and
//! markdown as text, and the actions the view offers with whether you may
//! press them. `-j` returns the validated view document instead.

use crate::cmd::helpers::connect_manager_client;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::app_ui::{AppView, Block, Cell, ColumnType, Slot, Tone};
use crate::structs::{AppViewContext, GetAppViewReply, GetAppViewRequest, ListUiExtensionsRequest};

/// The `view` subcommand of `legion app`.
pub fn command() -> Command {
    Command::new("view")
        .about(
            "Render an app's view in the terminal: what its panel on a job, task, or node page \
             (or its sidebar page or overview card) shows. Without a slot, list what the app \
             shows you, and where",
        )
        .arg(
            Arg::new("name")
                .required(true)
                .value_name("APP")
                .help("The app's name"),
        )
        .arg(
            Arg::new("slot")
                .value_name("SLOT")
                .value_parser(["nav", "overview", "job", "task", "node"])
                .help("Where the view is shown: nav, overview, job, task, or node"),
        )
        .arg(Arg::new("context").value_name("CTX").num_args(0..=2).help(
            "The slot's context: `job` takes a job (`web` or `ns/web`), `task` a job and \
                     a task, `node` a node id; nav and overview take none",
        ))
        .arg(
            Arg::new("view")
                .long("view")
                .value_name("VIEW")
                .help("Which of the slot's views (default: the first one you may see)"),
        )
        .arg(
            Arg::new("group")
                .long("group")
                .value_name("GROUP")
                .help("The task's group, for the task slot"),
        )
        .arg(crate::cmd::helpers::namespace_flag())
        .arg(crate::cmd::helpers::json_flag())
}

fn usage(format: OutputFormat, message: String) -> anyhow::Error {
    output::fail_local(
        format,
        CliError::local(
            "invalid_arguments",
            message,
            Some("legion app view <APP> job <JOB> | task <JOB> <TASK> | node <NODE> | nav | overview"),
        ),
    )
}

/// The context the positional arguments name.
fn context_of(matches: &ArgMatches, slot: Slot, format: OutputFormat) -> R<AppViewContext> {
    let args: Vec<String> = matches
        .get_many::<String>("context")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    let group = matches
        .get_one::<String>("group")
        .cloned()
        .unwrap_or_default();
    if slot != Slot::Task && !group.is_empty() {
        return Err(usage(format, "--group is for the task slot".into()));
    }
    let job = |raw: &str| -> R<(String, String)> {
        let id = crate::cmd::helpers::job_ref(matches, raw)?;
        let (namespace, name) = crate::structs::namespace::split_job_id(&id);
        Ok((namespace.to_string(), name.to_string()))
    };
    let mut context = AppViewContext {
        slot: slot.as_str().to_string(),
        ..Default::default()
    };
    match (slot, args.as_slice()) {
        (Slot::Nav | Slot::Overview, []) => {}
        (Slot::Job, [name]) => {
            (context.namespace, context.job) = job(name)?;
        }
        (Slot::Task, [name, task]) => {
            (context.namespace, context.job) = job(name)?;
            context.task = task.clone();
            context.group = group;
        }
        (Slot::Node, [node]) => context.node = node.clone(),
        (Slot::Nav | Slot::Overview, _) => {
            return Err(usage(format, format!("the {slot} slot takes no context")))
        }
        (Slot::Job, _) => return Err(usage(format, "the job slot takes a job".into())),
        (Slot::Task, _) => {
            return Err(usage(format, "the task slot takes a job and a task".into()))
        }
        (Slot::Node, _) => return Err(usage(format, "the node slot takes a node id".into())),
    }
    Ok(context)
}

/// The JSON form of a rendered view: what `-j` prints and what
/// `legion_app_view` returns.
pub fn view_json(reply: &GetAppViewReply) -> Value {
    let document: Value = serde_json::from_str(&reply.view_json).unwrap_or(Value::Null);
    let actions: Vec<Value> = reply
        .actions
        .iter()
        .map(|action| {
            json!({
                "name": action.name,
                "title": action.title,
                "confirm": action.confirm,
                "inputs": serde_json::from_str::<Value>(&action.inputs_json).unwrap_or(json!({})),
                "permission": action.permission.as_ref().map(|permission| {
                    format!("{}:{}", permission.resource, permission.action)
                }),
                "allowed": action.allowed,
                "mutating": action.mutating,
            })
        })
        .collect();
    json!({
        "app": reply.app,
        "view": reply.view,
        "title": document
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or(&reply.title),
        "context": reply.context,
        "document": document,
        "actions": actions,
        "refreshSeconds": reply.refresh_seconds,
        "allocationId": reply.allocation_id,
        "nodeId": reply.node_id,
    })
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let app = matches
        .get_one::<String>("name")
        .expect("required")
        .trim()
        .to_string();
    let Some(slot) = matches.get_one::<String>("slot") else {
        return list(format, app).await;
    };
    let slot = Slot::parse(slot).map_err(|err| usage(format, err))?;
    let context = context_of(matches, slot, format)?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "render app view",
        client
            .get_app_view(GetAppViewRequest {
                app,
                view: matches
                    .get_one::<String>("view")
                    .cloned()
                    .unwrap_or_default(),
                context: Some(context),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&view_json(&reply));
    } else {
        print!("{}", render_reply(&reply));
    }
    Ok(())
}

async fn list(format: OutputFormat, app: String) -> R<()> {
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "list app views",
        client
            .list_ui_extensions(ListUiExtensionsRequest {
                app: app.clone(),
                ..Default::default()
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({ "extensions": reply.extensions }));
        return Ok(());
    }
    if reply.extensions.is_empty() {
        println!(
            "{app} shows you nothing: it registers no views, is disabled, or each view needs a \
             permission you do not hold anywhere."
        );
        return Ok(());
    }
    println!("{app} shows:");
    for extension in &reply.extensions {
        let what = match extension.slot.as_str() {
            "nav" => format!("a {} page in the sidebar", extension.title),
            "overview" => format!("a {} card on the overview", extension.title),
            "job" => format!("a {} panel on job pages", extension.title),
            "node" => format!("a {} panel on node pages", extension.title),
            "task" if extension.view.is_empty() => "a badge on task rows".to_string(),
            "task" => format!("a {} panel on task pages, and a badge", extension.title),
            other => other.to_string(),
        };
        let needs = extension
            .permission
            .as_ref()
            .map(|permission| format!(" (needs {}:{})", permission.resource, permission.action))
            .unwrap_or_default();
        let view = if extension.view.is_empty() {
            &extension.badge
        } else {
            &extension.view
        };
        let example = match extension.slot.as_str() {
            "job" => format!("legion app view {app} job <JOB> --view {view}"),
            "task" => format!("legion app view {app} task <JOB> <TASK> --view {view}"),
            "node" => format!("legion app view {app} node <NODE> --view {view}"),
            slot => format!("legion app view {app} {slot} --view {view}"),
        };
        println!("  {what}{needs}");
        println!("      {example}");
    }
    Ok(())
}

fn tone_mark(tone: Option<Tone>) -> &'static str {
    match tone {
        Some(Tone::Warn) => " (!)",
        Some(Tone::Danger) => " (!!)",
        Some(Tone::Ok) => " (ok)",
        _ => "",
    }
}

fn format_time(unix: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(unix)
        .ok()
        .and_then(|at| {
            at.format(time::macros::format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second]Z"
            ))
            .ok()
        })
        .unwrap_or_else(|| unix.to_string())
}

fn format_duration(seconds: f64) -> String {
    humantime::format_duration(std::time::Duration::from_secs(seconds.max(0.0) as u64)).to_string()
}

/// A sparkline of `values`, at most `width` wide.
fn sparkline(values: &[f64], width: usize) -> String {
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if values.is_empty() {
        return String::new();
    }
    let step = values.len().div_ceil(width.max(1));
    let sampled: Vec<f64> = values
        .chunks(step)
        .map(|chunk| chunk[chunk.len() - 1])
        .collect();
    let (min, max) = sampled
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), value| {
            (lo.min(*value), hi.max(*value))
        });
    sampled
        .iter()
        .map(|value| {
            if max <= min {
                BARS[3]
            } else {
                BARS[(((value - min) / (max - min)) * 7.0).round() as usize]
            }
        })
        .collect()
}

fn cell_text(cell: Option<&Cell>, kind: ColumnType) -> String {
    let Some(cell) = cell else {
        return String::new();
    };
    let number = match cell {
        Cell::Number(number) => Some(*number),
        _ => None,
    };
    let text = match (kind, number) {
        (ColumnType::Time, Some(at)) => format_time(at as i64),
        (ColumnType::Duration, Some(seconds)) => format_duration(seconds),
        _ => cell.text(),
    };
    match cell {
        Cell::Rich(rich) => format!("{text}{}", tone_mark(rich.tone)),
        _ => text,
    }
}

/// A view as text.
pub fn render_view(title: &str, view: &AppView) -> String {
    let mut out = String::new();
    let title = view.title.as_deref().unwrap_or(title);
    if !title.is_empty() {
        out.push_str(&format!("{title}\n"));
    }
    if view.hidden {
        out.push_str("  Nothing to show here (the app hides this view for this context).\n");
        return out;
    }
    if let Some(badge) = &view.badge {
        out.push_str(&format!(
            "  Badge: {}{}\n",
            badge.text,
            tone_mark(badge.tone)
        ));
    }
    for block in &view.blocks {
        out.push('\n');
        match block {
            Block::Stats(stats) => {
                if let Some(title) = &stats.title {
                    out.push_str(&format!("{title}\n"));
                }
                let line: Vec<String> = stats
                    .items
                    .iter()
                    .map(|stat| format!("{}: {}{}", stat.label, stat.value, tone_mark(stat.tone)))
                    .collect();
                out.push_str(&format!("  {}\n", line.join("   ")));
                for stat in stats.items.iter().filter(|stat| stat.hint.is_some()) {
                    out.push_str(&format!(
                        "    {}: {}\n",
                        stat.label,
                        stat.hint.as_deref().unwrap_or_default()
                    ));
                }
            }
            Block::Series(series) => {
                if let Some(title) = &series.title {
                    out.push_str(&format!("{title}\n"));
                }
                for line in &series.series {
                    let values: Vec<f64> = line.points.iter().map(|(_, value)| *value).collect();
                    let unit = line.unit.as_deref().unwrap_or_default();
                    let summary = match (values.first(), values.last()) {
                        (Some(_), Some(last)) => {
                            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
                            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                            format!(
                                "now {}{unit}, min {}{unit}, max {}{unit}",
                                crate::structs::app_ui::format_number(*last),
                                crate::structs::app_ui::format_number(min),
                                crate::structs::app_ui::format_number(max)
                            )
                        }
                        _ => "no points".to_string(),
                    };
                    out.push_str(&format!(
                        "  {:<12} {:<40} {summary}\n",
                        line.name,
                        sparkline(&values, 40)
                    ));
                }
            }
            Block::Table(table) => {
                if let Some(title) = &table.title {
                    out.push_str(&format!("{title}\n"));
                }
                if table.rows.is_empty() {
                    out.push_str(&format!(
                        "  {}\n",
                        table.empty.as_deref().unwrap_or("(no rows)")
                    ));
                    continue;
                }
                let mut builder = tabled::builder::Builder::default();
                builder.push_record(table.columns.iter().map(|column| column.label.clone()));
                for row in &table.rows {
                    builder.push_record(
                        table
                            .columns
                            .iter()
                            .map(|column| cell_text(row.get(&column.key), column.kind)),
                    );
                }
                let mut rendered = builder.build();
                rendered.with(tabled::settings::Style::blank());
                for line in rendered.to_string().lines() {
                    out.push_str(&format!("  {}\n", line.trim_end()));
                }
            }
            Block::Timeline(timeline) => {
                if let Some(title) = &timeline.title {
                    out.push_str(&format!("{title}\n"));
                }
                if timeline.items.is_empty() {
                    out.push_str("  (nothing yet)\n");
                }
                for item in &timeline.items {
                    out.push_str(&format!(
                        "  {}  {}{}\n",
                        format_time(item.at),
                        item.title,
                        tone_mark(item.tone)
                    ));
                    if let Some(detail) = &item.detail {
                        out.push_str(&format!("                        {detail}\n"));
                    }
                }
            }
            Block::KeyValue(key_value) => {
                if let Some(title) = &key_value.title {
                    out.push_str(&format!("{title}\n"));
                }
                let width = key_value
                    .items
                    .iter()
                    .map(|item| item.key.chars().count())
                    .max()
                    .unwrap_or(0);
                for item in &key_value.items {
                    out.push_str(&format!(
                        "  {:<width$}  {}{}\n",
                        format!("{}:", item.key),
                        item.value,
                        tone_mark(item.tone),
                        width = width + 1
                    ));
                }
            }
            Block::Notice(notice) => {
                let label = match notice.tone {
                    Tone::Danger => "error",
                    Tone::Warn => "warning",
                    Tone::Ok => "ok",
                    Tone::Info => "info",
                    Tone::Neutral => "note",
                };
                match &notice.title {
                    Some(title) => out.push_str(&format!("[{label}] {title}: {}\n", notice.text)),
                    None => out.push_str(&format!("[{label}] {}\n", notice.text)),
                }
            }
            Block::Markdown(markdown) => {
                for line in markdown.text.lines() {
                    out.push_str(&format!("  {line}\n"));
                }
            }
            Block::Actions(actions) => {
                let labels: Vec<String> = actions
                    .items
                    .iter()
                    .map(|button| {
                        button
                            .label
                            .clone()
                            .unwrap_or_else(|| button.action.clone())
                    })
                    .collect();
                out.push_str(&format!("Actions: {}\n", labels.join(", ")));
            }
        }
    }
    out
}

/// A GetAppView reply as text: the view, then the actions it offers.
pub fn render_reply(reply: &GetAppViewReply) -> String {
    let view: AppView = serde_json::from_str(&reply.view_json).unwrap_or_default();
    let mut out = render_view(&reply.title, &view);
    if !reply.actions.is_empty() {
        out.push('\n');
        for action in &reply.actions {
            let permission = action
                .permission
                .as_ref()
                .map(|permission| format!("{}:{}", permission.resource, permission.action))
                .unwrap_or_default();
            let can = if action.allowed {
                "you may run it from the UI"
            } else {
                "you lack it"
            };
            out.push_str(&format!(
                "  {} ({}): needs {permission}; {can}\n",
                action.name, action.title
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::app_ui::validate_view_value;

    /// The plan's example view renders as text: stats on one line, the
    /// series as sparklines, the timeline with times, and the actions.
    #[test]
    fn the_example_view_renders_as_text() {
        let view = validate_view_value(
            &json!({ "view": "legion-view/1", "blocks": [
                { "type": "stats", "items": [
                    { "label": "Now", "value": "6" }, { "label": "Bounds", "value": "2–8" },
                    { "label": "CPU", "value": "91%", "tone": "warn" } ] },
                { "type": "series", "title": "Replicas vs CPU (1h)", "series": [
                    { "name": "replicas", "points": [[1792412400, 4], [1792412460, 6]] },
                    { "name": "cpu", "axis": "right", "unit": "%", "points": [[1792412400, 91]] } ] },
                { "type": "table", "columns": [
                    { "key": "job", "label": "JOB" }, { "key": "at", "label": "AT", "type": "time" } ],
                  "rows": [{ "job": { "text": "web", "route": "/jobs/default/web" }, "at": 1792412545 }] },
                { "type": "timeline", "items": [
                    { "at": 1792412545, "title": "4 → 6", "detail": "cpu 0.91 vs 0.70 (4 ready)" } ] },
                { "type": "keyValue", "items": [{ "key": "Policy", "value": "cpu" }] },
                { "type": "notice", "tone": "warn", "text": "Paused until 13:00" },
                { "type": "actions", "items": [{ "action": "pause", "label": "Pause 1h" }] }
            ] }),
            None,
            None,
        )
        .expect("valid");
        let text = render_view("Autoscaling", &view);
        assert!(text.starts_with("Autoscaling\n"), "{text}");
        assert!(
            text.contains("Now: 6   Bounds: 2–8   CPU: 91% (!)"),
            "{text}"
        );
        assert!(text.contains("replicas"), "{text}");
        assert!(text.contains("now 6, min 4, max 6"), "{text}");
        assert!(text.contains("now 91%"), "{text}");
        assert!(text.contains("2026-10-19 12:22:25Z"), "{text}");
        assert!(text.contains("4 → 6"), "{text}");
        assert!(text.contains("Policy:  cpu"), "{text}");
        assert!(text.contains("[warning] Paused until 13:00"), "{text}");
        assert!(text.contains("Actions: Pause 1h"), "{text}");
        assert!(text.contains("web"), "{text}");
        assert_eq!(
            render_view("x", &AppView::hidden()),
            "x\n  Nothing to show here (the app hides this view for this context).\n"
        );
    }

    #[test]
    fn sparklines_scale_to_the_range() {
        assert_eq!(sparkline(&[0.0, 7.0], 10), "▁█");
        assert_eq!(sparkline(&[3.0, 3.0], 10), "▄▄");
        assert_eq!(sparkline(&[1.0; 100], 10).chars().count(), 10);
    }

    #[test]
    fn positional_context_follows_the_slot() {
        let parse = |args: &[&str]| {
            let mut argv = vec!["view", "fake"];
            argv.extend_from_slice(args);
            command().try_get_matches_from(argv)
        };
        let matches = parse(&["job", "payments/api"]).expect("parses");
        let context = context_of(&matches, Slot::Job, OutputFormat::Human).expect("context");
        assert_eq!(
            (context.namespace.as_str(), context.job.as_str()),
            ("payments", "api")
        );
        let matches = parse(&["task", "web", "agent", "--group", "g"]).expect("parses");
        let context = context_of(&matches, Slot::Task, OutputFormat::Human).expect("context");
        assert_eq!(context.task, "agent");
        assert_eq!(context.group, "g");
        assert!(parse(&["sidebar"]).is_err());
    }
}
