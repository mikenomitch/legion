//! `legion device` — device plugin authoring tools (docs/devices.md).
//!
//! - *"Does my device plugin speak `legion-device/1`?"* — `device plugin
//!   test -- <command>`.
//!
//! Where a node's devices are shown: `legion node info` (inventory and
//! health) and an allocation's `resources.devices` (the assignment).

use crate::cmd::helpers::json_flag;
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;

pub fn config() -> Command {
    Command::new("device")
        .about("Device plugin tools (GPUs and other devices; see docs/devices.md)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("plugin")
                .about("Device plugin authoring tools")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("test")
                        .about("Run the legion-device/1 conformance checks against a plugin")
                        .long_about(
                            "Run a device plugin the way a worker does:\n\n  \
                             fingerprint -> fingerprint again (must agree) -> allocate one \
                             healthy instance of each group\n\n\
                             and check every reply against the protocol: valid groups, \
                             unique ids, valid container edits. Runs locally against the \
                             real hardware; nothing is sent to a manager.\n\n\
                             Example: legion device plugin test -- \
                             /opt/legion/plugins/devices/nvidia/legion-device-nvidia",
                        )
                        .arg(
                            Arg::new("name")
                                .long("name")
                                .default_value("plugin")
                                .help("The plugin's name, as the worker type would key it"),
                        )
                        .arg(
                            Arg::new("config")
                                .long("config")
                                .value_name("JSON")
                                .help("The plugin's `config` record, as JSON"),
                        )
                        .arg(
                            Arg::new("timeout")
                                .long("timeout")
                                .default_value("10s")
                                .help("How long one call may run"),
                        )
                        .arg(
                            Arg::new("command")
                                .help("The plugin command and its arguments")
                                .required(true)
                                .num_args(1..)
                                .trailing_var_arg(true)
                                .allow_hyphen_values(true),
                        )
                        .arg(json_flag()),
                ),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("plugin", sub)) => match sub.subcommand() {
            Some(("test", sub)) => test_plugin(sub).await,
            _ => bail!("unknown device plugin command"),
        },
        _ => bail!("unknown device command"),
    }
}

async fn test_plugin(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches
        .get_one::<String>("name")
        .cloned()
        .unwrap_or_else(|| "plugin".into());
    let command: Vec<String> = matches
        .get_many::<String>("command")
        .unwrap_or_default()
        .cloned()
        .collect();
    let config = match matches.get_one::<String>("config") {
        Some(raw) => serde_json::from_str(raw).context("--config must be JSON")?,
        None => Value::Null,
    };
    let plugin = crate::devices::plugin::DevicePluginConfig {
        command,
        config,
        timeout: matches.get_one::<String>("timeout").cloned(),
    };
    let check_name = name.clone();
    let steps = tokio::task::spawn_blocking(move || {
        crate::devices::plugin::conformance(&check_name, &plugin)
    })
    .await?;
    let passed = steps.iter().all(|step| step.passed);
    if format.is_json() {
        output::print_success(&json!({
            "plugin": name,
            "protocol": crate::devices::PROTOCOL,
            "passed": passed,
            "checks": steps,
        }));
    } else {
        println!(
            "Device plugin conformance: {name} ({})\n",
            crate::devices::PROTOCOL
        );
        for step in &steps {
            let mark = if step.passed { "✓" } else { "✗" };
            println!("  {mark} {:<32} {}", step.check, step.detail);
        }
        println!();
        if passed {
            println!("PASS — {name} speaks {}.", crate::devices::PROTOCOL);
        } else {
            println!("FAIL — see docs/devices.md \"Writing a device plugin\".");
        }
    }
    if passed {
        Ok(())
    } else {
        bail!("device plugin {name} failed conformance")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_plugin_test_command_parses() -> R<()> {
        let matches = crate::cmd::cli().try_get_matches_from([
            "legion",
            "device",
            "plugin",
            "test",
            "--json",
            "--",
            "/opt/plugin",
            "--cdi=index",
        ])?;
        let (_, device) = matches.subcommand().context("device")?;
        let (_, plugin) = device.subcommand().context("plugin")?;
        let (_, test) = plugin.subcommand().context("test")?;
        let command: Vec<&String> = test.get_many::<String>("command").unwrap().collect();
        assert_eq!(command, vec!["/opt/plugin", "--cdi=index"]);
        Ok(())
    }
}
