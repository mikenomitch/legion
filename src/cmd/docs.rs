use crate::prelude::*;
use crate::task_plugins::render_builtin_task_plugin_schemas_markdown;

pub fn config() -> Command {
    let task_schema_cmd = Command::new("task-schemas")
        .about("Generate Markdown docs for built-in task plugin config schemas")
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("PATH")
                .value_hint(ValueHint::FilePath)
                .help("Write Markdown output to a file instead of stdout"),
        );

    Command::new("docs")
        .about("Generate documentation from in-code schemas")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(task_schema_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("task-schemas", args)) => generate_task_schema_docs(args),
        _ => unreachable!(),
    }
}

fn generate_task_schema_docs(arg_matches: &ArgMatches) -> R<()> {
    let markdown = render_builtin_task_plugin_schemas_markdown()?;

    if let Some(path) = arg_matches.get_one::<String>("output") {
        std::fs::write(path, &markdown)
            .with_context(|| format!("failed to write task schema docs to {}", path))?;
        println!("Wrote task plugin schema docs to {}", path);
    } else {
        println!("{markdown}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docs_command_parses_task_schemas_subcommand() -> R<()> {
        let matches = config().try_get_matches_from(["docs", "task-schemas"])?;
        let (name, _args) = matches.subcommand().context("missing subcommand")?;
        assert_eq!(name, "task-schemas");
        Ok(())
    }
}
