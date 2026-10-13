use crate::cmd::helpers::{connect_manager_client, friendly_status};
use crate::prelude::*;
use crate::structs::CreateJoinTokenRequest;
use crate::worker::daemon::run_worker;

pub fn config() -> Command {
    Command::new("join")
        .about("Join a cluster")
        .arg(
            Arg::new("token")
                .value_name("TOKEN")
                .help(
                    "Join token issued by the manager. Visible to every local user in the \
                     process list; prefer --token-file for a long-running worker",
                )
                .required(false)
                .conflicts_with_all(["auto", "token-file"]),
        )
        .arg(token_file_arg().conflicts_with("auto"))
        .arg(
            Arg::new("auto")
                .short('a')
                .long("auto")
                .help("Request a join token then start the worker")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("type")
                .long("type")
                .value_name("TYPE")
                .help("Worker type to request (default: default)")
                .requires("auto"),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("TTL")
                .help("Join token TTL (e.g. 1h); 0 disables expiry")
                .requires("auto"),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("Override the worker name/ID"),
        )
        .arg(advertise_addr_arg())
}

/// `--token-file`, shared by `legion join` and `legion start worker`.
///
/// A file, not an environment variable: tasks inherit the worker's
/// environment, so a token there would reach every host-mode exec task.
pub fn token_file_arg() -> Arg {
    Arg::new("token-file")
        .long("token-file")
        .value_name("PATH")
        .value_hint(ValueHint::FilePath)
        .help(
            "Read the join token from this file instead of the command line, where every \
             local user can see it (keep the file mode 0600)",
        )
}

/// The join token from `--token-file` or the positional `TOKEN`, or `None`
/// when neither was given.
///
/// A token on the command line works, but it sits in the worker's argv for
/// the life of the process, readable by any local user through `ps` or
/// `/proc/<pid>/cmdline`, so it earns a warning.
pub fn join_token_from(arg_matches: &ArgMatches) -> R<Option<String>> {
    if let Some(path) = arg_matches.get_one::<String>("token-file") {
        return read_token_file(std::path::Path::new(path)).map(Some);
    }
    let Some(token) = arg_matches.get_one::<String>("token") else {
        return Ok(None);
    };
    warn!(
        "The join token was passed on the command line, where any local user can read it \
         (ps, /proc/<pid>/cmdline); pass it with --token-file instead"
    );
    Ok(Some(token.to_owned()))
}

fn read_token_file(path: &std::path::Path) -> R<String> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the join token from {}", path.display()))?;
    let token = raw.trim();
    if token.is_empty() {
        bail!("the join token file {} is empty", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            warn!(
                path = %path.display(),
                mode = format!("{mode:o}"),
                "The join token file is readable by other users; restrict it with chmod 600"
            );
        }
    }
    Ok(token.to_string())
}

/// `--advertise-addr`, shared by `legion join` and `legion start worker`.
pub fn advertise_addr_arg() -> Arg {
    Arg::new("advertise-addr")
        .long("advertise-addr")
        .value_name("HOST[:PORT]")
        .help(
            "Address managers and routers dial to reach this worker (default: this host's \
             address on the route to the manager; also LEGION_WORKER_ADVERTISE_ADDR)",
        )
}

#[instrument(err, skip_all)]
pub async fn handle(arg_matches: &ArgMatches) -> R<()> {
    let token = if arg_matches.get_flag("auto") {
        let ttl = arg_matches
            .get_one::<String>("ttl")
            .map_or("1h", String::as_str);

        let worker_type = arg_matches
            .get_one::<String>("type")
            .map_or("default", String::as_str);

        request_join_token(ttl, worker_type).await?
    } else {
        join_token_from(arg_matches)?.context("Must provide a token, --token-file, or --auto/-a")?
    };

    let name = arg_matches.get_one::<String>("name").map(String::as_str);
    let advertise = arg_matches
        .get_one::<String>("advertise-addr")
        .map(String::as_str);
    run_worker(&token, name, advertise).await
}

/// `--auto` convenience: mint a join token through the normal authenticated
/// API and immediately join with it (requires `node:write` when ACLs are on).
#[instrument(err, skip_all)]
async fn request_join_token(ttl: &str, worker_type: &str) -> R<String> {
    let mut client = connect_manager_client().await?;

    let request = tonic::Request::new(CreateJoinTokenRequest {
        worker_type: worker_type.to_string(),
        ttl: Some(ttl.to_string()),
        description: "created by `legion join --auto`".to_string(),
    });

    let response = client
        .create_join_token(request)
        .await
        .map_err(|status| friendly_status("failed to request join token", status))?;

    Ok(response.get_ref().token.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_token_file(contents: &str) -> R<tempfile::NamedTempFile> {
        let file = tempfile::NamedTempFile::new()?;
        std::fs::write(file.path(), contents)?;
        Ok(file)
    }

    /// `legion start worker` and `legion join` take the token from a file,
    /// so it never sits in the worker's argv (spot-check P1).
    #[test]
    fn the_token_file_replaces_the_positional_token_test() -> R<()> {
        let file = write_token_file("lgnjoin_abc\n")?;
        let path = file.path().to_str().context("utf-8 path")?;
        for args in [
            vec!["start", "worker", "--token-file", path],
            vec!["join", "--token-file", path],
        ] {
            let mut argv = vec!["legion"];
            argv.extend(args.iter());
            let matches = crate::cmd::cli().try_get_matches_from(&argv)?;
            let mut current = &matches;
            while let Some((_, sub)) = current.subcommand() {
                current = sub;
            }
            assert_eq!(
                join_token_from(current)?.as_deref(),
                Some("lgnjoin_abc"),
                "{argv:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_worker_needs_a_token_from_one_place_test() {
        let cli = crate::cmd::cli;
        assert!(
            cli()
                .try_get_matches_from(["legion", "start", "worker"])
                .is_err(),
            "no token and no --token-file must be a usage error"
        );
        assert!(
            cli()
                .try_get_matches_from([
                    "legion",
                    "start",
                    "worker",
                    "lgnjoin_abc",
                    "--token-file",
                    "/etc/legion/join-token",
                ])
                .is_err(),
            "a token and a token file together are ambiguous"
        );
        assert!(cli()
            .try_get_matches_from(["legion", "join", "--auto", "--token-file", "/x"])
            .is_err());
    }

    #[test]
    fn an_empty_or_missing_token_file_is_an_error_test() -> R<()> {
        let file = write_token_file("  \n")?;
        let err = read_token_file(file.path()).expect_err("empty file");
        assert!(format!("{err:#}").contains("is empty"), "{err:#}");

        let err = read_token_file(std::path::Path::new("/nonexistent/legion/join-token"))
            .expect_err("missing file");
        assert!(
            format!("{err:#}").contains("/nonexistent/legion/join-token"),
            "{err:#}"
        );
        Ok(())
    }
}
