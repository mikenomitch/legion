//! Nickel evaluation — the one place Legion turns configuration source into
//! JSON.
//!
//! Every configuration surface Legion accepts is a [Nickel] program: job
//! specs (`legion job run web.ncl`), the manager and worker configs, ACL
//! policy files, and server-side job templates. They all funnel through this
//! module so that error reporting, the `vars` injection point, and the
//! sandbox rules stay identical everywhere.
//!
//! Three entry points cover the surfaces:
//!
//! - [`eval_file`] — evaluate a configuration file to JSON. Imports resolve
//!   relative to the file, as they do under `nickel export`.
//! - [`eval_file_tracked`] — the same, but also reports every source file the
//!   evaluation loaded, which the CLI stores alongside a submitted jobspec.
//! - [`eval_sandboxed`] — evaluate an in-memory snippet with imports rejected
//!   outright. Used by the manager for job templates, which must be
//!   self-contained because the manager renders them on behalf of a caller.
//!
//! All three accept an optional `vars` record which is bound as the free
//! identifier `vars` in the program's initial environment, next to `std`.
//! That is Legion's parameterization hook: `legion job run web.ncl --var
//! image=nginx` makes `vars.image` available to the jobspec. A program that
//! never mentions `vars` is unaffected.
//!
//! # JSON is accepted too
//!
//! Nickel is the language Legion is written for and documented in, but a
//! `.json` file is accepted for job specs, the manager config, worker
//! configs, and ACL policies, and is parsed as plain JSON rather than
//! evaluated. That exists for **machine-emitted config**: Terraform, CI, a
//! control plane, or any program that already has a JSON serializer and no
//! reason to grow a Nickel one. Job templates are the exception — a template
//! declares `job` as a *function* of its variables, which JSON cannot express,
//! so `legion template create` refuses a `.json` file outright.
//!
//! The dispatch is by file extension ([`format_for`]) and nothing else — no
//! content sniffing, so a file's name always tells you how it will be read.
//! JSON is *not* a subset of Nickel: records are
//! `key = value`, and a JSON object fed to the Nickel parser fails obscurely
//! (`:` is Nickel's type-annotation operator, so `{"a": 1}` reads as a field
//! annotated with the contract `1`). Dispatching on the extension is what
//! keeps that confusion off users.
//!
//! JSON has no variables and no imports, so a `.json` file with `--var` is an
//! error rather than a silent no-op. To *combine* the two — machine-generated
//! data with hand-written structure — import the JSON from Nickel, which
//! reads it natively by extension:
//!
//! ```text
//! let fleet = import "generated/fleet.json" in
//! { name = "web", tasks = [{ name = "server", runtime = "noop", count = fleet.count }] }
//! ```
//!
//! [Nickel]: https://nickel-lang.org

use crate::prelude::*;

use nickel_lang_core::error::report::{report_as_str, ColorOpt};
use nickel_lang_core::error::{Error as NickelError, PointedExportErrorData};
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::eval::value::NickelValue;
use nickel_lang_core::identifier::Ident;
use nickel_lang_core::parser::lexer::{Lexer, NormalToken, Token};
use nickel_lang_core::program::{Program, ProgramBuilder};
use nickel_lang_core::serialize::{self, ExportFormat};
use std::path::{Path, PathBuf};

/// The file extension Legion's configuration files use.
pub const CONFIG_EXTENSION: &str = "ncl";
/// The dotted form, for `ends_with` checks and error messages.
pub const CONFIG_SUFFIX: &str = ".ncl";

/// The file extension for pre-serialized JSON configuration.
pub const JSON_EXTENSION: &str = "json";

/// How a configuration file's bytes should be turned into JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    /// Evaluate as a Nickel program. The default for every extension that is
    /// not explicitly JSON, so an unrecognized name gets Legion's real
    /// configuration language and its error reporting.
    Nickel,
    /// Parse as plain JSON. No evaluation, no variables, no imports.
    Json,
}

/// Picks the format for a path from its extension alone.
///
/// Extension-only, deliberately: the alternative is sniffing the content,
/// which makes a file's meaning depend on its first non-space byte and turns
/// a typo in a Nickel record into "well, it didn't look like JSON". Answering
/// from the name means the failure is always reported in the language the
/// user named.
pub fn format_for(path: impl AsRef<Path>) -> ConfigFormat {
    match path.as_ref().extension().and_then(|ext| ext.to_str()) {
        // Case-insensitive so `FLEET.JSON` off a Windows-y toolchain works.
        Some(ext) if ext.eq_ignore_ascii_case(JSON_EXTENSION) => ConfigFormat::Json,
        _ => ConfigFormat::Nickel,
    }
}

/// Whether a path found by scanning a directory is a configuration file.
///
/// This is the *discovery* question, not the *parsing* one, and the two have
/// different answers on purpose: [`format_for`] reads anything it is handed
/// and defaults to Nickel, while a directory scan (the manager's worker-config
/// directory) must not try to parse `README.md` or an editor's `.swp` file
/// just because they share the directory.
pub fn is_config_file(path: impl AsRef<Path>) -> bool {
    matches!(
        path.as_ref().extension().and_then(|ext| ext.to_str()),
        Some(ext)
            if ext.eq_ignore_ascii_case(CONFIG_EXTENSION)
                || ext.eq_ignore_ascii_case(JSON_EXTENSION)
    )
}

/// The identifier the caller-supplied variables are bound to.
///
/// A jobspec reads `vars.image`; `std` and `vars` are the only free
/// identifiers Legion adds to a program's scope.
pub const VARS_IDENT: &str = "vars";

/// A source file an evaluation read.
///
/// The primary source is the file (or snippet) that was evaluated; the rest
/// are the files it imported, transitively.
#[derive(Debug, Clone)]
pub struct LoadedSource {
    pub path: String,
    pub content: String,
    pub is_primary: bool,
}

/// Builds the `vars` binding for a program's initial environment.
///
/// `None` and an empty map both bind `vars` to `{}`, so a program can always
/// mention `vars` without the caller having passed anything.
fn vars_env(vars: Option<&Map<String, Value>>) -> R<Vec<(Ident, NickelValue)>> {
    let object = Value::Object(vars.cloned().unwrap_or_default());
    let value: NickelValue = serde_json::from_value(object)
        .context("failed to convert configuration variables into a Nickel record")?;
    Ok(vec![(Ident::from(VARS_IDENT), value)])
}

/// Renders a Nickel error with source snippets, the way `nickel export` does.
///
/// Colors are off: these strings end up in gRPC statuses, JSON error
/// envelopes, and log lines as often as they end up on a terminal.
fn describe(program: &Program<CacheImpl>, error: NickelError) -> anyhow::Error {
    let mut files = program.files();
    let rendered = report_as_str(&mut files, error, ColorOpt::Never);
    anyhow!("{}", rendered.trim_end())
}

/// Exports an evaluated value as JSON.
///
/// Export errors carry a position that only means something against the
/// program's position table, so they are resolved through it before being
/// rendered.
fn export_json(program: &Program<CacheImpl>, value: &NickelValue) -> R<String> {
    let locate = |err: PointedExportErrorData| {
        describe(
            program,
            NickelError::from(err.with_pos_table(program.pos_table().clone())),
        )
    };
    serialize::validate(ExportFormat::Json, value).map_err(locate)?;
    serialize::to_string(ExportFormat::Json, value).map_err(locate)
}

/// Evaluates a built program to JSON.
///
/// `eval_full_for_export` is the `nickel export` evaluation mode: it forces
/// the whole value and drops fields marked `| not_exported`, which is how a
/// job template hides its `job` function from the manifest it publishes.
fn run(mut program: Program<CacheImpl>) -> R<String> {
    let value = program
        .eval_full_for_export()
        .map_err(|err| describe(&program, err))?;
    export_json(&program, &value)
}

/// Reads a `.json` configuration file.
///
/// The text is parsed and re-serialized rather than passed through, so every
/// caller downstream gets canonical JSON regardless of how the file was
/// formatted, and a malformed file fails here — with a line and column —
/// instead of somewhere in a `serde` deserializer that has lost the path.
fn read_json(path: &Path, vars: Option<&Map<String, Value>>) -> R<String> {
    // A JSON file cannot read `vars`, so accepting them silently would make
    // `--var` look like it worked. Only a non-empty set is an error: callers
    // pass an empty map unconditionally.
    if vars.is_some_and(|vars| !vars.is_empty()) {
        bail!(
            "{} is JSON, which has no variables — drop the variable flags, or use a \
             {CONFIG_SUFFIX} file that reads `vars` and imports the JSON",
            path.display()
        );
    }

    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let value: Value = serde_json::from_str(&text)
        .with_context(|| format!("{} is not valid JSON", path.display()))?;
    serde_json::to_string(&value)
        .with_context(|| format!("failed to re-serialize {}", path.display()))
}

/// Evaluates a configuration file to JSON.
///
/// A `.json` file is parsed as JSON; anything else is evaluated as Nickel,
/// with imports resolving relative to the file itself.
pub fn eval_file(path: impl AsRef<Path>) -> R<String> {
    let path = path.as_ref();
    eval_file_with_vars(path, None)
}

/// Evaluates a configuration file to JSON with `vars` bound in scope.
///
/// `vars` reaches a Nickel program as the free identifier `vars`; supplying a
/// non-empty set for a `.json` file is an error, since JSON cannot read it.
pub fn eval_file_with_vars(path: impl AsRef<Path>, vars: Option<&Map<String, Value>>) -> R<String> {
    let path = path.as_ref();
    if format_for(path) == ConfigFormat::Json {
        return read_json(path, vars);
    }
    let program = ProgramBuilder::new()
        .add_path(path)
        .extend_initial_env(vars_env(vars)?)
        .build::<CacheImpl>()
        .with_context(|| format!("failed to read {}", path.display()))?;
    run(program)
}

/// Evaluates a configuration file to JSON and reports every source it read.
///
/// The returned list always starts with `path` itself (`is_primary`), then
/// every file the program imported, transitively. `legion job run` stores
/// that set with the job so the exact source of a running job can be shown
/// back later. A `.json` spec has no imports, so it reports only itself.
pub fn eval_file_tracked(
    path: impl AsRef<Path>,
    vars: Option<&Map<String, Value>>,
) -> R<(String, Vec<LoadedSource>)> {
    let path = path.as_ref();
    if format_for(path) == ConfigFormat::Json {
        let json = read_json(path, vars)?;
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return Ok((
            json,
            vec![LoadedSource {
                path: path.to_string_lossy().to_string(),
                content,
                is_primary: true,
            }],
        ));
    }

    let mut program = ProgramBuilder::new()
        .add_path(path)
        .extend_initial_env(vars_env(vars)?)
        .build::<CacheImpl>()
        .with_context(|| format!("failed to read {}", path.display()))?;

    let value = program
        .eval_full_for_export()
        .map_err(|err| describe(&program, err))?;
    let json = export_json(&program, &value)?;

    Ok((json, loaded_sources(&program, path)))
}

/// Collects the on-disk files an evaluation touched.
///
/// Nickel's file table also holds the standard library and any generated
/// sources, so entries are kept only when they name a real file. The primary
/// path is emitted first and read from disk directly, so it is present even
/// if the program is trivial.
fn loaded_sources(program: &Program<CacheImpl>, primary: &Path) -> Vec<LoadedSource> {
    let mut sources = Vec::new();
    let canonical_primary =
        std::fs::canonicalize(primary).unwrap_or_else(|_| primary.to_path_buf());

    if let Ok(content) = std::fs::read_to_string(primary) {
        sources.push(LoadedSource {
            path: primary.to_string_lossy().to_string(),
            content,
            is_primary: true,
        });
    }

    let files = program.files();
    for name in files.filenames() {
        let candidate = PathBuf::from(name);
        if !candidate.is_file() {
            continue;
        }
        let canonical =
            std::fs::canonicalize(&candidate).unwrap_or_else(|_| candidate.to_path_buf());
        if canonical == canonical_primary {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&candidate) else {
            continue;
        };
        sources.push(LoadedSource {
            path: candidate.to_string_lossy().to_string(),
            content,
            is_primary: false,
        });
    }

    sources
}

/// Evaluates an in-memory Nickel snippet to JSON with imports rejected.
///
/// `name` labels the snippet in error messages. This is what the manager uses
/// for job templates: the manager evaluates them on a caller's behalf, so a
/// template that could `import "/etc/shadow"` would be a file-disclosure
/// primitive. Imports are refused before evaluation starts rather than
/// detected afterwards.
pub fn eval_sandboxed(name: &str, source: &str, vars: Option<&Map<String, Value>>) -> R<String> {
    reject_imports(source)?;
    let program = ProgramBuilder::new()
        .add_source_string(source.to_string(), name)
        .extend_initial_env(vars_env(vars)?)
        .build::<CacheImpl>()
        .context("failed to prepare the Nickel program")?;
    run(program)
}

/// Evaluates a Nickel expression given on the command line to a JSON value.
///
/// Backs `--var-code key=<expr>`: the expression is evaluated in isolation
/// (no imports, no `vars`) and the resulting value becomes one entry of the
/// `vars` record for the real evaluation.
pub fn eval_expression(expr: &str) -> R<Value> {
    let json = eval_sandboxed("<--var-code>", expr, None)?;
    serde_json::from_str(&json).context("expression did not evaluate to a JSON-representable value")
}

/// Rejects any use of the `import` keyword.
///
/// Lexing rather than string matching, so `"import"` inside a string literal
/// and `# import` inside a comment are not false positives. Lexical errors
/// are ignored here: the parser reports them with far better messages a
/// moment later.
fn reject_imports(source: &str) -> R<()> {
    for token in Lexer::new(source) {
        match token {
            Ok((_, Token::Normal(NormalToken::Import), _)) => {
                bail!("imports are not allowed here; the source must be self-contained")
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod config_lang_test {
    use super::*;

    #[test]
    fn evaluates_a_record_to_json_test() {
        let json = eval_sandboxed("t", "{ name = \"web\", count = 1 + 1 }", None).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["name"], "web");
        assert_eq!(value["count"], 2);
    }

    #[test]
    fn binds_vars_in_scope_test() {
        let mut vars = Map::new();
        vars.insert("image".into(), Value::String("nginx".into()));
        vars.insert("count".into(), Value::from(3));
        let json = eval_sandboxed(
            "t",
            "{ image = vars.image, count = vars.count * 2 }",
            Some(&vars),
        )
        .unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["image"], "nginx");
        assert_eq!(value["count"], 6);
    }

    #[test]
    fn vars_defaults_to_an_empty_record_test() {
        let json = eval_sandboxed("t", "{ n = std.record.length vars }", None).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["n"], 0);
    }

    #[test]
    fn drops_not_exported_fields_test() {
        let json = eval_sandboxed("t", "{ shown = 1, hidden | not_exported = 2 }", None).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["shown"], 1);
        assert!(value.get("hidden").is_none());
    }

    #[test]
    fn rejects_imports_in_sandboxed_sources_test() {
        let err = eval_sandboxed("t", "import \"other.ncl\"", None).unwrap_err();
        assert!(err.to_string().contains("imports are not allowed"), "{err}");
    }

    #[test]
    fn does_not_mistake_the_word_import_for_the_keyword_test() {
        let json = eval_sandboxed("t", "{ note = \"import me\" } # import\n", None).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["note"], "import me");
    }

    #[test]
    fn reports_contract_errors_with_source_context_test() {
        let err = eval_sandboxed("t", "{ n | String = 1 }", None).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("contract broken"), "{message}");
    }

    #[test]
    fn evaluates_command_line_expressions_test() {
        assert_eq!(eval_expression("[1, 2, 3]").unwrap(), json!([1, 2, 3]));
        assert_eq!(eval_expression("{ a = true }").unwrap(), json!({"a": true}));
        assert!(eval_expression("1 +").is_err());
    }

    #[test]
    fn picks_the_format_from_the_extension_test() {
        assert_eq!(format_for("job.json"), ConfigFormat::Json);
        assert_eq!(format_for("/a/b/FLEET.JSON"), ConfigFormat::Json);
        assert_eq!(format_for("job.ncl"), ConfigFormat::Nickel);
        // Unknown and missing extensions fall back to Legion's real
        // configuration language rather than being refused.
        assert_eq!(format_for("job.txt"), ConfigFormat::Nickel);
        assert_eq!(format_for("job"), ConfigFormat::Nickel);
    }

    #[test]
    fn recognizes_config_files_for_directory_scans_test() {
        assert!(is_config_file("worker.ncl"));
        assert!(is_config_file("worker.json"));
        // Discovery is stricter than parsing: a scan must not pick these up.
        assert!(!is_config_file("README.md"));
        assert!(!is_config_file(".worker.ncl.swp"));
        assert!(!is_config_file("worker"));
    }

    #[test]
    fn reads_json_config_files_test() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.json");
        std::fs::write(&path, "{\n  \"name\": \"web\",\n  \"count\": 2\n}\n").unwrap();

        let json = eval_file(&path).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["name"], "web");
        assert_eq!(value["count"], 2);
    }

    #[test]
    fn json_config_files_are_not_evaluated_as_nickel_test() {
        // The point of the extension dispatch: this exact text is a *Nickel*
        // error ("invalid contract expression", because `:` annotates a
        // contract there) and a perfectly good JSON document.
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("job.json");
        let ncl_path = dir.path().join("job.ncl");
        let source = "{\"name\": \"web\"}";
        std::fs::write(&json_path, source).unwrap();
        std::fs::write(&ncl_path, source).unwrap();

        let value: Value = serde_json::from_str(&eval_file(&json_path).unwrap()).unwrap();
        assert_eq!(value["name"], "web");
        assert!(eval_file(&ncl_path).is_err());
    }

    #[test]
    fn reports_malformed_json_with_a_position_test() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.json");
        std::fs::write(&path, "{\n  \"name\": \"web\",\n}\n").unwrap();

        let err = format!("{:#}", eval_file(&path).unwrap_err());
        assert!(err.contains("is not valid JSON"), "{err}");
        assert!(err.contains("line 3"), "{err}");
    }

    #[test]
    fn rejects_vars_for_json_config_files_test() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.json");
        std::fs::write(&path, "{\"name\": \"web\"}").unwrap();

        let mut vars = Map::new();
        vars.insert("image".into(), Value::String("nginx".into()));
        let err = eval_file_with_vars(&path, Some(&vars)).unwrap_err();
        assert!(err.to_string().contains("has no variables"), "{err}");

        // An empty set is what every caller passes unconditionally, so it
        // must not trip the check.
        assert!(eval_file_with_vars(&path, Some(&Map::new())).is_ok());
    }

    #[test]
    fn tracks_a_json_config_file_as_its_own_only_source_test() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.json");
        std::fs::write(&path, "{\"name\": \"web\"}").unwrap();

        let (json, sources) = eval_file_tracked(&path, None).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&json).unwrap()["name"], "web");
        assert_eq!(sources.len(), 1);
        assert!(sources[0].is_primary);
        assert_eq!(sources[0].content, "{\"name\": \"web\"}");
    }

    #[test]
    fn nickel_can_import_a_json_file_test() {
        // The combining path: machine-generated data, hand-written structure.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fleet.json"), "{\"memory\": 128}").unwrap();
        let main = dir.path().join("job.ncl");
        std::fs::write(
            &main,
            "let fleet = import \"fleet.json\" in { name = \"web\", memory = fleet.memory }",
        )
        .unwrap();

        let json = eval_file(&main).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["memory"], 128);
    }

    #[test]
    fn evaluates_files_and_tracks_imports_test() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("lib.ncl");
        std::fs::write(&lib, "{ memory = 128 }").unwrap();
        let main = dir.path().join("job.ncl");
        std::fs::write(
            &main,
            "let lib = import \"lib.ncl\" in { name = \"web\", memory = lib.memory }",
        )
        .unwrap();

        let (json, sources) = eval_file_tracked(&main, None).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["memory"], 128);

        assert_eq!(sources.len(), 2);
        assert!(sources[0].is_primary);
        assert!(sources[0].path.ends_with("job.ncl"));
        assert!(sources.iter().any(|s| s.path.ends_with("lib.ncl")));
    }
}
