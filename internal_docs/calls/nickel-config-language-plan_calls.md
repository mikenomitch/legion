# Judgement Calls — Jsonnet → Nickel Configuration Language

Plan: [`../plans/nickel-config-language-plan.md`](../plans/nickel-config-language-plan.md)

Entries are ranked by project impact, highest first; the entry number is its
rank.

---

## 1. One free identifier, `vars`, replaces both of Jsonnet's injection mechanisms

**The call.** Every configuration file is evaluated with exactly one extra
identifier in scope besides `std`: a record named `vars`, always bound
(defaulting to `{}`), built from four CLI flags.

```bash
legion job run web.ncl --var image=nginx --var-code replicas=3
```

```nickel
{ name = "web", task = { config = { image = vars.image }, count = vars.replicas } }
```

Jsonnet had two orthogonal mechanisms — external variables (`std.extVar`) and
top-level arguments (a file that evaluates to a function) — each with a string
and a code form, each with a KV-backed variant. Eight flags. Nickel has neither
concept, so the shape was genuinely open.

**The alternatives.**
- **Files-as-functions**, the direct TLA analogue: a jobspec may evaluate to
  `fun vars => {…}` and the CLI applies it. Reads naturally, and it is what a
  Jsonnet user would reach for.
- **Nickel's own idiom, `--override field=value`**, which `nickel eval`
  provides: no injected identifier at all, the file declares `| default`
  fields and the CLI overrides them by path.
- **Both `vars` and files-as-functions**, accepting either.

**The trade-off.** `vars` is a free identifier Legion invents — a Nickel user
reading a Legion jobspec cannot evaluate it with the stock `nickel` CLI without
supplying it. `--override` would have kept files evaluable by any Nickel
tooling. It also means a spec that misspells `vars.imagee` fails at evaluation
rather than being caught by a schema.

**Why.** Files-as-functions was rejected because it makes a jobspec's *type*
depend on whether it takes parameters: a plain record and a function need
different handling in `job run`, `job plan`, `job validate`, imports, and the
template path. `--override` was rejected because it only reaches fields that
already exist in the output — you cannot use an override to pick a task's
image *and* derive three other fields from it, which is the actual use.
Supporting both was rejected on simplicity grounds: two ways to do the same
thing is the thing this migration was removing. One identifier, four flags,
and defaults expressed in the language itself (`{ replicas | Number | default =
1 } & vars`) puts the defaulting rules in one place — the config, not the CLI.

---

## 2. JSON is accepted alongside Nickel, dispatched on the file extension alone

**The call.** A `.json` file is read as plain JSON on four of the five config
surfaces — job specs, manager config, worker configs, ACL policies — and the
choice is made from the file extension, with no content sniffing and no flag.
`--var` against a `.json` file is a hard error rather than a silent no-op. Job
templates stay Nickel-only: a template declares `job` as a *function* of its
variables, so there is nothing for JSON to express, and `legion template create`
says that rather than letting the parser report a syntax error.

**The alternatives.**
- **Nickel only.** Generators emit Nickel, or pipe their JSON through `nickel
  export`.
- **Sniff the content** (leading `{` plus a successful `serde_json` parse) and
  pick a reader, so any extension works.
- **An explicit `--format json` flag** rather than inferring from the name.
- **Accept `vars` for JSON too** and ignore them, so the flags are uniform
  across both languages.

**The trade-off.** Two accepted languages is a second surface to document,
test, and keep in sync forever, and it weakens "every Legion config is a Nickel
program" — the sentence the migration was written to be able to say. A user who
names a Nickel file `.json` gets a parse error that does not mention Nickel.

**Why.** JSON is *not* a subset of Nickel, which is the fact that forces this.
Under Jsonnet it was — renaming `.json` to `.jsonnet` just worked, so machine
emitters were served for free and nobody had to decide anything. Under Nickel,
records are `key = value` and `:` is the type-annotation operator, so a
generated JSON object does not fail cleanly: `{"name": "web"}` parses as a
field annotated with the contract `"web"` and dies as `invalid contract
expression`. Removing the C++ VM would otherwise have quietly cost every
Terraform module, CI script, and agent a hand-written Nickel emitter, to
express data they already had a serializer for. Nickel's abstractions —
functions, imports, defaults, contracts — are for people; a generator has its
own and needs none of them.

Extension dispatch over sniffing because sniffing makes a file's meaning depend
on its first non-space byte: a typo'd Nickel record that happens to start with
`{` would be reported as bad JSON, in a language the author never chose. The
name is a declaration, and it makes the failure legible. Extension over a
`--format` flag because the flag would have to be threaded through every
surface, including the manager's worker-config directory scan, which has no
command line to put it on.

Erroring on `--var` rather than ignoring it: a dropped `--var image=nginx`
would submit a *working* job with the wrong image, which is the worst available
outcome. The error names the fix.

```
$ legion job run fleet.json --var image=nginx
error: fleet.json is JSON, which has no variables — drop the variable flags,
       or use a .ncl file that reads `vars` and imports the JSON
```

The documented way to have both is Nickel's native JSON import (`let fleet =
import "generated/fleet.json"`), which keeps generated data generated and
hand-written structure hand-written.

---

## 3. Template imports are rejected by lexing the source, not by an import hook

**The call.** `config_lang::eval_sandboxed` tokenizes the source with Nickel's
own lexer and rejects any `import` keyword *before* evaluation begins:

```
imports are not allowed here; the source must be self-contained
```

**The alternatives.**
- **An import-resolver hook**, which is what the Jsonnet implementation used
  (`vm.import_callback(|…| Err(…))`). Clean, and it is the mechanism a sandbox
  wants.
- **Detection after the fact** — evaluate, then check the file table for
  anything outside the snippet.
- **Evaluating in an empty temp directory** so relative imports fail
  file-not-found.

**The trade-off.** A lexical check is coarser than a semantic one: it rejects
the keyword wherever it appears, so a template can never import even a
hypothetical safe path. It also means the check has to be kept in step with
Nickel's grammar — if a future Nickel spells imports differently, the check
goes stale silently.

**Why.** `nickel-lang-core`'s `Program` has no import hook, and imports are
resolved *eagerly during program preparation*, not lazily at evaluation. That
kills the after-the-fact option outright: by the time you can inspect the file
table, `/etc/shadow` has already been read and can already be in the rendered
output. The empty-temp-directory idea fails on absolute paths for the same
reason. Lexing is the only option that refuses before any file is opened, and
using the real lexer rather than a substring search means `{ note = "import me"
}` and `# import` are not false positives — there is a test for exactly that.
This is a security boundary (a template author must not become a manager
operator), so the conservative version wins over the precise one.

---

## 4. No `.jsonnet` compatibility path at all

**The call.** The Jsonnet evaluator is deleted, `.jsonnet` files are not
recognized, and the `job_template_version.source_jsonnet` column is renamed in
the create-table migration rather than migrated.

**The alternatives.**
- **Dual evaluators dispatched on extension** for a release, with a deprecation
  warning.
- **Keep the column name**, rename only the proto/UI field.

**The trade-off.** An operator upgrading a live cluster loses their stored
template sources and has to re-publish them, and every `.jsonnet` file in their
repos stops working at once. There is no gradual path.

**Why.** The user waived backwards compatibility explicitly, twice. Given that,
dual evaluators would have kept the C++ dependency — the main thing the
migration was removing — and doubled the documentation surface for a
transitional benefit nobody asked for. Renaming the column in place rather than
adding a migration step is the same reasoning applied to storage: an
`ALTER TABLE … RENAME COLUMN` step would exist forever to serve a window that
was declared irrelevant.

---

## 5. A local jobspec is evaluated where it sits, rather than staged into a temp directory

**The call.** `legion job run/plan/validate` reads a local spec from its own
path. Only remote sources (`https://`, `git::`, `s3://`) are fetched into a
temp directory. `file://` counts as local.

**The alternatives.**
- **Copy the spec's parent directory** into the temp dir, preserving the
  staging model while making sibling imports resolvable.
- **Walk the import graph** and copy only the files it names.
- **Point Nickel's import resolver at the original directory** while still
  evaluating the copy.

**The trade-off.** The code path is no longer uniform — a local spec and a
remote one are resolved differently, and a future feature that assumes "the
spec always lives in a scratch directory we own" has to learn otherwise.

**Why.** Staging a *single file* was the bug: `import "lib/task-a.ncl"`
resolves against the importing file's directory, so copying the entry point
alone into an otherwise empty temp dir guarantees `could not find import`. The
shipped `configs/jobs/demo-imports-2.ncl` did not run. This predates the
migration — Jsonnet's import callback `unwrap()`ed the same failed read and
panicked — so the copy never served local specs at all; it existed for remote
ones and local paths fell through it by accident.

Copying the parent directory would fix the symptom and import a new problem:
jobspecs live in repositories, and the parent directory is as often a
1 GB checkout as a tidy `configs/jobs/`. Walking the import graph means
reimplementing Nickel's resolver, including its JSON/YAML/TOML extension
handling, and being wrong about it later. Local files are already readable by
the process — staging them bought nothing to begin with.

---

## 6. Library fragments moved to `configs/jobs/lib/` rather than given a distinguishing name

**The call.** `task-a.libsonnet` / `task-b.libsonnet` — importable task records,
not jobspecs — became `configs/jobs/lib/task-a.ncl` and `lib/task-b.ncl`.

**The alternatives.**
- **A naming convention** (`_task-a.ncl`, `task-a.lib.ncl`) with the test
  skipping matching names.
- **Leaving them in `configs/jobs/`** and teaching `config_jobs_parse` to skip
  files that do not deserialize as a jobspec.

**The trade-off.** The import path in `demo-imports-2.ncl` grows a directory
(`import "lib/task-a.ncl"`), and a reader has to notice the directory to know
what the file is. Jsonnet's `.libsonnet` convention said it in the extension.

**Why.** Nickel has one extension, so the information has to live somewhere
else. A directory is a stronger signal than a filename prefix and needs no
special-casing anywhere: `read_dir` is not recursive, so
`config_jobs_parse` skips `lib/` for free. Teaching the test to ignore
non-deserializing files was rejected outright — that is exactly the failure the
test exists to catch.

---

## 7. Errors are rendered with Nickel's own diagnostic reporter, colorless

**The call.** Every evaluation failure goes through
`error::report::report_as_str(..., ColorOpt::Never)` and becomes an
`anyhow::Error` carrying the full snippet:

```
error: contract broken by a value
  ┌─ web.ncl:3:11
  │
3 │   count | String = 1,
  │           ------   ^ applied to this expression
```

**The alternatives.**
- **`err.to_string()`**, a one-line message — what the Jsonnet path did.
- **Colored output**, detecting a TTY.

**The trade-off.** Multi-line errors travel badly: they land in gRPC status
messages, JSON error envelopes, and structured log fields, where a five-line
string with box-drawing characters is awkward. A one-line message would embed
more cleanly everywhere.

**Why.** "The error does not say where" was the second of the three problems
that motivated the migration; keeping `to_string()` would have shipped the
migration without its main user-visible benefit. Colors were rejected because
the same string reaches a terminal, a JSON field, and a log line, and only one
of those wants ANSI escapes — TTY detection at the point where the error is
*constructed* is wrong, because that is not where it is displayed.

---

## 8. KV-backed variables are threaded through `std.deserialize`, not pasted as source

**The call.** `--kv-var-code key=path` renders the KV entry as
`std.deserialize 'Json "<escaped json>"` rather than inlining the value as
Nickel source.

**The alternatives.**
- **Inline the JSON**, which is what the Jsonnet implementation did — JSON is
  valid Jsonnet, so `vm.ext_code(key, json)` was free.
- **Build a `NickelValue` directly** from the JSON and inject it, as the plain
  `--var` path does.

**The trade-off.** A tiny amount of parsing at evaluation time, and a string
escaper (`nickel_string_literal`) that has to get Nickel's escapes right —
including `%`, which starts interpolation and which JSON escaping does not
touch.

**Why.** JSON is *not* valid Nickel: records use `=`, not `:`. Inlining is
simply wrong. Direct `NickelValue` construction would have been the cleanest,
but `--var-code` values are expressions that must be *evaluated* anyway, and
KV values flow through the same `code` list — one path is simpler than two, and
`std.deserialize` is the language's own answer for "this string is JSON". The
escaper has a dedicated test for `%{` because getting it wrong turns a KV value
into an injection.
