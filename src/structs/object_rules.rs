//! CEL validation `rules` on app object kinds (app-object-kinds plan, Phase
//! O4; docs/app-objects.md, "Validation rules").
//!
//! A kind declares rules a stored spec must satisfy beyond its JSON Schema,
//! usually across fields:
//!
//! ```nickel
//! rules = [
//!   { rule = "self.min <= self.max", message = "min must not exceed max", path = "/spec/min" },
//!   { rule = "self.target.plugin == oldSelf.target.plugin", message = "the target plugin cannot change" },
//! ]
//! ```
//!
//! `self` is the effective spec (defaults filled); `oldSelf` is the stored
//! spec on an update, and a rule that mentions it is a *transition rule*,
//! skipped on create. The manager evaluates every rule on every apply
//! (dry runs too), after the schema and `immutable` checks and before
//! `requires`, and refuses the write when any fails, naming each.
//!
//! The engine is the `cel` crate (cel-rust). It has no cost estimator and
//! cannot be interrupted, so the cost is bounded here, by construction:
//!
//! - a rule's source is at most [`MAX_RULE_BYTES`], and its bracket nesting
//!   and conditionals are counted on the raw text before any parser sees it
//!   ([`MAX_RULE_NESTING`]); the parser is the recursion-limited Pratt
//!   parser;
//! - the parsed tree is at most [`MAX_RULE_AST_DEPTH`] deep (evaluation
//!   recurses once per level), and nests comprehensions (`all`, `exists`,
//!   `exists_one`, `map`, `filter`) at most [`MAX_COMPREHENSION_NESTING`]
//!   deep;
//! - every comprehension is rewritten at compile time to call a budget
//!   check on each iteration, which fails the rule once its deadline
//!   passes: the one super-linear construct stops on time;
//! - evaluation has a wall-clock budget, [`RULE_BUDGET`] per rule and
//!   [`RULES_BUDGET`] per write, checked after each rule whatever it
//!   returned; an overrun fails the write (fail closed). At apply the
//!   evaluation runs on a blocking thread under a timeout: work the budget
//!   check cannot stop (a long chain of list concatenations) finishes on
//!   that thread, but the write is refused at the timeout;
//! - the spec is at most 64 KiB and its values at most [`MAX_VALUE_DEPTH`]
//!   deep.

use crate::structs::app::digest_value;
use cel::common::ast::{operators, CallExpr, EntryExpr, Expr, IdedExpr, LiteralValue};
use cel::objects::{Key, Map as CelMap};
use cel::parser::PrattParser;
use cel::{Context, Env, ExecutionError, Value as CelValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Most rules one kind declares.
pub const MAX_RULES: usize = 32;
/// Longest rule source.
pub const MAX_RULE_BYTES: usize = 1024;
/// Longest rule `message`.
pub const MAX_RULE_MESSAGE_BYTES: usize = 512;
/// Deepest bracket nesting (`(`, `[`, `{`, counted on the raw source, string
/// literals included), and most conditionals (`? :`), in one rule.
pub const MAX_RULE_NESTING: usize = 32;
/// Deepest a rule's parsed expression may be. Evaluation recurses once per
/// level, so this bounds its stack.
pub const MAX_RULE_AST_DEPTH: usize = 24;
/// Deepest comprehensions (`all`, `exists`, `exists_one`, `map`, `filter`)
/// may nest in one rule.
pub const MAX_COMPREHENSION_NESTING: usize = 2;
/// How long one rule may run.
pub const RULE_BUDGET: Duration = Duration::from_millis(10);
/// How long all of a kind's rules may run, for one write.
pub const RULES_BUDGET: Duration = Duration::from_millis(50);
/// How much longer than [`RULES_BUDGET`] an apply waits for its blocking
/// evaluation thread before refusing the write anyway.
pub const RULES_GRACE: Duration = Duration::from_millis(50);
/// Deepest a spec value may nest to be checked (serde_json reads at most
/// 128 levels).
pub const MAX_VALUE_DEPTH: usize = 128;
/// The parser's own recursion limit (it reports an error past it).
const PARSER_RECURSION: u16 = 96;
/// Most nodes a parsed rule may have, macros expanded.
const MAX_RULE_NODES: usize = 4096;
/// The budget check every comprehension calls on each iteration. A CEL
/// identifier cannot start with `@`, so no rule can call or shadow it.
const BUDGET_FN: &str = "@legion_budget";
/// The two variables a rule may use.
pub const SELF_VAR: &str = "self";
pub const OLD_SELF_VAR: &str = "oldSelf";
/// The type names an expression may also name (`type(self.min) == int`).
const TYPE_NAMES: &[&str] = &[
    "bool",
    "bytes",
    "double",
    "dyn",
    "int",
    "list",
    "map",
    "null_type",
    "string",
    "type",
    "uint",
];
/// The CEL implementation and its version, for the docs and `explain`.
pub const CEL_ENGINE: &str = "cel-rust (cel 0.15.0)";

/// One rule a kind declares (`kinds[].rules[]`).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KindRule {
    /// A CEL expression that must be true: over `self`, the effective spec
    /// (defaults filled), and `oldSelf`, the stored spec. A rule that uses
    /// `oldSelf` is a transition rule, checked only on update. At most 1
    /// KiB.
    pub rule: String,
    /// What a failure says (`min must not exceed max`).
    pub message: String,
    /// A JSON pointer into the object (`/spec/min`) the failure is reported
    /// at; default the spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Not supported: a rule's message is fixed text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub message_expression: Option<Value>,
}

/// One rule, compiled.
#[derive(Debug)]
pub struct CompiledRule {
    /// The source as declared.
    pub rule: String,
    pub message: String,
    pub path: Option<String>,
    /// Where a failure is reported, dotted (`spec.min`; `spec` by default).
    pub at: String,
    /// Whether it uses `oldSelf`: checked on update only.
    pub transition: bool,
    /// The expression, with the budget check in every comprehension.
    program: IdedExpr,
}

/// A kind's rules, compiled (cached by their digest).
#[derive(Debug, Default)]
pub struct CompiledRules {
    pub rules: Vec<CompiledRule>,
}

/// The standard environment, built once and shared.
fn cel_env() -> Arc<Env> {
    static ENV: OnceLock<Arc<Env>> = OnceLock::new();
    ENV.get_or_init(|| Arc::new(Env::stdlib())).clone()
}

fn rules_cache() -> &'static Mutex<HashMap<String, Arc<CompiledRules>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<CompiledRules>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The most rule sets the cache keeps; past it, it starts over.
const RULES_CACHE_LIMIT: usize = 512;

/// `/spec/min` → `spec.min`.
fn dotted(pointer: &str) -> String {
    pointer.trim_start_matches('/').replace('/', ".")
}

/// Compile a kind's rules (cached by their digest): at most [`MAX_RULES`],
/// each parsing, using only `self` and `oldSelf`, not known to evaluate to
/// something other than a bool, within the cost bounds, with a message and
/// an optional `path` into the spec.
pub fn compile_rules(rules: &[KindRule]) -> Result<Arc<CompiledRules>, String> {
    if rules.is_empty() {
        static EMPTY: OnceLock<Arc<CompiledRules>> = OnceLock::new();
        return Ok(EMPTY.get_or_init(Arc::default).clone());
    }
    if rules.len() > MAX_RULES {
        return Err(format!(
            "rules: {} declared; a kind declares at most {MAX_RULES}",
            rules.len()
        ));
    }
    let digest = digest_value(&serde_json::to_value(rules).unwrap_or(Value::Null));
    if let Some(found) = rules_cache()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .get(&digest)
    {
        return Ok(found.clone());
    }
    let mut compiled = Vec::with_capacity(rules.len());
    for (n, rule) in rules.iter().enumerate() {
        let at = format!("rules[{n}]");
        if rule.message_expression.is_some() {
            return Err(format!(
                "{at}: messageExpression is not supported; write the failure as a fixed \
                 `message`"
            ));
        }
        let message = rule.message.trim();
        if message.is_empty() {
            return Err(format!(
                "{at}: `message` is required: what a person whose object fails the rule reads"
            ));
        }
        if message.len() > MAX_RULE_MESSAGE_BYTES {
            return Err(format!(
                "{at}: the message is {} bytes; at most {MAX_RULE_MESSAGE_BYTES}",
                message.len()
            ));
        }
        if let Some(path) = &rule.path {
            if path != "/spec" && !path.starts_with("/spec/") {
                return Err(format!(
                    "{at}: path {path:?} must be a JSON pointer into the spec (/spec/…), like \
                     every pointer in a kind's declaration"
                ));
            }
        }
        let (program, transition) = compile_rule(&rule.rule)
            .map_err(|err| format!("{at} ({}): {err}", short(&rule.rule)))?;
        compiled.push(CompiledRule {
            rule: rule.rule.clone(),
            message: message.to_string(),
            path: rule.path.clone(),
            at: rule
                .path
                .as_deref()
                .map(dotted)
                .unwrap_or_else(|| "spec".to_string()),
            transition,
            program,
        });
    }
    let compiled = Arc::new(CompiledRules { rules: compiled });
    let mut cache = rules_cache()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if cache.len() >= RULES_CACHE_LIMIT {
        cache.clear();
    }
    cache.insert(digest, compiled.clone());
    Ok(compiled)
}

/// A rule's source for a message, cut at 80 characters.
fn short(source: &str) -> String {
    let source = source.trim();
    if source.chars().count() > 80 {
        format!("{}…", source.chars().take(79).collect::<String>())
    } else {
        source.to_string()
    }
}

/// The deepest bracket nesting in `source`, and how many conditionals it
/// has, counted on the raw text (string literals included, which can only
/// over-count): what bounds the parser before it runs.
fn raw_nesting(source: &str) -> (usize, usize) {
    let (mut depth, mut deepest, mut conditionals) = (0usize, 0usize, 0usize);
    for c in source.chars() {
        match c {
            '(' | '[' | '{' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            '?' => conditionals += 1,
            _ => {}
        }
    }
    (deepest, conditionals)
}

/// What a walk of a parsed rule found.
#[derive(Default)]
struct Shape {
    /// The deepest level reached (the walk stops descending past the
    /// limit).
    depth: usize,
    /// The deepest comprehension nesting.
    comprehensions: usize,
    /// Variables used and not bound by a comprehension.
    free: BTreeSet<String>,
    /// Whether it builds a message (`Type{field: …}`), which no rule can.
    structs: bool,
}

fn walk(
    expr: &IdedExpr,
    depth: usize,
    comprehensions: usize,
    bound: &mut Vec<String>,
    shape: &mut Shape,
) {
    shape.depth = shape.depth.max(depth);
    shape.comprehensions = shape.comprehensions.max(comprehensions);
    if depth > MAX_RULE_AST_DEPTH {
        return;
    }
    let next = depth + 1;
    match &expr.expr {
        Expr::Unspecified | Expr::Literal(_) => {}
        Expr::Ident(name) => {
            if !name.starts_with('@')
                && !bound.iter().any(|b| b == name)
                && !TYPE_NAMES.contains(&name.as_str())
            {
                shape.free.insert(name.clone());
            }
        }
        Expr::Select(select) => walk(&select.operand, next, comprehensions, bound, shape),
        Expr::Call(call) => {
            if let Some(target) = &call.target {
                walk(target, next, comprehensions, bound, shape);
            }
            for arg in &call.args {
                walk(arg, next, comprehensions, bound, shape);
            }
        }
        Expr::List(list) => {
            for element in &list.elements {
                walk(element, next, comprehensions, bound, shape);
            }
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                match &entry.expr {
                    EntryExpr::MapEntry(entry) => {
                        walk(&entry.key, next, comprehensions, bound, shape);
                        walk(&entry.value, next, comprehensions, bound, shape);
                    }
                    EntryExpr::StructField(field) => {
                        walk(&field.value, next, comprehensions, bound, shape)
                    }
                }
            }
        }
        Expr::Struct(strct) => {
            shape.structs = true;
            for entry in &strct.entries {
                if let EntryExpr::StructField(field) = &entry.expr {
                    walk(&field.value, next, comprehensions, bound, shape);
                }
            }
        }
        Expr::Comprehension(comprehension) => {
            walk(
                &comprehension.iter_range,
                next,
                comprehensions,
                bound,
                shape,
            );
            walk(&comprehension.accu_init, next, comprehensions, bound, shape);
            let before = bound.len();
            bound.push(comprehension.iter_var.clone());
            if let Some(second) = &comprehension.iter_var2 {
                bound.push(second.clone());
            }
            bound.push(comprehension.accu_var.clone());
            for part in [
                &comprehension.loop_cond,
                &comprehension.loop_step,
                &comprehension.result,
            ] {
                walk(part, next, comprehensions + 1, bound, shape);
            }
            bound.truncate(before);
        }
    }
}

/// What a rule's top-level expression is known to evaluate to, when it is
/// known not to be a bool.
fn known_non_bool(expr: &IdedExpr) -> Option<&'static str> {
    match &expr.expr {
        Expr::Literal(LiteralValue::Boolean(_)) => None,
        Expr::Literal(LiteralValue::Null) => Some("null"),
        Expr::Literal(LiteralValue::String(_)) => Some("a string"),
        Expr::Literal(LiteralValue::Bytes(_)) => Some("bytes"),
        Expr::Literal(_) => Some("a number"),
        Expr::List(_) => Some("a list"),
        Expr::Map(_) => Some("a map"),
        Expr::Struct(_) => Some("a message"),
        Expr::Call(call) => match call.func_name.as_str() {
            operators::ADD
            | operators::SUBSTRACT
            | operators::MULTIPLY
            | operators::DIVIDE
            | operators::MODULO
            | operators::NEGATE => Some("a number, string, or list"),
            "size" | "int" | "uint" | "double" => Some("a number"),
            "string" => Some("a string"),
            "bytes" => Some("bytes"),
            _ => None,
        },
        Expr::Comprehension(comprehension) => match comprehension.accu_init.expr {
            // `map` and `filter` build a list.
            Expr::List(_) => Some("a list"),
            _ => None,
        },
        _ => None,
    }
}

fn budget_call(arg: IdedExpr) -> IdedExpr {
    IdedExpr {
        id: arg.id,
        expr: Expr::Call(CallExpr {
            func_name: BUDGET_FN.to_string(),
            target: None,
            args: vec![arg],
        }),
    }
}

/// Make every comprehension check the budget on each iteration: its loop
/// condition when it has one (`all`, `exists`), else the guard of its step
/// (`exists_one`, `filter`; `map` gets an always-true guard), which keeps
/// the evaluator's in-place append for `map` and `filter`. The check passes
/// its argument through until the deadline, and fails the rule after.
fn add_budget_checks(expr: &mut IdedExpr) {
    match &mut expr.expr {
        Expr::Unspecified | Expr::Literal(_) | Expr::Ident(_) => {}
        Expr::Select(select) => add_budget_checks(&mut select.operand),
        Expr::Call(call) => {
            if let Some(target) = &mut call.target {
                add_budget_checks(target);
            }
            for arg in &mut call.args {
                add_budget_checks(arg);
            }
        }
        Expr::List(list) => {
            for element in &mut list.elements {
                add_budget_checks(element);
            }
        }
        Expr::Map(map) => {
            for entry in &mut map.entries {
                match &mut entry.expr {
                    EntryExpr::MapEntry(entry) => {
                        add_budget_checks(&mut entry.key);
                        add_budget_checks(&mut entry.value);
                    }
                    EntryExpr::StructField(field) => add_budget_checks(&mut field.value),
                }
            }
        }
        Expr::Struct(strct) => {
            for entry in &mut strct.entries {
                if let EntryExpr::StructField(field) = &mut entry.expr {
                    add_budget_checks(&mut field.value);
                }
            }
        }
        Expr::Comprehension(comprehension) => {
            for part in [
                &mut comprehension.iter_range,
                &mut comprehension.accu_init,
                &mut comprehension.loop_cond,
                &mut comprehension.loop_step,
                &mut comprehension.result,
            ] {
                add_budget_checks(part);
            }
            if !matches!(comprehension.loop_cond.expr, Expr::Literal(_)) {
                let condition = std::mem::take(&mut comprehension.loop_cond);
                comprehension.loop_cond = budget_call(condition);
                return;
            }
            let step = std::mem::take(&mut comprehension.loop_step);
            let id = step.id;
            comprehension.loop_step = match step.expr {
                Expr::Call(mut call)
                    if call.func_name == operators::CONDITIONAL
                        && call.target.is_none()
                        && call.args.len() == 3 =>
                {
                    let guard = std::mem::take(&mut call.args[0]);
                    call.args[0] = budget_call(guard);
                    IdedExpr {
                        id,
                        expr: Expr::Call(call),
                    }
                }
                other => IdedExpr {
                    id,
                    expr: Expr::Call(CallExpr {
                        func_name: operators::CONDITIONAL.to_string(),
                        target: None,
                        args: vec![
                            budget_call(IdedExpr {
                                id,
                                expr: Expr::Literal(LiteralValue::Boolean(true.into())),
                            }),
                            IdedExpr { id, expr: other },
                            IdedExpr {
                                id,
                                expr: Expr::Ident(comprehension.accu_var.clone()),
                            },
                        ],
                    }),
                },
            };
        }
    }
}

/// Compile one rule: the expression with its budget checks, and whether it
/// is a transition rule.
pub fn compile_rule(source: &str) -> Result<(IdedExpr, bool), String> {
    if source.trim().is_empty() {
        return Err(
            "a rule is a CEL expression that must be true, like `self.min <= self.max`".into(),
        );
    }
    if source.len() > MAX_RULE_BYTES {
        return Err(format!(
            "is {} bytes; a rule is at most {MAX_RULE_BYTES}",
            source.len()
        ));
    }
    // Before any parser sees it: deep nesting is what makes a parser (or
    // the evaluator) recurse without bound.
    let (nesting, conditionals) = raw_nesting(source);
    if nesting > MAX_RULE_NESTING {
        return Err(format!(
            "nests brackets {nesting} deep; a rule nests at most {MAX_RULE_NESTING}"
        ));
    }
    if conditionals > MAX_RULE_NESTING {
        return Err(format!(
            "has {conditionals} conditionals (`? :`); a rule has at most {MAX_RULE_NESTING}"
        ));
    }
    let mut program = PrattParser::new()
        .max_recursion_depth(PARSER_RECURSION)
        .max_expression_node_count(MAX_RULE_NODES)
        .parse(source)
        .map_err(|errors| {
            let shown: Vec<String> = errors
                .errors
                .iter()
                .take(3)
                .map(|error| {
                    if error.pos.0 > 0 {
                        format!("{}:{}: {}", error.pos.0, error.pos.1, error.msg)
                    } else {
                        error.msg.clone()
                    }
                })
                .collect();
            format!("does not parse: {}", shown.join("; "))
        })?;
    let mut shape = Shape::default();
    walk(&program, 1, 0, &mut Vec::new(), &mut shape);
    if shape.depth > MAX_RULE_AST_DEPTH {
        return Err(format!(
            "nests more than {MAX_RULE_AST_DEPTH} levels deep; split it into several rules"
        ));
    }
    if shape.comprehensions > MAX_COMPREHENSION_NESTING {
        return Err(format!(
            "nests comprehensions (all, exists, exists_one, map, filter) {} deep; at most \
             {MAX_COMPREHENSION_NESTING}",
            shape.comprehensions
        ));
    }
    if shape.structs {
        return Err("builds a message (`Type{…}`); a rule reads `self` and `oldSelf` only".into());
    }
    if let Some(unknown) = shape
        .free
        .iter()
        .find(|name| *name != SELF_VAR && *name != OLD_SELF_VAR)
    {
        return Err(format!(
            "uses `{unknown}`; a rule reads only `self` (the spec, defaults filled) and \
             `oldSelf` (the stored spec, on update)"
        ));
    }
    if let Some(what) = known_non_bool(&program) {
        return Err(format!(
            "evaluates to {what}; a rule must evaluate to a bool (true passes)"
        ));
    }
    let transition = shape.free.contains(OLD_SELF_VAR);
    add_budget_checks(&mut program);
    Ok((program, transition))
}

/// A JSON value as CEL sees it: integers are `int` (`uint` past `i64`),
/// other numbers `double`, objects maps with string keys. Fails past
/// [`MAX_VALUE_DEPTH`].
fn to_cel(value: &Value, depth: usize) -> Result<CelValue, ()> {
    if depth > MAX_VALUE_DEPTH {
        return Err(());
    }
    Ok(match value {
        Value::Null => CelValue::Null,
        Value::Bool(boolean) => CelValue::Bool(*boolean),
        Value::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(int), _) => CelValue::Int(int),
            (None, Some(uint)) => CelValue::UInt(uint),
            _ => CelValue::Float(number.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(text) => CelValue::String(Arc::new(text.clone())),
        Value::Array(items) => CelValue::List(Arc::new(
            items
                .iter()
                .map(|item| to_cel(item, depth + 1))
                .collect::<Result<_, _>>()?,
        )),
        Value::Object(map) => {
            let mut out = HashMap::with_capacity(map.len());
            for (key, value) in map {
                out.insert(
                    Key::String(Arc::new(key.clone())),
                    to_cel(value, depth + 1)?,
                );
            }
            CelValue::Map(CelMap { map: Arc::new(out) })
        }
    })
}

/// An evaluation error, without the values it may carry (a `writeOnly`
/// value must never reach an error): a missing key is named only when the
/// rule's own text names it.
fn describe(error: &ExecutionError, source: &str) -> String {
    match error {
        ExecutionError::NoSuchKey(key) if source.contains(key.as_str()) => {
            format!("no such key: {key}")
        }
        ExecutionError::NoSuchKey(_) => "a key the spec does not have".to_string(),
        ExecutionError::UndeclaredReference(name) => format!("undeclared reference to {name}"),
        ExecutionError::NoSuchOverload(overload) => overload.to_string(),
        ExecutionError::UnexpectedType { got, want } => format!("got {got}, want {want}"),
        ExecutionError::InvalidArgumentCount { expected, actual } => {
            format!("expected {expected} arguments, got {actual}")
        }
        ExecutionError::FunctionError { function, .. } => format!("{function} failed"),
        ExecutionError::DivisionByZero(_) => "division by zero".to_string(),
        ExecutionError::RemainderByZero(_) => "remainder by zero".to_string(),
        ExecutionError::ValuesNotComparable(..) => "values that cannot be compared".to_string(),
        _ => "an unsupported operation".to_string(),
    }
}

impl CompiledRules {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Check `spec` (`self`, defaults filled) against every rule, and, when
    /// `old` is the stored spec of an update, the transition rules against
    /// it (`oldSelf`). One line per failing rule (`spec.min: min must not
    /// exceed max`), every one of them; a rule that errors, does not
    /// evaluate to a bool, or runs past its budget fails (fail closed). It
    /// runs on the calling thread: [`evaluate_bounded`] runs it on a
    /// blocking thread under a timeout.
    pub fn evaluate(&self, spec: &Value, old: Option<&Value>) -> Result<(), Vec<String>> {
        if !self
            .rules
            .iter()
            .any(|rule| !rule.transition || old.is_some())
        {
            return Ok(());
        }
        let too_deep = || {
            vec![format!(
                "spec: cannot be checked against the kind's rules: it nests more than \
                 {MAX_VALUE_DEPTH} levels deep"
            )]
        };
        let mut context = Context::with_env(cel_env());
        context.add_variable_from_value(SELF_VAR, to_cel(spec, 0).map_err(|_| too_deep())?);
        if let Some(old) = old {
            context.add_variable_from_value(OLD_SELF_VAR, to_cel(old, 0).map_err(|_| too_deep())?);
        }
        // The budget check reads the current rule's deadline, as nanoseconds
        // after `base`.
        let base = Instant::now();
        let deadline = Arc::new(AtomicU64::new(u64::MAX));
        let reads = deadline.clone();
        if context
            .add_function(BUDGET_FN, move |value: CelValue| {
                if base.elapsed().as_nanos() as u64 > reads.load(Ordering::Relaxed) {
                    Err(ExecutionError::function_error(
                        "budget",
                        "the rule ran past its time budget",
                    ))
                } else {
                    Ok(value)
                }
            })
            .is_err()
        {
            return Err(vec![
                "spec: the kind's rules could not be set up".to_string()
            ]);
        }
        let all_by = base + RULES_BUDGET;
        let mut failures = Vec::new();
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.transition && old.is_none() {
                continue;
            }
            let start = Instant::now();
            if start >= all_by {
                failures.push(format!(
                    "{}: not checked: the kind's rules ran past their {} ms budget (rules[{index}] \
                     and later); the write is refused",
                    rule.at,
                    RULES_BUDGET.as_millis()
                ));
                break;
            }
            let by = (start + RULE_BUDGET).min(all_by);
            deadline.store((by - base).as_nanos() as u64, Ordering::Relaxed);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                CelValue::resolve(&rule.program, &context)
            }));
            if Instant::now() > by {
                failures.push(format!(
                    "{}: {} (not checked: rules[{index}] ran past its {} ms budget, so the write \
                     is refused)",
                    rule.at,
                    rule.message,
                    RULE_BUDGET.as_millis()
                ));
                continue;
            }
            match result {
                Ok(Ok(CelValue::Bool(true))) => {}
                Ok(Ok(CelValue::Bool(false))) => {
                    failures.push(format!("{}: {}", rule.at, rule.message))
                }
                Ok(Ok(other)) => failures.push(format!(
                    "{}: {} (rules[{index}] evaluated to {}, not a bool)",
                    rule.at,
                    rule.message,
                    other.type_of()
                )),
                Ok(Err(error)) => failures.push(format!(
                    "{}: {} (rules[{index}] could not be evaluated: {})",
                    rule.at,
                    rule.message,
                    describe(&error, &rule.rule)
                )),
                Err(_) => failures.push(format!(
                    "{}: {} (rules[{index}] could not be evaluated)",
                    rule.at, rule.message
                )),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

/// [`CompiledRules::evaluate`] on a blocking thread, waiting at most
/// [`RULES_BUDGET`] plus [`RULES_GRACE`]. Past that the write is refused,
/// though the thread finishes what it started: the budget check stops a
/// comprehension at its deadline, but not work between checks.
pub async fn evaluate_bounded(
    rules: Arc<CompiledRules>,
    spec: Value,
    old: Option<Value>,
) -> Result<(), Vec<String>> {
    if rules.is_empty() {
        return Ok(());
    }
    let task = tokio::task::spawn_blocking(move || rules.evaluate(&spec, old.as_ref()));
    match tokio::time::timeout(RULES_BUDGET + RULES_GRACE, task).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(_)) => Err(vec![
            "spec: the kind's rules could not be evaluated; the write is refused".to_string(),
        ]),
        Err(_) => Err(vec![format!(
            "spec: the kind's rules did not finish within their {} ms budget; the write is \
             refused (rules fail closed)",
            RULES_BUDGET.as_millis()
        )]),
    }
}

#[cfg(test)]
mod object_rules_tests {
    use super::*;
    use serde_json::json;

    fn rule(source: &str, message: &str) -> KindRule {
        KindRule {
            rule: source.to_string(),
            message: message.to_string(),
            path: None,
            message_expression: None,
        }
    }

    fn compiled(rules: &[KindRule]) -> Arc<CompiledRules> {
        compile_rules(rules).expect("the rules compile")
    }

    fn refusal(source: &str) -> String {
        compile_rule(source).expect_err(source)
    }

    #[test]
    fn rules_compile_and_evaluate_test() {
        let rules = compiled(&[
            KindRule {
                path: Some("/spec/min".into()),
                ..rule("self.min <= self.max", "min must not exceed max")
            },
            rule("self.max >= 1", "max must be at least 1"),
        ]);
        assert!(!rules.rules[0].transition);
        assert_eq!(rules.rules[0].at, "spec.min");
        assert_eq!(rules.rules[1].at, "spec");
        assert!(rules.evaluate(&json!({ "min": 1, "max": 3 }), None).is_ok());
        // Every failing rule is reported, with its path.
        assert_eq!(
            rules.evaluate(&json!({ "min": 4, "max": 0 }), None),
            Err(vec![
                "spec.min: min must not exceed max".to_string(),
                "spec: max must be at least 1".to_string(),
            ])
        );
        // A rule that errors fails, saying why, without values.
        let missing = rules
            .evaluate(&json!({ "min": 1 }), None)
            .expect_err("no max");
        assert!(
            missing[0].contains("could not be evaluated: no such key: max"),
            "{missing:?}"
        );
        // The cache hands back the same compiled rules.
        assert!(Arc::ptr_eq(
            &rules,
            &compiled(&[
                KindRule {
                    path: Some("/spec/min".into()),
                    ..rule("self.min <= self.max", "min must not exceed max")
                },
                rule("self.max >= 1", "max must be at least 1"),
            ])
        ));
    }

    #[test]
    fn transition_rules_run_on_update_only_test() {
        let rules = compiled(&[rule(
            "self.target.plugin == oldSelf.target.plugin",
            "the target plugin cannot change",
        )]);
        assert!(rules.rules[0].transition);
        let next = json!({ "target": { "plugin": "b" } });
        // Skipped on create.
        assert!(rules.evaluate(&next, None).is_ok());
        assert!(rules
            .evaluate(&next, Some(&json!({ "target": { "plugin": "b" } })))
            .is_ok());
        assert_eq!(
            rules.evaluate(&next, Some(&json!({ "target": { "plugin": "a" } }))),
            Err(vec!["spec: the target plugin cannot change".to_string()])
        );
    }

    #[test]
    fn the_cel_subset_test() {
        let spec = json!({
            "min": 1, "max": 10, "tolerance": 0, "name": "web",
            "labels": { "team": "a" },
            "checks": [{ "name": "q", "source": "legion" }, { "name": "p", "source": "prom", "query": "x" }],
        });
        for source in [
            "self.checks.all(c, self.checks.exists_one(d, d.name == c.name))",
            "self.checks.all(c, c.source != 'legion' || !has(c.query) || c.query == '')",
            "self.tolerance >= 0.0 && self.tolerance < 1.0",
            "'team' in self.labels && !('x' in self.labels)",
            "self.name.startsWith('w') && self.name.endsWith('b') && self.name.contains('e')",
            "size(self.checks.map(c, c.name)) == 2 && size(self.checks.filter(c, has(c.query))) == 1",
            "self.checks.exists(c, c.name == 'q')",
            "type(self.min) == int",
            "has(self.labels.team) && !has(self.labels.owner)",
            "self.max - self.min == 9",
            "int('5') == 5 && string(5) == '5' && double(1) == 1.0 && uint(1) == 1u",
        ] {
            let rules = compiled(&[rule(source, "fails")]);
            assert_eq!(rules.evaluate(&spec, None), Ok(()), "{source}");
        }
    }

    #[test]
    fn compile_refusals_test() {
        assert!(refusal("self.min <=").contains("does not parse"));
        assert!(refusal("").contains("CEL expression"));
        assert!(refusal("spec.min <= 1").contains("uses `spec`"));
        assert!(refusal("self.items.all(x, x > other)").contains("uses `other`"));
        // A comprehension's variables are its own.
        assert!(compile_rule("self.items.all(x, x > 0)").is_ok());
        assert!(refusal("self.min + 1").contains("must evaluate to a bool"));
        assert!(refusal("size(self.items)").contains("must evaluate to a bool"));
        assert!(refusal("self.items.map(x, x)").contains("must evaluate to a bool"));
        assert!(refusal("'yes'").contains("must evaluate to a bool"));
        assert!(
            refusal(&format!("self.a == '{}'", "x".repeat(MAX_RULE_BYTES)))
                .contains("at most 1024")
        );
        assert!(
            refusal("self.a.all(x, self.a.all(y, self.a.all(z, x == y && y == z)))")
                .contains("nests comprehensions")
        );
        assert!(compile_rule("self.a.all(x, self.a.all(y, x == y || x != y))").is_ok());
        let deep = format!("self.a{} == 1", ".b".repeat(30));
        assert!(refusal(&deep).contains("levels deep"), "{}", refusal(&deep));
        let sum = format!("{} == 1", vec!["self.a"; 100].join(" + "));
        let refused = refusal(&sum);
        assert!(
            refused.contains("levels deep") || refused.contains("recursion"),
            "{refused}"
        );
        // A sum within the depth limit compiles.
        assert!(compile_rule(&format!("{} > 0", vec!["self.a"; 16].join(" + "))).is_ok());
        let ternaries = format!("{}true", "true ? true : ".repeat(40));
        assert!(refusal(&ternaries).contains("conditionals"));

        let decl = |edit: fn(&mut KindRule)| {
            let mut decl = rule("self.a == 1", "a must be 1");
            edit(&mut decl);
            compile_rules(&[decl]).expect_err("refused")
        };
        assert!(decl(|r| r.message = " ".into()).contains("`message` is required"));
        assert!(decl(|r| r.path = Some("/min".into())).contains("into the spec"));
        assert!(decl(|r| r.message_expression = Some(json!("'x'"))).contains("messageExpression"));
        let too_many: Vec<KindRule> = (0..=MAX_RULES).map(|_| rule("true", "x")).collect();
        assert!(compile_rules(&too_many)
            .expect_err("too many")
            .contains("at most 32"));
    }

    /// A 500-deep nested rule (about 1 KiB) aborted the process in the
    /// ANTLR parser cel 0.10 shipped, on a 2 MiB stack: it is refused on
    /// its raw text, before any parser sees it.
    #[test]
    fn a_deeply_nested_rule_never_reaches_the_parser_test() {
        for (open, close, depth) in [("(", ")", 500), ("[", "]", 500), ("{1:", "}", 250)] {
            let source = format!("{}1{} == 1", open.repeat(depth), close.repeat(depth));
            assert!(source.len() <= MAX_RULE_BYTES, "within the size limit");
            let refused = std::thread::Builder::new()
                .stack_size(2 << 20)
                .spawn(move || compile_rule(&source).expect_err("too deep"))
                .expect("thread")
                .join()
                .expect("no crash");
            assert!(refused.contains("nests brackets"), "{refused}");
        }
        // Nesting without brackets: the parser folds pairs of `!` and `-`,
        // and the depth check refuses a long chain; nothing crashes.
        for source in [
            format!("{}true", "!".repeat(1000)),
            format!("{}1 == 1", "-".repeat(1000)),
            format!("{}true", "!".repeat(999)),
        ] {
            std::thread::Builder::new()
                .stack_size(2 << 20)
                .spawn(move || compile_rule(&source).map(|_| ()))
                .expect("thread")
                .join()
                .expect("no crash")
                .ok();
        }
        let chain = format!("{} == 1", vec!["1"; 400].join("+"));
        let refused = std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || compile_rule(&chain).expect_err("too deep"))
            .expect("thread")
            .join()
            .expect("no crash");
        assert!(
            refused.contains("levels deep") || refused.contains("recursion"),
            "{refused}"
        );
    }

    /// The spec is at most 128 levels deep as JSON reads it; a deeper value
    /// built in memory is refused, not evaluated, and neither crashes.
    #[test]
    fn a_deeply_nested_spec_does_not_crash_test() {
        let rules = compiled(&[rule("self == self", "equal")]);
        let mut fits = json!(1);
        for _ in 0..120 {
            fits = json!({ "a": fits });
        }
        let mut deep = json!(1);
        for _ in 0..1000 {
            deep = json!({ "a": deep });
        }
        let outcome = std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || (rules.evaluate(&fits, None), rules.evaluate(&deep, None)))
            .expect("thread")
            .join()
            .expect("no crash");
        assert_eq!(outcome.0, Ok(()));
        assert!(outcome.1.expect_err("too deep")[0].contains("nests more than 128"));
    }

    /// Nested comprehensions over a large list stop at the rule's budget
    /// (the budget check), and fail closed even when the rule would absorb
    /// the error (`|| true`).
    #[test]
    fn an_expensive_rule_runs_out_of_budget_test() {
        let items: Vec<i64> = (0..20_000).collect();
        let spec = json!({ "items": items });
        for source in [
            "self.items.all(x, self.items.all(y, x == y || x != y))",
            "self.items.all(x, self.items.all(y, x == y || x != y)) || true",
            "size(self.items.map(x, self.items.filter(y, y > x))) > 0",
            "self.items.exists_one(x, self.items.exists(y, y < 0))",
        ] {
            let rules = compiled(&[rule(source, "expensive")]);
            let start = Instant::now();
            let failures = rules.evaluate(&spec, None).expect_err(source);
            assert!(
                failures[0].contains("ran past its 10 ms budget"),
                "{source}: {failures:?}"
            );
            // Stopped near the budget, not after 400 million iterations.
            assert!(start.elapsed() < Duration::from_secs(2), "{source}");
        }
    }

    #[tokio::test]
    async fn evaluation_is_bounded_on_a_blocking_thread_test() {
        let rules = compiled(&[rule("self.a == 1", "a must be 1")]);
        assert_eq!(
            evaluate_bounded(rules.clone(), json!({ "a": 1 }), None).await,
            Ok(())
        );
        assert_eq!(
            evaluate_bounded(rules, json!({ "a": 2 }), None).await,
            Err(vec!["spec: a must be 1".to_string()])
        );
    }

    /// Typical rules cost microseconds; printed for the docs.
    #[test]
    fn typical_rules_are_cheap_test() {
        let rules = compiled(&[
            rule("self.min <= self.max", "x"),
            rule("self.target.plugin == oldSelf.target.plugin", "x"),
            rule(
                "self.checks.all(c, self.checks.exists_one(d, d.name == c.name))",
                "x",
            ),
        ]);
        let spec = json!({
            "min": 1, "max": 10, "target": { "plugin": "noop" },
            "checks": [{ "name": "a" }, { "name": "b" }, { "name": "c" }],
        });
        let start = Instant::now();
        for _ in 0..100 {
            rules.evaluate(&spec, Some(&spec)).expect("passes");
        }
        let each = start.elapsed() / 100;
        eprintln!("three rules over a policy-sized spec: {each:?} per evaluation");
        assert!(each < RULES_BUDGET);
    }
}
