//! `plugin` — run a stored plugin object's WASM module (proxy-wasm ABI) at
//! the stages the object declares (docs/network-plugins.md).
//!
//! On an L4 path the hop runs `on_new_connection` once with the connect-time
//! facts; a module that closes the stream (or pauses it) refuses the
//! connection. On an HTTP path it keeps one stream context for the whole
//! exchange ([`PluginExchange`]): request headers, the buffered request
//! body, the response head, the response body chunk by chunk, and `on_log`
//! when it is over. A module answers instead of the upstream with
//! `send_local_response`, rewrites headers, the path, or bodies through the
//! ABI's map and buffer calls, reads the flow's facts as properties
//! (`legion.job`, `legion.identity`, `legion.labels.<k>`, ...), and may call
//! the HTTP targets its object declares.
//!
//! A failed callback (a trap, exhausted fuel, a passed deadline, a module
//! that cannot start) is a handler error: with `onError = "allow"` the
//! exchange goes on without the plugin, otherwise the client gets a `503`
//! naming the hop and the failure.

use super::{
    ExchangeOutcome, Handler, HopExchange, HopFacts, HopOutcome, LocalResponse, RequestHead,
    ResponseHead,
};
use crate::prelude::*;
use crate::structs::hops::{handler_entry, HandlerEntry, HopConfig, Stage};
use crate::structs::netplugin::PluginConfig;
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::plugins::abi::{Pairs, ACTION_PAUSE};
use crate::worker::netd::plugins::vm::{
    prop_key, CallResponse, ContextState, Facts, PendingCall, PropertySource,
};
use crate::worker::netd::plugins::{InstancePool, PluginHost, VmSlot};

use bytes::Bytes;
use std::time::Duration;

pub(super) struct Plugin {
    source: String,
    hop: HopConfig,
    shared: Arc<Shared>,
}

/// What every exchange through one hop shares: the reference, its config
/// encoded once, and the pools it last ran on.
struct Shared {
    reference: PluginConfig,
    configuration: Vec<u8>,
    config_hash: u64,
    /// Scope (`policy`, `job:<id>`) to the pool for the revision it was
    /// built for; a new revision of the plugin builds a new one.
    pools: Mutex<HashMap<String, (u64, Arc<InstancePool>)>>,
}

impl Plugin {
    pub(super) fn new(source: String, config: PluginConfig, hop: HopConfig) -> Self {
        let configuration = config.configuration_bytes();
        let config_hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            configuration.hash(&mut hasher);
            hasher.finish()
        };
        Plugin {
            source,
            hop,
            shared: Arc::new(Shared {
                reference: config,
                configuration,
                config_hash,
                pools: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The scope a reference runs under: the plugin itself for a policy
    /// object's hop, the job for a job's own.
    fn scope(&self, context: &ForwarderContext, allocation_id: i64) -> String {
        if self.source.starts_with("policy/") {
            "policy".to_string()
        } else {
            let job = context
                .identity_for(allocation_id)
                .map(|who| who.job_id)
                .unwrap_or_default();
            format!("job:{job}")
        }
    }

    fn stages(&self, plugins: &PluginHost) -> Option<Vec<Stage>> {
        plugins
            .entry(&self.shared.reference.name)
            .map(|e| e.object.stage_set())
    }
}

#[async_trait::async_trait]
impl Handler for Plugin {
    fn entry(&self) -> &'static HandlerEntry {
        handler_entry("plugin").expect("plugin is registered")
    }

    fn source(&self) -> &str {
        &self.source
    }

    fn fails_open(&self) -> bool {
        self.hop.fails_open()
    }

    async fn on_connect(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> HopOutcome {
        let plugins = context.plugins().clone();
        match self.stages(&plugins) {
            Some(stages) if !stages.contains(&Stage::Connect) => return HopOutcome::Allow,
            _ => {}
        }
        let mut run = Run::new(self, context, facts);
        let outcome = run.connect(context).await;
        run.done(0);
        let name = &self.shared.reference.name;
        match outcome {
            Ok(true) => HopOutcome::Allow,
            Ok(false) => HopOutcome::Deny {
                source: self.source.clone(),
                reason: Some(format!("plugin {name} refused the connection")),
            },
            Err(_) if self.fails_open() => HopOutcome::Allow,
            Err(error) => HopOutcome::Deny {
                source: self.source.clone(),
                reason: Some(format!("plugin {name} {error}")),
            },
        }
    }

    fn exchange(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> Option<Box<dyn HopExchange>> {
        let stages = self.stages(context.plugins());
        Some(Box::new(PluginExchange {
            run: Run::new(self, context, facts),
            // Not on the node yet: the request stage reports why.
            stages: stages.unwrap_or_else(|| vec![Stage::Request]),
            held: Vec::new(),
        }))
    }
}

/// The flow's facts, answered when a module asks (`legion.*`).
struct FlowFacts {
    context: Arc<ForwarderContext>,
    allocation_id: i64,
    direction: &'static str,
    protocol: &'static str,
    destination: String,
    source: String,
    upstream: Option<String>,
    peer: Option<String>,
}

impl PropertySource for FlowFacts {
    fn property(&self, path: &[&str]) -> Option<Vec<u8>> {
        let text = |value: &str| Some(value.as_bytes().to_vec());
        match path {
            ["legion", "allocation"] => text(&self.allocation_id.to_string()),
            ["legion", "direction"] => text(self.direction),
            ["legion", "protocol"] => text(self.protocol),
            ["legion", "destination"] => text(&self.destination),
            ["legion", "source"] => text(&self.source),
            ["legion", "upstream"] => self.upstream.as_deref().and_then(text),
            ["legion", "peer_identity"] => self.peer.as_deref().and_then(text),
            ["legion", "identity"] => self
                .context
                .identity_string_for(self.allocation_id)
                .map(String::into_bytes),
            ["legion", "node"] => self.context.node_id().map(String::into_bytes),
            ["legion", field] => {
                let who = self.context.identity_for(self.allocation_id)?;
                match *field {
                    "job" => text(&who.job_id),
                    "group" => text(&who.group),
                    "task" => text(&who.task_name),
                    _ => None,
                }
            }
            ["legion", "labels", key] => {
                let who = self.context.identity_for(self.allocation_id)?;
                who.labels.get(*key).and_then(|v| text(v))
            }
            _ => None,
        }
    }
}

/// The `direction` and `protocol` facts are a fixed vocabulary; keep them
/// as statics so a context carries no copies.
fn known(value: &str, choices: &[&'static str]) -> &'static str {
    choices.iter().copied().find(|c| *c == value).unwrap_or("")
}

/// One stream context's worth of plugin calls.
struct Run {
    source: String,
    plugin: String,
    fails_open: bool,
    scope: String,
    shared: Arc<Shared>,
    plugins: Arc<PluginHost>,
    /// For the hop's health row (`NetHandlerStatus`), as `authz` and
    /// `proxy` keep theirs: one sample per exchange.
    context: Arc<ForwarderContext>,
    facts: Arc<FlowFacts>,
    allocation_id: i64,
    started: Option<Started>,
    /// Why the context could not start, once it failed.
    failed: Option<String>,
    /// The first callback failure of the exchange, for its health sample.
    first_error: Option<String>,
}

struct Started {
    pool: Arc<InstancePool>,
    slot: Arc<VmSlot>,
    ctx: Option<ContextState>,
    deleted: bool,
}

impl Run {
    fn new(hop: &Plugin, context: &Arc<ForwarderContext>, facts: &HopFacts<'_>) -> Self {
        Run {
            source: hop.source.clone(),
            plugin: hop.shared.reference.name.clone(),
            fails_open: hop.fails_open(),
            scope: hop.scope(context, facts.allocation_id),
            shared: hop.shared.clone(),
            plugins: context.plugins().clone(),
            context: context.clone(),
            facts: Arc::new(FlowFacts {
                context: context.clone(),
                allocation_id: facts.allocation_id,
                direction: known(facts.direction, &["egress", "ingress"]),
                protocol: known(facts.protocol, &["tcp", "http"]),
                destination: facts.destination.to_string(),
                source: hop.source.clone(),
                upstream: facts.upstream.map(str::to_string),
                peer: facts.identity.map(str::to_string),
            }),
            allocation_id: facts.allocation_id,
            started: None,
            failed: None,
            first_error: None,
        }
    }

    /// The pool for this run's scope at the plugin's current revision.
    fn pool(&self) -> Result<Arc<InstancePool>, String> {
        let entry = self.plugins.entry(&self.plugin).ok_or_else(|| {
            "is not on this node yet (its object has not arrived from the manager)".to_string()
        })?;
        if let Some((revision, pool)) = self
            .shared
            .pools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&self.scope)
        {
            if *revision == entry.revision {
                return Ok(pool.clone());
            }
        }
        let (entry, pool) = self.plugins.pool_for(
            &self.plugin,
            &self.shared.configuration,
            self.shared.config_hash,
            &self.scope,
        )?;
        self.shared
            .pools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(self.scope.clone(), (entry.revision, pool.clone()));
        Ok(pool)
    }

    /// Start the stream context on an instance, once.
    fn start(&mut self) -> Result<(), String> {
        if let Some(failed) = &self.failed {
            return Err(failed.clone());
        }
        if self.started.is_some() {
            return Ok(());
        }
        let result = (|| {
            let pool = self.pool()?;
            let slot = pool.pick()?;
            let mut ctx = slot
                .vm
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .new_context()?;
            ctx.facts = Some(Facts(self.facts.clone()));
            Ok::<_, String>(Started {
                pool,
                slot,
                ctx: Some(ctx),
                deleted: false,
            })
        })();
        match result {
            Ok(started) => {
                self.started = Some(started);
                Ok(())
            }
            Err(err) => {
                debug!(plugin = %self.plugin, allocation_id = self.allocation_id, error = %err, "Net plugin context could not start");
                self.first_error.get_or_insert_with(|| err.clone());
                self.failed = Some(err.clone());
                Err(err)
            }
        }
    }

    /// One callback on this context. `prepare` sets what the callback reads;
    /// the context comes back with what the module did. `stage` callbacks
    /// are counted in the plugin's stats and metrics; the context's own
    /// bookkeeping (`on_done`, `on_delete`) passes `None`.
    fn call(
        &mut self,
        stage: Option<Stage>,
        name: &'static str,
        args: impl FnOnce(&crate::worker::netd::plugins::vm::Vm, u32) -> Vec<i32>,
        prepare: impl FnOnce(&mut ContextState),
    ) -> Result<i32, String> {
        self.start()?;
        let started = self.started.as_mut().expect("started above");
        let mut ctx = started.ctx.take().unwrap_or_default();
        prepare(&mut ctx);
        let (outcome, ctx) = {
            let mut vm = started.slot.vm.lock().unwrap_or_else(|p| p.into_inner());
            // An instance that already broke fails every later callback of
            // the contexts it held; the break itself was counted once.
            if let Some(broken) = vm.broken.clone() {
                drop(vm);
                started.ctx = Some(ctx);
                return Err(broken);
            }
            let args = args(&vm, ctx.id);
            vm.call(name, &args, ctx)
        };
        started.ctx = Some(ctx);
        let error = outcome.result.as_ref().err().map(String::as_str);
        if let Some(error) = error {
            self.first_error.get_or_insert_with(|| error.to_string());
        }
        if let (true, Some(stage)) = (outcome.exported, stage) {
            let stats = &started.pool.stats;
            stats.record(outcome.elapsed, error);
            stats
                .fuel
                .fetch_add(outcome.fuel, std::sync::atomic::Ordering::Relaxed);
            net_metrics::plugin_call(
                &self.plugin,
                stage.as_str(),
                if error.is_some() { "error" } else { "ok" },
                outcome.elapsed.as_secs_f64(),
            );
        }
        outcome.result
    }

    fn ctx(&mut self) -> &mut ContextState {
        self.started
            .as_mut()
            .and_then(|s| s.ctx.as_mut())
            .expect("a started context")
    }

    /// Run the module's `dispatch_http_call`s, feeding each answer back with
    /// `on_http_call_response`, until it stops making them. Calls go only to
    /// the object's declared targets, through the host guard and the
    /// allocation's egress policy.
    async fn drain_calls(
        &mut self,
        context: &Arc<ForwarderContext>,
        stage: Stage,
    ) -> Result<(), String> {
        for _round in 0..8 {
            let calls: Vec<PendingCall> = match self.started.as_mut().and_then(|s| s.ctx.as_mut()) {
                Some(ctx) => std::mem::take(&mut ctx.calls),
                None => return Ok(()),
            };
            if calls.is_empty() {
                return Ok(());
            }
            for call in calls {
                let response = dispatch(
                    context,
                    &self.plugins,
                    &self.plugin,
                    self.allocation_id,
                    &call,
                )
                .await;
                let (headers, body) = match response {
                    Ok(answer) => answer,
                    Err(err) => {
                        debug!(plugin = %self.plugin, target = %call.target, error = %err, "Net plugin HTTP call failed");
                        (
                            vec![(":status".to_string(), b"503".to_vec())],
                            err.into_bytes(),
                        )
                    }
                };
                let count = headers.len() as i32;
                let size = body.len() as i32;
                self.call(
                    Some(stage),
                    "proxy_on_http_call_response",
                    |_, id| vec![id as i32, call.token as i32, count, size, 0],
                    |ctx| {
                        ctx.call_response = Some(CallResponse {
                            headers,
                            body,
                            trailers: Vec::new(),
                        })
                    },
                )?;
                self.ctx().call_response = None;
            }
        }
        Err("kept dispatching HTTP calls (more than 8 rounds)".to_string())
    }

    /// The `connect` stage: whether the module lets the connection through.
    async fn connect(&mut self, context: &Arc<ForwarderContext>) -> Result<bool, String> {
        let action = self.call(
            Some(Stage::Connect),
            "proxy_on_new_connection",
            |_, id| vec![id as i32],
            |_| {},
        )?;
        self.drain_calls(context, Stage::Connect).await?;
        let ctx = self.ctx();
        Ok(!ctx.closed && (action != ACTION_PAUSE || ctx.resumed))
    }

    /// After a stream callback: the module's answer, if it gave one, or a
    /// pause it never resumed.
    async fn settle(
        &mut self,
        context: &Arc<ForwarderContext>,
        stage: Stage,
        action: i32,
    ) -> Result<Option<LocalResponse>, String> {
        self.drain_calls(context, stage).await?;
        let source = self.source.clone();
        let plugin = self.plugin.clone();
        let ctx = self.ctx();
        if let Some(reply) = ctx.local_reply.take() {
            // The module's own words: its status details, else the
            // `legion-denied-reason` it set, else a plain line.
            let header_reason = reply
                .headers
                .iter()
                .find(|(name, _)| name == "legion-denied-reason")
                .map(|(_, value)| String::from_utf8_lossy(value).into_owned());
            let reason = (!reply.details.is_empty())
                .then(|| reply.details.clone())
                .or(header_reason)
                .or_else(|| {
                    (reply.status >= 400)
                        .then(|| format!("answered {} by plugin {plugin}", reply.status))
                });
            return Ok(Some(LocalResponse {
                source,
                status: reply.status,
                headers: reply
                    .headers
                    .into_iter()
                    .map(|(name, value)| (name, String::from_utf8_lossy(&value).into_owned()))
                    .collect(),
                body: reply.body,
                reason,
            }));
        }
        if ctx.closed {
            return Ok(Some(LocalResponse {
                source,
                status: 403,
                headers: Vec::new(),
                body: Vec::new(),
                reason: Some(format!("plugin {plugin} closed the stream")),
            }));
        }
        if action == ACTION_PAUSE && !ctx.resumed {
            return Err("paused the stream and never resumed it".to_string());
        }
        ctx.resumed = false;
        Ok(None)
    }

    /// What a failed callback means for the exchange.
    fn failure(&self, error: String) -> ExchangeOutcome {
        if self.fails_open {
            return ExchangeOutcome::Continue;
        }
        let reason = format!("plugin {} {error}", self.plugin);
        ExchangeOutcome::Respond(LocalResponse {
            source: self.source.clone(),
            status: 503,
            headers: Vec::new(),
            body: json!({
                "legion": {
                    "error": reason,
                    "source": self.source,
                    "plugin": self.plugin,
                }
            })
            .to_string()
            .into_bytes(),
            reason: Some(reason),
        })
    }

    /// `on_log` (when the plugin has the `done` stage), then `on_done` and
    /// `on_delete`: the context is gone. The exchange's one health sample
    /// is recorded here.
    fn done(&mut self, status: u16) {
        let started = self.started.as_ref().map(|s| s.deleted);
        if started == Some(true) {
            return;
        }
        if started == Some(false) {
            let logs = self
                .plugins
                .entry(&self.plugin)
                .map(|e| e.object.has_stage(Stage::Done))
                .unwrap_or(false);
            if logs {
                let code = (status as i64).to_le_bytes().to_vec();
                let _ = self.call(
                    Some(Stage::Done),
                    "proxy_on_log",
                    |_, id| vec![id as i32],
                    |ctx| {
                        ctx.properties.insert(prop_key(&["response", "code"]), code);
                    },
                );
            }
            let _ = self.call(None, "proxy_on_done", |_, id| vec![id as i32], |_| {});
            let _ = self.call(None, "proxy_on_delete", |_, id| vec![id as i32], |_| {});
            if let Some(started) = self.started.as_mut() {
                started.deleted = true;
            }
        }
        self.context.note_handler(
            self.allocation_id,
            &self.source,
            "plugin",
            &format!("plugin {}", self.plugin),
            match self.first_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            },
        );
    }
}

/// A plugin's state for one HTTP exchange.
struct PluginExchange {
    run: Run,
    stages: Vec<Stage>,
    /// Response body the module asked to see more of before passing on.
    held: Vec<u8>,
}

fn request_pairs(head: &RequestHead) -> Pairs {
    let mut pairs: Pairs = vec![
        (":method".into(), head.method.as_bytes().to_vec()),
        (":path".into(), head.path.as_bytes().to_vec()),
        (":authority".into(), head.authority.as_bytes().to_vec()),
        (":scheme".into(), head.scheme.as_bytes().to_vec()),
    ];
    pairs.extend(
        head.headers
            .iter()
            .map(|(name, value)| (name.clone(), value.as_bytes().to_vec())),
    );
    pairs
}

#[async_trait::async_trait]
impl HopExchange for PluginExchange {
    async fn on_request(
        &mut self,
        context: &Arc<ForwarderContext>,
        _facts: &HopFacts<'_>,
        head: &mut RequestHead,
    ) -> ExchangeOutcome {
        if !self.stages.contains(&Stage::Request) {
            return ExchangeOutcome::Continue;
        }
        let pairs = request_pairs(head);
        let count = pairs.len();
        let eos = head.end_of_stream;
        let mut request_props: Vec<(String, Vec<u8>)> = vec![
            (
                prop_key(&["request", "method"]),
                head.method.clone().into_bytes(),
            ),
            (
                prop_key(&["request", "path"]),
                head.path.clone().into_bytes(),
            ),
            (
                prop_key(&["request", "host"]),
                head.authority.clone().into_bytes(),
            ),
            (
                prop_key(&["request", "scheme"]),
                head.scheme.clone().into_bytes(),
            ),
        ];
        let action = self.run.call(
            Some(Stage::Request),
            "proxy_on_request_headers",
            |vm, id| vm.headers_args(id, count, eos),
            move |ctx| {
                ctx.request_headers = Some(pairs);
                ctx.request_headers_changed = false;
                ctx.properties.extend(request_props.drain(..));
            },
        );
        let action = match action {
            Ok(action) => action,
            Err(err) => return self.run.failure(err),
        };
        match self.run.settle(context, Stage::Request, action).await {
            Ok(Some(local)) => return ExchangeOutcome::Respond(local),
            Ok(None) => {}
            Err(err) => return self.run.failure(err),
        }
        let ctx = self.run.ctx();
        if ctx.request_headers_changed {
            if let Some(pairs) = ctx.request_headers.clone() {
                head.headers.clear();
                for (name, value) in pairs {
                    let value = String::from_utf8_lossy(&value).into_owned();
                    match name.as_str() {
                        ":path" => head.path = value,
                        // The method, authority, and scheme are the flow's
                        // facts, already judged: a plugin cannot redirect a
                        // request through a header.
                        ":method" | ":authority" | ":scheme" => {}
                        _ if name.starts_with(':') => {}
                        _ => head.headers.push((name, value)),
                    }
                }
                head.changed = true;
            }
        }
        ExchangeOutcome::Continue
    }

    fn body_cap(&self) -> Option<u64> {
        self.stages.contains(&Stage::Body).then(|| {
            self.run
                .plugins
                .entry(&self.run.plugin)
                .map(|e| e.object.limits.max_body_bytes())
                .unwrap_or(crate::structs::netplugin::DEFAULT_PLUGIN_BODY_BYTES)
        })
    }

    async fn on_body(
        &mut self,
        context: &Arc<ForwarderContext>,
        body: &mut Bytes,
    ) -> ExchangeOutcome {
        if !self.stages.contains(&Stage::Body) {
            return ExchangeOutcome::Continue;
        }
        let data = body.to_vec();
        let size = data.len() as i32;
        let action = self.run.call(
            Some(Stage::Body),
            "proxy_on_request_body",
            |_, id| vec![id as i32, size, 1],
            move |ctx| {
                ctx.request_body = Some(data);
                ctx.request_body_changed = false;
            },
        );
        let action = match action {
            Ok(action) => action,
            Err(err) => return self.run.failure(err),
        };
        match self.run.settle(context, Stage::Body, action).await {
            Ok(Some(local)) => return ExchangeOutcome::Respond(local),
            Ok(None) => {}
            Err(err) => return self.run.failure(err),
        }
        let ctx = self.run.ctx();
        if ctx.request_body_changed {
            *body = Bytes::from(ctx.request_body.take().unwrap_or_default());
        }
        ExchangeOutcome::Continue
    }

    fn reads_response(&self) -> bool {
        self.stages.contains(&Stage::Response) || self.stages.contains(&Stage::Done)
    }

    async fn on_response(
        &mut self,
        context: &Arc<ForwarderContext>,
        head: &mut ResponseHead,
    ) -> ExchangeOutcome {
        if !self.stages.contains(&Stage::Response) {
            return ExchangeOutcome::Continue;
        }
        let mut pairs: Pairs = vec![(":status".into(), head.status.to_string().into_bytes())];
        pairs.extend(
            head.headers
                .iter()
                .map(|(name, value)| (name.clone(), value.as_bytes().to_vec())),
        );
        let count = pairs.len();
        let code = (head.status as i64).to_le_bytes().to_vec();
        let action = self.run.call(
            Some(Stage::Response),
            "proxy_on_response_headers",
            |vm, id| vm.headers_args(id, count, false),
            move |ctx| {
                ctx.response_headers = Some(pairs);
                ctx.response_headers_changed = false;
                ctx.properties.insert(prop_key(&["response", "code"]), code);
            },
        );
        let action = match action {
            Ok(action) => action,
            Err(err) => return self.run.failure(err),
        };
        match self.run.settle(context, Stage::Response, action).await {
            Ok(Some(local)) => return ExchangeOutcome::Respond(local),
            Ok(None) => {}
            Err(err) => return self.run.failure(err),
        }
        let ctx = self.run.ctx();
        if ctx.response_headers_changed {
            if let Some(pairs) = ctx.response_headers.clone() {
                head.headers.clear();
                for (name, value) in pairs {
                    let value = String::from_utf8_lossy(&value).into_owned();
                    if name == ":status" {
                        if let Ok(status) = value.trim().parse() {
                            head.status = status;
                        }
                    } else if !name.starts_with(':') {
                        head.headers.push((name, value));
                    }
                }
                head.changed = true;
            }
        }
        ExchangeOutcome::Continue
    }

    fn reads_response_body(&self) -> bool {
        self.stages.contains(&Stage::Response)
    }

    async fn on_response_chunk(
        &mut self,
        context: &Arc<ForwarderContext>,
        chunk: Bytes,
        end: bool,
    ) -> Result<Bytes, String> {
        if self.run.failed.is_some() && self.run.fails_open {
            return Ok(chunk);
        }
        let mut data = std::mem::take(&mut self.held);
        data.extend_from_slice(&chunk);
        let cap = self
            .run
            .plugins
            .entry(&self.run.plugin)
            .map(|e| e.object.limits.max_body_bytes())
            .unwrap_or(crate::structs::netplugin::DEFAULT_PLUGIN_BODY_BYTES);
        if data.len() as u64 > cap {
            let err = format!(
                "plugin {} held more than its maxBodyBytes ({cap}) of the response",
                self.run.plugin
            );
            return if self.run.fails_open {
                Ok(Bytes::from(data))
            } else {
                Err(err)
            };
        }
        let size = data.len() as i32;
        let action = self.run.call(
            Some(Stage::Response),
            "proxy_on_response_body",
            |_, id| vec![id as i32, size, end as i32],
            move |ctx| {
                ctx.response_body = Some(data);
                ctx.response_body_changed = false;
            },
        );
        let action = match action {
            Ok(action) => action,
            Err(_) if self.run.fails_open => {
                let held = self.run.ctx().response_body.take().unwrap_or_default();
                return Ok(Bytes::from(held));
            }
            Err(err) => return Err(format!("plugin {} {err}", self.run.plugin)),
        };
        if let Err(err) = self.run.drain_calls(context, Stage::Response).await {
            return Err(format!("plugin {} {err}", self.run.plugin));
        }
        let ctx = self.run.ctx();
        let body = ctx.response_body.take().unwrap_or_default();
        if action == ACTION_PAUSE && !end && !ctx.resumed {
            // The module wants to see more before any of it goes on.
            self.held = body;
            return Ok(Bytes::new());
        }
        ctx.resumed = false;
        Ok(Bytes::from(body))
    }

    fn on_done(&mut self, status: u16) {
        self.run.done(status);
    }
}

/// One `dispatch_http_call`: to the declared target only, judged by the host
/// guard and the allocation's egress policy, answered as pairs and a body.
async fn dispatch(
    context: &Arc<ForwarderContext>,
    plugins: &PluginHost,
    plugin: &str,
    allocation_id: i64,
    call: &PendingCall,
) -> Result<(Pairs, Vec<u8>), String> {
    let entry = plugins
        .entry(plugin)
        .ok_or_else(|| format!("plugin {plugin} is gone"))?;
    let url = entry
        .object
        .targets
        .get(&call.target)
        .ok_or_else(|| format!("plugin {plugin} declares no target {:?}", call.target))?;
    let target = crate::structs::netplugin::parse_target(url).map_err(|err| err.to_string())?;
    let header = |name: &str| {
        call.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
    };
    let method = header(":method").unwrap_or_else(|| "GET".to_string());
    let path = header(":path").unwrap_or_else(|| "/".to_string());
    if !path.starts_with('/') {
        return Err(format!("a call's :path must start with '/', got {path:?}"));
    }
    // The allocation's egress chain judges the call like any flow the task
    // could make itself.
    if let Some(policy) = context.policy_for(allocation_id) {
        if let Some(chain) = policy.egress.as_ref() {
            let facts = crate::structs::netpolicy::ConnectFacts {
                host: Some(target.host.as_str()),
                ip: target.host.parse().ok(),
                port: target.port,
                identity: None,
            };
            if let crate::structs::netpolicy::Verdict::Deny { source, reason } =
                crate::structs::netpolicy::evaluate_connect(chain, &facts)
            {
                return Err(format!(
                    "egress policy denies the call to {} ({source}{})",
                    target.authority(),
                    reason.map(|r| format!(": {r}")).unwrap_or_default()
                ));
            }
        }
    }
    let addrs = crate::worker::netd::guard::resolve_and_guard(
        context.resolver(),
        context.host_guard(),
        &target.host,
        target.port,
    )
    .await
    .map_err(|refusal| match refusal {
        crate::worker::netd::guard::GuardRefusal::Denied(reason) => {
            format!("the host guard denies the call: {reason}")
        }
        crate::worker::netd::guard::GuardRefusal::Unresolved(reason) => reason,
    })?;
    // Pin the client to the address the guard passed, so the name is never
    // resolved again between the check and the dial.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve(&target.host, addrs[0])
        .timeout(call.timeout.min(Duration::from_secs(60)))
        .user_agent("legion-netd-plugin")
        .build()
        .map_err(|err| err.to_string())?;
    let scheme = if target.tls { "https" } else { "http" };
    let url = format!("{scheme}://{}{path}", target.authority());
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|err| err.to_string())?;
    let mut request = client.request(method, url).body(call.body.clone());
    for (name, value) in &call.headers {
        if name.starts_with(':') || name == "host" {
            continue;
        }
        request = request.header(name.as_str(), value.as_slice());
    }
    let response = request.send().await.map_err(|err| err.to_string())?;
    let mut pairs: Pairs = vec![(
        ":status".into(),
        response.status().as_u16().to_string().into_bytes(),
    )];
    for (name, value) in response.headers() {
        pairs.push((
            name.as_str().to_ascii_lowercase(),
            value.as_bytes().to_vec(),
        ));
    }
    let max = entry.object.limits.max_body_bytes() as usize;
    let body = response.bytes().await.map_err(|err| err.to_string())?;
    if body.len() > max {
        return Err(format!("the call's answer exceeds maxBodyBytes ({max})"));
    }
    Ok((pairs, body.to_vec()))
}

/// The plugin host against real proxy-wasm modules built with the Rust SDK
/// (`tests/netplugins/`, rebuilt by `tests/netplugins/build.sh`).
#[cfg(test)]
mod plugin_host_test {
    use super::*;
    use crate::structs::netplugin::NetPluginObject;
    use crate::worker::netd::handlers::{Exchange, HopBody, Pipeline};
    use crate::worker::netd::policy::AllocIdentity;
    use crate::worker::netd::{Denials, NetPolicyCache};
    use http_body_util::BodyExt;

    fn module(name: &str) -> Vec<u8> {
        std::fs::read(format!(
            "{}/tests/netplugins/{name}.wasm",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    fn object(name: &str, stages: &[&str], extra: Value) -> NetPluginObject {
        let mut value = json!({
            "name": name,
            "module": { "source": "file:///unused.wasm", "checksum": format!("sha256:{}", "0".repeat(64)) },
            "stages": stages,
            "limits": { "deadline": "50ms", "memory": "8Mi" },
        });
        if let (Some(base), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
            for (k, v) in extra {
                base.insert(k.clone(), v.clone());
            }
        }
        let mut object: NetPluginObject = serde_json::from_value(value).unwrap();
        object.normalize_and_validate().unwrap();
        object
    }

    /// A context whose allocation 7 is job `web` labeled tier=free, and
    /// allocation 8 is job `api`.
    fn context(guard_allow: &[String]) -> Arc<ForwarderContext> {
        let cache = Arc::new(NetPolicyCache::new());
        let mut identities = HashMap::new();
        for (id, job, tier) in [(7, "web", "free"), (8, "api", "paid")] {
            identities.insert(
                id,
                AllocIdentity {
                    job_id: job.into(),
                    task_name: "main".into(),
                    group: "g".into(),
                    labels: [("tier".to_string(), tier.to_string())]
                        .into_iter()
                        .collect(),
                    ports: Default::default(),
                },
            );
        }
        cache.set_identities(identities);
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: guard_allow.to_vec(),
                deny: Vec::new(),
            },
        )
        .unwrap();
        Arc::new(
            ForwarderContext::new(cache, Arc::new(Denials::new())).with_host_guard(Arc::new(guard)),
        )
    }

    fn pipeline(hops: Vec<Value>, source_prefix: &str) -> Arc<Pipeline> {
        let chain = crate::structs::netpolicy::EffectiveChain {
            protocol: "http".into(),
            policy: "allow".into(),
            access_log: false,
            callers: None,
            rules: Vec::new(),
            handlers: hops
                .into_iter()
                .enumerate()
                .map(|(i, hop)| crate::structs::netpolicy::EffectiveHandler {
                    handler: serde_json::from_value(hop).unwrap(),
                    source: format!("{source_prefix}#{i}"),
                })
                .collect(),
        };
        Arc::new(Pipeline::compile(Some(&chain), None, &[]))
    }

    fn hop(plugin: &str, mode: &str) -> Value {
        json!({ "plugin": { "name": plugin, "config": { "mode": mode } } })
    }

    fn facts(allocation_id: i64) -> HopFacts<'static> {
        HopFacts {
            allocation_id,
            direction: "egress",
            protocol: "http",
            destination: "api.example.com:443",
            host: Some("api.example.com"),
            port: 443,
            ..Default::default()
        }
    }

    fn head(method: &str, path: &str) -> RequestHead {
        RequestHead {
            method: method.into(),
            path: path.into(),
            authority: "api.example.com".into(),
            scheme: "https".into(),
            headers: vec![
                ("x-remove-me".into(), "1".into()),
                ("accept".into(), "*/*".into()),
            ],
            end_of_stream: true,
            changed: false,
        }
    }

    async fn request(
        context: &Arc<ForwarderContext>,
        pipeline: &Arc<Pipeline>,
        allocation_id: i64,
        method: &str,
        path: &str,
    ) -> (ExchangeOutcome, RequestHead, Exchange) {
        let facts = facts(allocation_id);
        let mut exchange = pipeline.exchange(context, &facts);
        let mut head = head(method, path);
        let outcome = exchange.request(&facts, &mut head).await;
        (outcome, head, exchange)
    }

    fn response(status: u16, body: &'static str) -> hyper::Response<HopBody> {
        hyper::Response::builder()
            .status(status)
            .header("content-length", body.len().to_string())
            .body(
                http_body_util::Full::new(Bytes::from_static(body.as_bytes()))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap()
    }

    fn shared(context: &ForwarderContext, plugin: &str, scope: &str, key: &str) -> Option<String> {
        let entry = context.plugins().entry(plugin)?;
        let data = entry.shared_scope(scope)?;
        data.get(key)
            .map(|(v, _)| String::from_utf8_lossy(&v).into_owned())
    }

    #[tokio::test]
    async fn the_documented_example_denies_through_a_policy_object_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("read-only-free-tier", &["request"], json!({})),
                1,
                &module("read-only-free-tier"),
            )
            .unwrap();
        let pipeline = pipeline(
            vec![json!({ "plugin": { "name": "read-only-free-tier" } })],
            "policy/free",
        );
        let (outcome, _, _) = request(&context, &pipeline, 7, "POST", "/repos/x/issues").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("expected a local response, got {outcome:?}");
        };
        assert_eq!(local.status, 403);
        assert_eq!(local.source, "policy/free#0");
        assert!(local
            .headers
            .iter()
            .any(|(n, v)| n == "legion-denied-reason" && v == "the free tier is read-only"));
        let response = local.into_response();
        assert_eq!(response.headers()["legion-denied-by"], "policy/free#0");
        // A GET from the free tier, and anything from the paid tier, pass.
        let (outcome, _, _) = request(&context, &pipeline, 7, "GET", "/repos/x").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
        let (outcome, _, _) = request(&context, &pipeline, 8, "POST", "/repos/x/issues").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
    }

    #[tokio::test]
    async fn a_plugin_rewrites_headers_and_the_path_from_the_flows_facts_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("c", &["request"], json!({})),
                1,
                &module("conformance"),
            )
            .unwrap();
        let pipeline = pipeline(vec![hop("c", "rewrite")], "policy/p");
        let (outcome, head, _) = request(&context, &pipeline, 7, "GET", "/old/thing?x=1").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
        assert!(head.changed);
        assert_eq!(head.path, "/new/thing?x=1");
        let get = |name: &str| {
            head.headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("x-legion-job"), Some("web"));
        assert_eq!(get("x-tier"), Some("free"));
        assert_eq!(get("x-added"), Some("1"));
        assert_eq!(get("x-remove-me"), None);
        assert_eq!(get("accept"), Some("*/*"));
        assert!(head.headers.iter().all(|(n, _)| !n.starts_with(':')));
    }

    #[tokio::test]
    async fn shared_data_is_scoped_per_job_and_meters_requests_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("c", &["request"], json!({ "usableBy": "jobs" })),
                1,
                &module("conformance"),
            )
            .unwrap();
        // A job's own reference: scoped to that job.
        let pipeline = pipeline(vec![hop("c", "meter")], "task");
        for _ in 0..2 {
            let (outcome, _, _) = request(&context, &pipeline, 7, "GET", "/").await;
            assert_eq!(outcome, ExchangeOutcome::Continue);
        }
        let (outcome, _, _) = request(&context, &pipeline, 7, "GET", "/").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("the third request is over budget");
        };
        assert_eq!(local.status, 429);
        assert_eq!(local.body, b"budget spent");
        // Another job's count starts at zero: it never sees job web's data.
        let (outcome, _, _) = request(&context, &pipeline, 8, "GET", "/").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
        assert_eq!(
            shared(&context, "c", "job:web", "count").as_deref(),
            Some("3")
        );
        assert_eq!(
            shared(&context, "c", "job:api", "count").as_deref(),
            Some("1")
        );
        // Module metrics are kept per scope.
        let entry = context.plugins().entry("c").unwrap();
        assert_eq!(entry.metric_value("job:web", "requests"), Some(3));
    }

    #[tokio::test]
    async fn the_body_stage_denies_and_rewrites_bodies_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(object("c", &["body"], json!({})), 1, &module("conformance"))
            .unwrap();
        let pipeline = pipeline(vec![hop("c", "body")], "policy/p");
        let facts = facts(7);
        let mut exchange = pipeline.exchange(&context, &facts);
        assert!(exchange.reads_body());
        assert_eq!(
            exchange.body_cap(),
            crate::structs::netplugin::DEFAULT_PLUGIN_BODY_BYTES
        );
        let mut head = head("POST", "/");
        head.end_of_stream = false;
        assert_eq!(
            exchange.request(&facts, &mut head).await,
            ExchangeOutcome::Continue
        );
        let mut body = Bytes::from_static(b"card 4242 4242");
        assert_eq!(exchange.body(&mut body).await, ExchangeOutcome::Continue);
        assert_eq!(&body[..], b"card #### ####");
        let mut exchange = pipeline.exchange(&context, &facts);
        let mut body = Bytes::from_static(b"my secret");
        let ExchangeOutcome::Respond(local) = exchange.body(&mut body).await else {
            panic!("a secret is refused");
        };
        assert_eq!(
            (local.status, local.body.as_slice()),
            (403, b"no secrets".as_slice())
        );
    }

    #[tokio::test]
    async fn the_response_stage_rewrites_heads_and_buffered_bodies_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("c", &["response", "done"], json!({})),
                1,
                &module("conformance"),
            )
            .unwrap();
        let pipeline = pipeline(vec![hop("c", "response")], "policy/p");
        let (_, _, exchange) = request(&context, &pipeline, 7, "GET", "/").await;
        let answered = exchange.respond(response(404, "not here")).await;
        assert_eq!(answered.status(), 410);
        assert_eq!(answered.headers()["x-plugin"], "seen");
        assert!(answered.headers().get("content-length").is_none());
        let body = answered.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"NOT HERE");
        // `done` ran once the body ended, with the status the client got.
        for _ in 0..50 {
            if shared(&context, "c", "policy", "last_status").is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            shared(&context, "c", "policy", "last_status").as_deref(),
            Some("410")
        );
    }

    #[tokio::test]
    async fn a_streamed_response_passes_through_chunk_by_chunk_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("c", &["response"], json!({})),
                1,
                &module("conformance"),
            )
            .unwrap();
        let pipeline = pipeline(vec![hop("c", "sse")], "policy/p");
        let (_, _, exchange) = request(&context, &pipeline, 7, "GET", "/events").await;
        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<hyper::body::Frame<Bytes>, super::super::BoxError>,
        >(4);
        let body = http_body_util::StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(rx))
            .boxed();
        let upstream = hyper::Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(body)
            .unwrap();
        let answered = exchange.respond(upstream).await;
        let mut body = answered.into_body();
        tx.send(Ok(hyper::body::Frame::data(Bytes::from_static(
            b"data: one\n\n",
        ))))
        .await
        .unwrap();
        // The first event arrives while the upstream is still open.
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), body.frame())
            .await
            .expect("the first event streams through")
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(&first[..], b"data: [seen] one\n\n");
        tx.send(Ok(hyper::body::Frame::data(Bytes::from_static(
            b"data: two\n\n",
        ))))
        .await
        .unwrap();
        drop(tx);
        let rest = body.collect().await.unwrap().to_bytes();
        assert_eq!(&rest[..], b"data: [seen] two\n\n");
    }

    #[tokio::test]
    async fn a_trapping_plugin_answers_503_and_its_status_shows_the_error_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("c", &["request"], json!({})),
                1,
                &module("conformance"),
            )
            .unwrap();
        let pipeline = pipeline(vec![hop("c", "trap")], "policy/p");
        let (outcome, _, _) = request(&context, &pipeline, 7, "GET", "/").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("a trap fails closed");
        };
        assert_eq!(local.status, 503);
        assert!(
            local
                .reason
                .as_deref()
                .unwrap()
                .contains("plugin c trapped"),
            "{local:?}"
        );
        let response = local.into_response();
        assert_eq!(response.headers()["legion-denied-by"], "policy/p#0");
        let status = context.plugins().report();
        assert_eq!(status[0].state, "ready");
        assert!(status[0].errors >= 1);
        assert!(status[0].last_error.contains("trapped"), "{:?}", status[0]);
        // With onError = "allow" the request goes on without it.
        let open = self::pipeline(
            vec![
                json!({ "plugin": { "name": "c", "config": { "mode": "trap" } }, "onError": "allow" }),
            ],
            "policy/p",
        );
        let (outcome, _, _) = request(&context, &open, 7, "GET", "/").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
    }

    #[tokio::test]
    async fn fuel_and_memory_limits_stop_runaway_modules_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object(
                    "c",
                    &["request"],
                    json!({ "limits": { "deadline": "1ms", "memory": "4Mi" } }),
                ),
                1,
                &module("conformance"),
            )
            .unwrap();
        let spin = pipeline(vec![hop("c", "spin")], "policy/p");
        let started = std::time::Instant::now();
        let (outcome, _, _) = request(&context, &spin, 7, "GET", "/").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("a spinning module is stopped");
        };
        assert!(
            local.reason.as_deref().unwrap().contains("deadline"),
            "{local:?}"
        );
        // Unoptimized test builds run wasmi far slower than release; the
        // bound here is only that fuel stopped it at all.
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
        let hog = pipeline(vec![hop("c", "hog")], "policy/p");
        let (outcome, _, _) = request(&context, &hog, 7, "GET", "/").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("a module over its memory is stopped");
        };
        assert!(
            local.reason.as_deref().unwrap().contains("memory limit"),
            "{local:?}"
        );
    }

    #[tokio::test]
    async fn a_module_signs_with_a_key_it_never_reads_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object(
                    "c",
                    &["request"],
                    json!({ "credentials": { "key": { "kv": "secrets/k" } } }),
                ),
                1,
                &module("conformance"),
            )
            .unwrap();
        let pipeline = pipeline(vec![hop("c", "sign")], "policy/p");
        // No key on the node yet: the module's own fallback.
        let (outcome, _, _) = request(&context, &pipeline, 7, "GET", "/sign-me").await;
        assert!(
            matches!(outcome, ExchangeOutcome::Respond(ref l) if l.status == 500),
            "{outcome:?}"
        );
        context
            .plugins()
            .set_credential("c", "key", 1, "k3y".into());
        let (outcome, head, _) = request(&context, &pipeline, 7, "GET", "/sign-me").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
        let expected: String = ring::hmac::sign(
            &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"k3y"),
            b"/sign-me",
        )
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        assert!(head
            .headers
            .contains(&("x-signature".to_string(), expected)));
    }

    #[tokio::test]
    async fn a_module_calls_only_its_declared_targets_test() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(
                        |req: hyper::Request<hyper::body::Incoming>| async move {
                            let answer = if req.uri().path() == "/quota" {
                                "yes"
                            } else {
                                "no"
                            };
                            Ok::<_, hyper::Error>(hyper::Response::new(http_body_util::Full::new(
                                Bytes::from(answer),
                            )))
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let context = context(&[format!("127.0.0.1:{port}")]);
        context
            .plugins()
            .install_compiled(
                object(
                    "c",
                    &["request"],
                    json!({ "targets": { "quota": format!("http://127.0.0.1:{port}") } }),
                ),
                1,
                &module("conformance"),
            )
            .unwrap();
        let granted = pipeline(vec![hop("c", "call")], "policy/p");
        let (outcome, head, _) = request(&context, &granted, 7, "GET", "/").await;
        assert_eq!(outcome, ExchangeOutcome::Continue);
        assert!(head
            .headers
            .contains(&("x-quota".to_string(), "granted".to_string())));
        let denied = pipeline(
            vec![
                json!({ "plugin": { "name": "c", "config": { "mode": "call", "path": "/other" } } }),
            ],
            "policy/p",
        );
        let (outcome, _, _) = request(&context, &denied, 7, "GET", "/").await;
        assert!(
            matches!(outcome, ExchangeOutcome::Respond(ref l) if l.status == 429),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn the_connect_stage_refuses_connections_on_an_l4_path_test() {
        let context = context(&[]);
        context
            .plugins()
            .install_compiled(
                object("c", &["connect"], json!({})),
                1,
                &module("conformance"),
            )
            .unwrap();
        let chain = crate::structs::netpolicy::EffectiveChain {
            protocol: "tcp".into(),
            policy: "allow".into(),
            access_log: false,
            callers: None,
            rules: Vec::new(),
            handlers: vec![crate::structs::netpolicy::EffectiveHandler {
                handler: serde_json::from_value(hop("c", "connect")).unwrap(),
                source: "policy/p#0".into(),
            }],
        };
        let pipeline = Pipeline::compile(Some(&chain), None, &[]);
        let mut smtp = facts(7);
        smtp.destination = "mail.example.com:25";
        let outcome = pipeline.run_connect(&context, &smtp).await;
        assert!(
            matches!(outcome, HopOutcome::Deny { ref reason, .. } if reason.as_deref() == Some("plugin c refused the connection")),
            "{outcome:?}"
        );
        assert_eq!(
            pipeline.run_connect(&context, &facts(7)).await,
            HopOutcome::Allow
        );
    }

    #[tokio::test]
    async fn a_plugin_not_yet_loaded_or_refusing_its_config_fails_by_its_hop_test() {
        let context = context(&[]);
        let pipeline = pipeline(vec![hop("missing", "noop")], "policy/p");
        let (outcome, _, _) = request(&context, &pipeline, 7, "GET", "/").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("an absent plugin fails closed");
        };
        assert!(
            local
                .reason
                .as_deref()
                .unwrap()
                .contains("not on this node yet"),
            "{local:?}"
        );
        context
            .plugins()
            .install_compiled(
                object("c", &["request"], json!({})),
                1,
                &module("conformance"),
            )
            .unwrap();
        let reject = pipeline_with("c", "reject");
        let (outcome, _, _) = request(&context, &reject, 7, "GET", "/").await;
        let ExchangeOutcome::Respond(local) = outcome else {
            panic!("a refused configuration fails closed");
        };
        assert!(
            local
                .reason
                .as_deref()
                .unwrap()
                .contains("refused to start"),
            "{local:?}"
        );
    }

    fn pipeline_with(plugin: &str, mode: &str) -> Arc<Pipeline> {
        pipeline(vec![hop(plugin, mode)], "policy/p")
    }

    #[test]
    fn a_module_that_is_not_proxy_wasm_is_refused_test() {
        let err = crate::worker::netd::plugins::vm::compile(b"(module)").unwrap_err();
        assert!(err.contains("not a proxy-wasm module"), "{err}");
        let err = crate::worker::netd::plugins::vm::compile(b"not wasm").unwrap_err();
        assert!(err.contains("not a valid module"), "{err}");
    }
}
