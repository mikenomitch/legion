//! App UI extensions, tier 1 (apps plan §6.5; docs/apps.md, "Views and
//! actions"): the manifest's `ui` block, the `legion-view/1` document an app
//! returns, the context a slot passes, typed action inputs, and the viewer
//! assertion the manager signs for every request it forwards to an app.
//!
//! Three parties share these types, so they live here, pure:
//!
//! - the **manager** parses an installed manifest's `ui` block
//!   ([`UiSpec`]), checks a viewer against an entry's declared permission
//!   on the context ([`ViewContext`]), signs a [`ViewerAssertion`], and
//!   validates what the app answers ([`validate_view`]) before the SPA, the
//!   CLI, or MCP sees it;
//! - the **app framework** (`crate::apps::ui`) checks that the assertion
//!   matches the request it rides on (the same context, the same view or
//!   action, the same inputs digest), and validates its own handlers'
//!   views before sending them;
//! - the **CLI** renders a validated view as text.
//!
//! A view is data, never code: a closed set of blocks the SPA draws with its
//! own components. Unknown blocks, unknown fields, raw HTML in markdown, and
//! links anywhere but `http(s)` or an in-app route are refused.

use crate::prelude::*;
use crate::structs::acl::{AclAction, AclResource, AclScope};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// The `view` field of every view document.
pub const VIEW_FORMAT: &str = "legion-view/1";
/// The header the viewer assertion rides in.
pub const VIEWER_HEADER: &str = "legion-viewer";
/// The gRPC metadata key an app sends an action's viewer assertion back to
/// the manager in, beside its own credential, to act as the viewer
/// (docs/auth-and-acls.md, "Delegated identities"): the manager then
/// authenticates the call as `<viewer> via app <app>`, holding only the
/// assertion's [`Delegation`] within the app's own grant.
pub const ON_BEHALF_OF_HEADER: &str = "legion-on-behalf-of";
/// Where an app serves view `<name>`: `GET /legion/v1/views/<name>?<context>`.
pub const VIEW_PATH_PREFIX: &str = "/legion/v1/views/";
/// Where an app serves action `<name>`: `POST /legion/v1/actions/<name>`
/// with `{ "context": {...}, "inputs": {...} }`.
pub const ACTION_PATH_PREFIX: &str = "/legion/v1/actions/";
/// How long a viewer assertion lives.
pub const ASSERTION_TTL_SECS: u64 = 60;
/// The claim that makes a token a viewer assertion. `legion.io/` claims are
/// reserved for the manager (a job's identity cannot set them), so a
/// workload-identity token whose audience happens to be `app:<name>` is
/// still not an assertion.
pub const ASSERTION_CLAIM: &str = "legion.io/viewer";
/// The port label apps serve views on when `ui.port` is unset.
pub const DEFAULT_UI_PORT: &str = "api";
/// Shortest `ui.refresh` the SPA polls at.
pub const MIN_REFRESH_SECS: u64 = 5;
/// Longest `ui.refresh`.
pub const MAX_REFRESH_SECS: u64 = 3600;

/// Largest view document, as JSON.
pub const MAX_VIEW_BYTES: usize = 512 * 1024;
/// Most blocks in one view.
pub const MAX_BLOCKS: usize = 32;
/// Most lines in a `series` block (apps plan §6.5).
pub const MAX_SERIES: usize = 4;
/// Most points in one line.
pub const MAX_POINTS: usize = 2_000;
/// Most rows in a `table`, entries in a `timeline`.
pub const MAX_ROWS: usize = 500;
/// Most columns in a `table`.
pub const MAX_COLUMNS: usize = 12;
/// Most items in `stats`, `keyValue`, and `actions` blocks.
pub const MAX_ITEMS: usize = 50;
/// Largest `markdown` block.
pub const MAX_MARKDOWN_BYTES: usize = 16 * 1024;
/// Longest single text value (a cell, a label, a notice).
pub const MAX_TEXT_LEN: usize = 4_096;
/// Most entries one slot may register.
pub const MAX_SLOT_ENTRIES: usize = 8;
/// Where an app serves sandboxed page `<name>` unless the page names
/// another `bundle`: `GET /legion/v1/pages/<name>`.
pub const PAGE_PATH_PREFIX: &str = "/legion/v1/pages/";
/// Largest page bundle: what a worker reads from an app (1 MiB).
pub const MAX_PAGE_BYTES: usize = 1024 * 1024;
/// Most sandboxed pages one app may declare.
pub const MAX_PAGES: usize = 8;
/// Most RPCs one page may list.
pub const MAX_PAGE_RPCS: usize = 64;
/// The gRPC metadata key the SPA stamps on every call it makes for a
/// sandboxed page (`<app>/<page>`), which the manager checks against the
/// page's `rpcs` (`crate::manager::app_page_guard`).
pub const APP_PAGE_HEADER: &str = "legion-app-page";
/// Most actions one app may declare.
pub const MAX_ACTIONS: usize = 32;
/// Most inputs one action may take.
pub const MAX_INPUTS: usize = 16;

/// The audience of the assertions the manager signs for app `app`.
pub fn assertion_audience(app: &str) -> String {
    format!("app:{app}")
}

/// Where an extension appears in the SPA (apps plan §6.5).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Slot {
    /// A page in the sidebar's Apps section. No context.
    Nav,
    /// A card on the cluster overview. No context.
    Overview,
    /// A panel on a job's page: `namespace`, `job`.
    Job,
    /// A badge on a task's row (and a panel on the task's page): `namespace`,
    /// `job`, `task`, and `group` when the task has one.
    Task,
    /// A panel on a node's page: `node`.
    Node,
}

impl Slot {
    pub const ALL: [Slot; 5] = [Slot::Nav, Slot::Overview, Slot::Job, Slot::Task, Slot::Node];

    pub fn as_str(&self) -> &'static str {
        match self {
            Slot::Nav => "nav",
            Slot::Overview => "overview",
            Slot::Job => "job",
            Slot::Task => "task",
            Slot::Node => "node",
        }
    }

    pub fn parse(raw: &str) -> Result<Slot, String> {
        Slot::ALL
            .into_iter()
            .find(|slot| slot.as_str() == raw.trim())
            .ok_or_else(|| {
                format!("unknown slot {raw:?}; the slots are nav, overview, job, task, and node")
            })
    }

    /// Whether the slot carries a context a permission is checked on. Only
    /// these slots may hold actions (apps plan §6.5, "Actions only where
    /// there is a scope").
    pub fn has_scope(&self) -> bool {
        matches!(self, Slot::Job | Slot::Task | Slot::Node)
    }
}

impl std::fmt::Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A permission a slot entry or action declares: checked as the viewer, on
/// the context's scope, before anything reaches the app.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UiPermission {
    /// An ACL resource (`job`, `scale`, `node`, ...).
    pub resource: String,
    /// `read` or `write`.
    pub action: String,
}

impl UiPermission {
    /// The resource and action, validated.
    pub fn parsed(&self) -> Result<(AclResource, AclAction), String> {
        let resource = AclResource::parse(self.resource.trim()).ok_or_else(|| {
            format!(
                "permission resource {:?} is not an ACL resource ({})",
                self.resource,
                AclResource::all().join(", ")
            )
        })?;
        let action = AclAction::parse(self.action.trim()).ok_or_else(|| {
            format!(
                "permission action {:?} must be `read` or `write`",
                self.action
            )
        })?;
        Ok((resource, action))
    }

    /// `scale:read`.
    pub fn label(&self) -> String {
        format!("{}:{}", self.resource.trim(), self.action.trim())
    }
}

/// One registration in a slot (`ui.nav`, `ui.job`, ...).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlotEntry {
    /// The panel's, card's, or page's title (the sidebar label, for `nav`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    /// The sidebar icon, for `nav` (an icon name; the SPA falls back to a
    /// default for one it does not know).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// The view the app serves for this entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    /// For `task`: the view whose `badge` the task row shows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub badge: Option<String>,
    /// Who sees it: checked as the viewer on the context (on any scope for
    /// `nav` and `overview`). Without one, `app:read` on the app suffices.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<UiPermission>,
}

/// The type of an action input (apps plan §6.5: typed values only).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    String,
    Number,
    /// `30s`, `5m`, `1h`.
    Duration,
    /// One of `values`.
    Enum,
    Boolean,
}

/// One input an action takes.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputDecl {
    #[serde(rename = "type")]
    pub kind: InputType,
    /// The value when the caller gives none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Value>")]
    pub default: Option<Value>,
    /// The choices of an `enum` input.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
    /// The form label (default the input's name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// A refusal when the caller gives no value and there is no default.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub required: bool,
}

/// An action an app declares (`ui.actions`): a button in a view's `actions`
/// block, checked as the viewer and audited.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionDecl {
    pub name: String,
    pub title: String,
    /// What the confirmation asks. `{namespace}`, `{job}`, `{task}`,
    /// `{group}`, and `{node}` are replaced from the context (not Nickel's
    /// `%{...}`, which a Nickel manifest would interpolate). A `write`
    /// action always asks; without `confirm` it asks "<title>?".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, InputDecl>,
    /// Checked as the viewer on the context. Required.
    pub permission: UiPermission,
}

impl ActionDecl {
    /// Whether the action changes something: its permission is a `write`.
    /// The SPA always confirms these.
    pub fn mutating(&self) -> bool {
        self.permission.action.trim() == "write"
    }

    /// The confirmation text for `context`, or `None` for a read action
    /// that declares none.
    pub fn confirmation(&self, context: &ViewContext) -> Option<String> {
        let template = match &self.confirm {
            Some(confirm) => confirm.clone(),
            None if self.mutating() => format!("{}?", self.title),
            None => return None,
        };
        let job = if context.job.is_empty() {
            String::new()
        } else {
            crate::structs::namespace::job_id(&context.namespace, &context.job)
        };
        Some(
            template
                .replace("{namespace}", &context.namespace)
                .replace("{job}", &job)
                .replace("{group}", &context.group)
                .replace("{task}", &context.task)
                .replace("{node}", &context.node),
        )
    }
}

/// The manifest's `ui` block (apps plan §6.5).
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiSpec {
    /// The port label (`network.ports[].name` of the app's task) the app
    /// serves views and actions on. Default `api`. Never a catalog service:
    /// the worker delivers the manager's requests to it locally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nav: Vec<SlotEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overview: Vec<SlotEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub job: Vec<SlotEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task: Vec<SlotEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node: Vec<SlotEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionDecl>,
    /// How often the SPA re-fetches the app's views (`10s`; at least 5s).
    /// Unset: on page load and on demand only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh: Option<String>,
    /// Sandboxed pages (tier 2): single-file HTML the SPA mounts in a
    /// sandboxed iframe, in the sidebar's Apps section.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pages: Vec<PageDecl>,
}

/// A sandboxed page an app declares (`ui.pages`; apps plan §6.5, tier 2):
/// a single-file HTML bundle the app serves, which the SPA fetches through
/// `GetAppPage` and mounts in an `<iframe sandbox="allow-scripts">` with a
/// strict CSP, listed in the sidebar's Apps section. Its only power is a
/// `postMessage` bridge that runs the Manager RPCs in `rpcs` as the viewer.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageDecl {
    /// The page's name: its route (`/apps/<app>/pages/<name>`) and the name
    /// the viewer assertion carries.
    pub name: String,
    /// The sidebar label and the page's title.
    pub title: String,
    /// The sidebar icon (an icon name; unknown ones show a default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Where the app serves the bundle, under `/legion/v1/`. Default
    /// `/legion/v1/pages/<name>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle: Option<String>,
    /// The Manager RPCs the page may call through the bridge, as the viewer
    /// (`RunJob`, `GetJobInfo`). Only the RPCs [`page_rpc_kind`] knows are
    /// allowed; calls that change something are confirmed by the viewer
    /// each time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rpcs: Vec<String>,
    /// Who sees the page: checked as the viewer on any scope, like a `nav`
    /// entry's. Without one, `app:read` on the app suffices.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<UiPermission>,
}

impl PageDecl {
    /// The path the bundle is fetched from.
    pub fn bundle_path(&self) -> String {
        self.bundle
            .as_deref()
            .map(str::trim)
            .filter(|bundle| !bundle.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{PAGE_PATH_PREFIX}{}", self.name))
    }

    /// Whether the page may call `method` through the bridge: it is listed
    /// in `rpcs`, and pages may call it at all.
    pub fn allows_rpc(&self, method: &str) -> bool {
        page_rpc_kind(method).is_some() && self.rpcs.iter().any(|rpc| rpc == method)
    }
}

/// What a page-callable RPC does (`ui.pages[].rpcs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageRpcKind {
    /// Reads only. The bridge runs it as the viewer without asking.
    Read,
    /// Changes something (or acts with a side effect). The SPA asks the
    /// viewer before every call, naming the app, the RPC, and the target.
    Write,
}

/// The Manager RPCs a sandboxed page may list in `rpcs`, and whether each
/// one changes anything. Classified by what the call does, not by its ACL
/// action (`RestoreInstance` needs only `snapshot:read` but creates an
/// instance). An RPC missing here is refused at install; see
/// [`page_rpc_refusal`] for why the rest are left out.
pub const PAGE_RPCS: &[(&str, PageRpcKind)] = {
    use PageRpcKind::{Read, Write};
    &[
        // Jobs and rollouts.
        ("RunJob", Write),
        ("GetJobSpec", Read),
        ("GetJobSpecByConfig", Read),
        ("ListJobSpecHistory", Read),
        ("GetJobInfo", Read),
        ("ListJobs", Read),
        ("PlanJob", Read),
        ("StopJob", Write),
        ("DestroyJob", Write),
        ("ListRollouts", Read),
        ("GetRollout", Read),
        ("GetRolloutStatus", Read),
        ("ListRolloutEvents", Read),
        ("AdvanceRollout", Write),
        ("StopRollout", Write),
        ("ListJobActions", Read),
        ("GetAllocationLogs", Read),
        // Scaling.
        ("ScaleTask", Write),
        ("GetJobScaleStatus", Read),
        ("GetScalingInputs", Read),
        ("GetScalingDefaults", Read),
        ("ApplyScalingDefaults", Write),
        ("GetScalingStatus", Read),
        ("GetScaleHistory", Read),
        ("PauseScaling", Write),
        ("ApplyScalingPolicy", Write),
        ("DeleteScalingPolicy", Write),
        ("ListScalingPolicies", Read),
        ("GetCapacityDemand", Read),
        // Namespaces and templates.
        ("ListNamespaces", Read),
        ("GetNamespace", Read),
        ("ApplyNamespace", Write),
        ("DeleteNamespace", Write),
        ("ListNamespacePolicies", Read),
        ("GetNamespacePolicy", Read),
        ("ApplyNamespacePolicy", Write),
        ("DeleteNamespacePolicy", Write),
        ("ListJobTemplates", Read),
        ("GetJobTemplate", Read),
        ("RenderJobTemplate", Read),
        ("UpsertJobTemplate", Write),
        ("DeleteJobTemplate", Write),
        ("RunJobTemplate", Write),
        // Instances, snapshots, volumes.
        ("CreateInstance", Write),
        ("CreateInstances", Write),
        ("DestroyInstance", Write),
        ("StopInstance", Write),
        ("StartInstance", Write),
        ("SuspendInstance", Write),
        ("ForkInstance", Write),
        ("ReplaceInstance", Write),
        ("PromoteInstance", Write),
        ("ListInstances", Read),
        ("GetInstance", Read),
        ("ListSessionNodes", Read),
        ("CreateSnapshot", Write),
        ("ListSnapshots", Read),
        ("GetSnapshot", Read),
        ("DeleteSnapshot", Write),
        ("RestoreInstance", Write),
        ("SetSnapshotAlias", Write),
        ("DeleteSnapshotAlias", Write),
        ("WarmSnapshot", Write),
        ("ListVolumes", Read),
        ("GetVolume", Read),
        ("DeleteVolume", Write),
        ("ListVolumeDrivers", Read),
        // Nodes.
        ("GetNodes", Read),
        ("GetNode", Read),
        ("GetNodeUsage", Read),
        ("ListNodeUsage", Read),
        ("GetNodeContent", Read),
        ("GetLedger", Read),
        ("RestartWorker", Write),
        ("SetNodeAvailability", Write),
        ("ForgetNode", Write),
        ("ListJoinTokens", Read),
        ("DeleteJoinToken", Write),
        ("ListBootTickets", Read),
        // Services and the network.
        ("ListServices", Read),
        ("ListServiceEdges", Read),
        ("ResolveService", Read),
        ("UpsertNetPolicy", Write),
        ("GetNetPolicyObject", Read),
        ("ListNetPolicies", Read),
        ("DeleteNetPolicy", Write),
        ("ShowNetPolicy", Read),
        ("TestNetPolicy", Read),
        ("UpsertNetPlugin", Write),
        ("GetNetPlugin", Read),
        ("ListNetPlugins", Read),
        ("DeleteNetPlugin", Write),
        ("GetNetPluginStatus", Read),
        ("GetMeshCa", Read),
        ("RotateMeshCa", Write),
        // KV.
        ("PutKv", Write),
        ("GetKv", Read),
        ("ListKv", Read),
        ("DeleteKv", Write),
        ("PatchKv", Write),
        ("AcquireKvLock", Write),
        ("RenewKvLock", Write),
        ("ReleaseKvLock", Write),
        ("GetKvLock", Read),
        // Operating the cluster.
        ("GetManagerConfigs", Read),
        ("ListManagers", Read),
        ("GetObservability", Read),
        ("GetSchedulerConcurrency", Read),
        ("GetRaftStatus", Read),
        ("ListRaftHistory", Read),
        ("GetKeyringStatus", Read),
        ("RotateDataKey", Write),
        ("RewrapKv", Write),
        ("QueryStateStore", Read),
        ("GetStateStoreSchema", Read),
        ("ListSavedStateQueries", Read),
        ("UpsertSavedStateQuery", Write),
        ("DeleteSavedStateQuery", Write),
        ("ListEvents", Read),
        // Access control and apps.
        ("WhoAmI", Read),
        ("GetAuthMethods", Read),
        ("UpsertAclPolicy", Write),
        ("GetAclPolicy", Read),
        ("ListAclPolicies", Read),
        ("DeleteAclPolicy", Write),
        ("ListAclTokens", Read),
        ("DeleteAclToken", Write),
        ("UpsertAclUser", Write),
        ("ListAclUsers", Read),
        ("DeleteAclUser", Write),
        ("ListExecKeys", Read),
        ("DeleteExecKey", Write),
        ("ListGrants", Read),
        ("RevokeGrant", Write),
        ("ListApps", Read),
        ("GetApp", Read),
        ("InstallApp", Write),
        ("UpgradeApp", Write),
        ("UninstallApp", Write),
        ("SetAppEnabled", Write),
        ("UpdateAppSettings", Write),
        ("SetAppConfig", Write),
    ]
};

/// Whether a page may call `method`, and what it does.
pub fn page_rpc_kind(method: &str) -> Option<PageRpcKind> {
    PAGE_RPCS
        .iter()
        .find(|(name, _)| *name == method)
        .map(|(_, kind)| *kind)
}

/// Why `method` is not available to sandboxed pages.
pub fn page_rpc_refusal(method: &str) -> String {
    let why = match method {
        "Login" | "CreateAclToken" | "CreateJoinToken" | "GetRaftJoinToken" | "MintExecTicket"
        | "CreateExecKey" | "IssueBootTicket" | "GetStateBackup" => {
            "returns a credential or the cluster's secrets, which a page must never hold"
        }
        "IssueRaftCommand" | "ResetKeyring" | "RewrapRootKey" => {
            "is a break-glass operator operation; run it from the CLI"
        }
        "WatchScalingPolicies" | "StreamEvents" | "ExecAllocation" | "StreamWork" => {
            "streams, and the bridge carries unary calls only"
        }
        "ListUiExtensions" | "GetAppView" | "InvokeAppAction" | "GetAppPage" => {
            "is how the SPA draws apps; a page reaches its own app's views and actions with \
             legion.view and legion.action"
        }
        "WorkerPreRegister"
        | "WorkerRegister"
        | "Heartbeat"
        | "HealthCheck"
        | "FetchDatapathMaterial"
        | "IssueWorkloadIdentityToken"
        | "IssueNodeIdentityToken"
        | "IssueNodeCertificate"
        | "GetArtifactCredential"
        | "RatifyClaim"
        | "UpdateNodeDynamic"
        | "GetHeartbeatCadence"
        | "ReportScalingStatus"
        | "RecordScalingAction" => "is called by workers and apps, not by people",
        _ => {
            "is not a Manager RPC a page may call (name one like `GetJobInfo`, as \
             proto/legion.proto spells it)"
        }
    };
    format!("RPC {method:?} {why}")
}

/// A view or action name: lowercase letters, digits, `-`, `_`, and `.`,
/// starting with a letter or digit, at most 64. It lands in a URL path.
pub fn validate_ui_name(kind: &str, name: &str) -> Result<(), String> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '_' | '.'));
    if valid {
        Ok(())
    } else {
        Err(format!(
            "{kind} name {name:?} must be 1 to 64 lowercase letters, digits, '-', '_' or '.', \
             starting with a letter or digit"
        ))
    }
}

/// Input names an action may not take: they name the context, which comes
/// from the slot, never from the caller (apps plan §6.5: an input cannot
/// name a different target).
const RESERVED_INPUTS: &[&str] = &[
    "app",
    "slot",
    "context",
    "namespace",
    "job",
    "group",
    "task",
    "node",
    "allocation",
    "instance",
];

impl UiSpec {
    /// Parse a manifest's `ui` value. Unknown fields are errors.
    pub fn parse(value: &Value) -> Result<UiSpec, String> {
        serde_json::from_value(value.clone()).map_err(|err| format!("ui: {err}"))
    }

    /// The parsed `ui` block of a manifest, or an empty one.
    pub fn of(ui: Option<&Value>) -> Result<UiSpec, String> {
        match ui {
            None | Some(Value::Null) => Ok(UiSpec::default()),
            Some(value) => UiSpec::parse(value),
        }
    }

    /// The port label views and actions are served on.
    pub fn port(&self) -> &str {
        self.port
            .as_deref()
            .map(str::trim)
            .filter(|port| !port.is_empty())
            .unwrap_or(DEFAULT_UI_PORT)
    }

    /// The entries of `slot`.
    pub fn entries(&self, slot: Slot) -> &[SlotEntry] {
        match slot {
            Slot::Nav => &self.nav,
            Slot::Overview => &self.overview,
            Slot::Job => &self.job,
            Slot::Task => &self.task,
            Slot::Node => &self.node,
        }
    }

    /// Whether the app registers anything the manager forwards to it.
    pub fn serves_anything(&self) -> bool {
        Slot::ALL.iter().any(|slot| !self.entries(*slot).is_empty())
            || !self.actions.is_empty()
            || !self.pages.is_empty()
    }

    /// The sandboxed page named `name`.
    pub fn page(&self, name: &str) -> Option<&PageDecl> {
        self.pages.iter().find(|page| page.name == name)
    }

    /// The action named `name`.
    pub fn action(&self, name: &str) -> Option<&ActionDecl> {
        self.actions.iter().find(|action| action.name == name)
    }

    /// The entry of `slot` that serves `view` (as its view or, in `task`,
    /// its badge).
    pub fn entry_for_view(&self, slot: Slot, view: &str) -> Option<&SlotEntry> {
        self.entries(slot).iter().find(|entry| {
            entry.view.as_deref() == Some(view) || entry.badge.as_deref() == Some(view)
        })
    }

    /// The refresh interval, when one is set.
    pub fn refresh_secs(&self) -> Result<Option<u64>, String> {
        let Some(raw) = self.refresh.as_deref().map(str::trim) else {
            return Ok(None);
        };
        let duration = humantime::parse_duration(raw)
            .map_err(|err| format!("ui.refresh {raw:?} is not a duration: {err}"))?;
        let secs = duration.as_secs();
        if !(MIN_REFRESH_SECS..=MAX_REFRESH_SECS).contains(&secs) {
            return Err(format!(
                "ui.refresh {raw:?} must be between {MIN_REFRESH_SECS}s and 1h"
            ));
        }
        Ok(Some(secs))
    }

    /// Check the block. `ports` are the port labels the app's tasks declare:
    /// an app that registers views or actions must declare its `ui.port`.
    pub fn validate(&self, ports: &[String]) -> Result<(), String> {
        if let Some(port) = &self.port {
            if port.trim().is_empty() {
                return Err("ui.port must name a port label".into());
            }
        }
        if self.serves_anything() && !ports.iter().any(|label| label == self.port()) {
            return Err(format!(
                "ui: the app's views and actions are served on port {:?}, which no task of the \
                 app's job declares (add `network = {{ ports = [{{ name = {:?} }}] }}`, or set \
                 ui.port)",
                self.port(),
                self.port()
            ));
        }
        self.refresh_secs()?;
        for slot in Slot::ALL {
            let entries = self.entries(slot);
            if entries.len() > MAX_SLOT_ENTRIES {
                return Err(format!(
                    "ui.{slot}: at most {MAX_SLOT_ENTRIES} entries per slot"
                ));
            }
            for (index, entry) in entries.iter().enumerate() {
                validate_entry(slot, entry).map_err(|err| format!("ui.{slot}[{index}]: {err}"))?;
            }
        }
        if self.actions.len() > MAX_ACTIONS {
            return Err(format!("ui.actions: at most {MAX_ACTIONS} actions"));
        }
        let mut names = std::collections::BTreeSet::new();
        for (index, action) in self.actions.iter().enumerate() {
            validate_action(action).map_err(|err| format!("ui.actions[{index}]: {err}"))?;
            if !names.insert(action.name.as_str()) {
                return Err(format!(
                    "ui.actions: two actions are named {:?}",
                    action.name
                ));
            }
        }
        if self.pages.len() > MAX_PAGES {
            return Err(format!("ui.pages: at most {MAX_PAGES} pages"));
        }
        let mut pages = std::collections::BTreeSet::new();
        let mut bundles = std::collections::BTreeSet::new();
        for (index, page) in self.pages.iter().enumerate() {
            validate_page(page).map_err(|err| format!("ui.pages[{index}]: {err}"))?;
            if !pages.insert(page.name.as_str()) {
                return Err(format!("ui.pages: two pages are named {:?}", page.name));
            }
            if !bundles.insert(page.bundle_path()) {
                return Err(format!(
                    "ui.pages: two pages are served from {:?}",
                    page.bundle_path()
                ));
            }
        }
        Ok(())
    }

    /// The UI slots with at least one entry.
    pub fn slots(&self) -> Vec<Slot> {
        Slot::ALL
            .into_iter()
            .filter(|slot| !self.entries(*slot).is_empty())
            .collect()
    }
}

fn validate_text(what: &str, text: &str, max: usize) -> Result<(), String> {
    if text.len() > max {
        return Err(format!("{what} is longer than {max} bytes"));
    }
    if text
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(format!("{what} contains control characters"));
    }
    Ok(())
}

fn validate_entry(slot: Slot, entry: &SlotEntry) -> Result<(), String> {
    validate_text("title", &entry.title, 80)?;
    if let Some(view) = &entry.view {
        validate_ui_name("view", view)?;
    }
    if let Some(badge) = &entry.badge {
        if slot != Slot::Task {
            return Err("`badge` is for the task slot; use `view`".into());
        }
        validate_ui_name("badge view", badge)?;
    }
    if let Some(icon) = &entry.icon {
        if slot != Slot::Nav {
            return Err("`icon` is for the nav slot".into());
        }
        validate_icon(icon)?;
    }
    match (slot, &entry.view, &entry.badge) {
        (Slot::Task, None, None) => {
            return Err("a task entry names a `badge` view, a `view`, or both".into())
        }
        (Slot::Task, _, _) => {}
        (_, None, _) => return Err("`view` is required".into()),
        _ => {}
    }
    if entry.view.is_some() && entry.title.trim().is_empty() {
        return Err("`title` is required".into());
    }
    if let Some(permission) = &entry.permission {
        permission.parsed()?;
    }
    Ok(())
}

fn validate_icon(icon: &str) -> Result<(), String> {
    let valid = !icon.is_empty()
        && icon.len() <= 32
        && icon.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    if valid {
        Ok(())
    } else {
        Err(format!("icon {icon:?} must be a short icon name"))
    }
}

/// A page's bundle path: under `/legion/v1/` (the only prefix a worker
/// delivers), outside the view and action paths, with no query, fragment,
/// or `..`.
fn validate_bundle(bundle: &str) -> Result<(), String> {
    let rest = bundle.strip_prefix("/legion/v1/").ok_or_else(|| {
        format!("bundle {bundle:?} must be a path under /legion/v1/ (the app's UI port)")
    })?;
    let valid = !rest.is_empty()
        && bundle.len() <= 256
        && !bundle.contains("..")
        && !bundle.contains("//")
        && bundle
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.'));
    if !valid {
        return Err(format!(
            "bundle {bundle:?} must be a plain path (letters, digits, '/', '-', '_', '.'), \
             with no query or '..'"
        ));
    }
    if bundle.starts_with(VIEW_PATH_PREFIX) || bundle.starts_with(ACTION_PATH_PREFIX) {
        return Err(format!(
            "bundle {bundle:?} is under {VIEW_PATH_PREFIX} or {ACTION_PATH_PREFIX}, where views \
             and actions are served"
        ));
    }
    Ok(())
}

fn validate_page(page: &PageDecl) -> Result<(), String> {
    validate_ui_name("page", &page.name)?;
    if page.title.trim().is_empty() {
        return Err(format!("page {:?} needs a `title`", page.name));
    }
    validate_text("title", &page.title, 80)?;
    if let Some(icon) = &page.icon {
        validate_icon(icon)?;
    }
    validate_bundle(&page.bundle_path()).map_err(|err| format!("page {:?}: {err}", page.name))?;
    if page.rpcs.len() > MAX_PAGE_RPCS {
        return Err(format!(
            "page {:?} lists more than {MAX_PAGE_RPCS} RPCs",
            page.name
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for rpc in &page.rpcs {
        if page_rpc_kind(rpc).is_none() {
            return Err(format!("page {:?}: {}", page.name, page_rpc_refusal(rpc)));
        }
        if !seen.insert(rpc.as_str()) {
            return Err(format!("page {:?} lists {rpc:?} twice", page.name));
        }
    }
    if let Some(permission) = &page.permission {
        permission.parsed()?;
    }
    Ok(())
}

fn validate_action(action: &ActionDecl) -> Result<(), String> {
    validate_ui_name("action", &action.name)?;
    if action.title.trim().is_empty() {
        return Err(format!("action {:?} needs a `title`", action.name));
    }
    validate_text("title", &action.title, 80)?;
    if let Some(confirm) = &action.confirm {
        validate_text("confirm", confirm, 400)?;
    }
    action
        .permission
        .parsed()
        .map_err(|err| format!("action {:?}: {err}", action.name))?;
    if action.inputs.len() > MAX_INPUTS {
        return Err(format!(
            "action {:?} takes more than {MAX_INPUTS} inputs",
            action.name
        ));
    }
    for (name, input) in &action.inputs {
        validate_ui_name("input", name)?;
        if RESERVED_INPUTS.contains(&name.as_str()) {
            return Err(format!(
                "action {:?}: input {name:?} names the context, which comes from the slot \
                 the action is pressed in; inputs cannot name a target",
                action.name
            ));
        }
        if input.kind == InputType::Enum {
            if input.values.is_empty() {
                return Err(format!(
                    "action {:?}: enum input {name:?} needs `values`",
                    action.name
                ));
            }
        } else if !input.values.is_empty() {
            return Err(format!(
                "action {:?}: input {name:?} has `values`, which only an enum takes",
                action.name
            ));
        }
        if let Some(label) = &input.label {
            validate_text("label", label, 80)?;
        }
        if let Some(default) = &input.default {
            check_input(name, input, default)
                .map_err(|err| format!("action {:?}: default: {err}", action.name))?;
        }
    }
    Ok(())
}

/// Check one input value against its declaration, and return it in the
/// form the app receives.
fn check_input(name: &str, decl: &InputDecl, value: &Value) -> Result<Value, String> {
    match decl.kind {
        InputType::String => match value {
            Value::String(text) => {
                validate_text(&format!("input {name:?}"), text, MAX_TEXT_LEN)?;
                Ok(value.clone())
            }
            _ => Err(format!("input {name:?} must be a string")),
        },
        InputType::Number => match value.as_f64() {
            Some(number) if number.is_finite() => Ok(value.clone()),
            _ => Err(format!("input {name:?} must be a number")),
        },
        InputType::Duration => match value.as_str() {
            Some(text) => humantime::parse_duration(text.trim())
                .map(|_| Value::String(text.trim().to_string()))
                .map_err(|err| {
                    format!("input {name:?}: {text:?} is not a duration (like 30s, 5m, 1h): {err}")
                }),
            None => Err(format!(
                "input {name:?} must be a duration string (like 30s, 5m, 1h)"
            )),
        },
        InputType::Enum => match value.as_str() {
            Some(text) if decl.values.iter().any(|choice| choice == text) => Ok(value.clone()),
            _ => Err(format!(
                "input {name:?} must be one of {}",
                decl.values.join(", ")
            )),
        },
        InputType::Boolean => match value {
            Value::Bool(_) => Ok(value.clone()),
            _ => Err(format!("input {name:?} must be true or false")),
        },
    }
}

/// The inputs an action runs with: what the caller gave, checked against
/// the declaration, with defaults filled in. Unknown inputs are refused.
pub fn resolve_inputs(action: &ActionDecl, given: &Value) -> Result<Value, String> {
    let given = match given {
        Value::Null => Map::new(),
        Value::Object(map) => map.clone(),
        _ => return Err("inputs must be an object".into()),
    };
    if let Some(unknown) = given.keys().find(|key| !action.inputs.contains_key(*key)) {
        let known: Vec<&str> = action.inputs.keys().map(String::as_str).collect();
        return Err(format!(
            "action {:?} takes no input {unknown:?}{}",
            action.name,
            if known.is_empty() {
                " (it takes none)".to_string()
            } else {
                format!(" (it takes {})", known.join(", "))
            }
        ));
    }
    let mut resolved = Map::new();
    for (name, decl) in &action.inputs {
        let value = given
            .get(name)
            .filter(|value| !value.is_null())
            .or(decl.default.as_ref());
        match value {
            Some(value) => {
                resolved.insert(name.clone(), check_input(name, decl, value)?);
            }
            None if decl.required => {
                return Err(format!("action {:?} needs input {name:?}", action.name))
            }
            None => {}
        }
    }
    Ok(Value::Object(resolved))
}

/// The digest of an action's inputs that its assertion carries: SHA-256 of
/// their canonical JSON, as hex.
pub fn inputs_digest(inputs: &Value) -> String {
    use sha2::Digest;
    let canonical = crate::structs::app::canonical_json(inputs);
    sha2::Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// What a slot passes to a view or action: where on the SPA it is shown.
/// Empty fields are absent.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ViewContext {
    pub slot: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    /// The job's bare name.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub job: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub task: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node: String,
}

fn validate_context_name(what: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 253 {
        return Err(format!("the context's {what} must be 1 to 253 characters"));
    }
    if value
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '/')
    {
        return Err(format!(
            "the context's {what} {value:?} is not a valid name"
        ));
    }
    Ok(())
}

impl ViewContext {
    /// The slot, parsed.
    pub fn slot(&self) -> Result<Slot, String> {
        Slot::parse(&self.slot)
    }

    /// Normalize and check the context for its slot: a job or task context
    /// carries a job (and its namespace, `default` when unset), a task
    /// context a task, a node context a node, and nothing else is allowed.
    /// A job written `namespace/name` is split.
    pub fn normalized(mut self) -> Result<ViewContext, String> {
        let slot = self.slot()?;
        self.slot = slot.as_str().to_string();
        for field in [
            &mut self.namespace,
            &mut self.job,
            &mut self.group,
            &mut self.task,
            &mut self.node,
        ] {
            *field = field.trim().to_string();
        }
        if self.job.contains('/') {
            let (namespace, name) = crate::structs::namespace::split_job_id(&self.job);
            if !self.namespace.is_empty() && self.namespace != namespace {
                return Err(format!(
                    "the context's job {:?} is in namespace {namespace}, not {}",
                    self.job, self.namespace
                ));
            }
            let (namespace, name) = (namespace.to_string(), name.to_string());
            self.namespace = namespace;
            self.job = name;
        }
        let present = |value: &str| !value.is_empty();
        match slot {
            Slot::Nav | Slot::Overview => {
                if [
                    &self.namespace,
                    &self.job,
                    &self.group,
                    &self.task,
                    &self.node,
                ]
                .iter()
                .any(|value| present(value))
                {
                    return Err(format!("the {slot} slot takes no context"));
                }
            }
            Slot::Job => {
                if present(&self.group) || present(&self.task) || present(&self.node) {
                    return Err("the job slot's context is a namespace and a job".into());
                }
                if self.namespace.is_empty() {
                    self.namespace = crate::structs::namespace::DEFAULT_NAMESPACE.to_string();
                }
                validate_context_name("job", &self.job)?;
                validate_context_name("namespace", &self.namespace)?;
            }
            Slot::Task => {
                if present(&self.node) {
                    return Err(
                        "the task slot's context is a namespace, a job, a group, and a task".into(),
                    );
                }
                if self.namespace.is_empty() {
                    self.namespace = crate::structs::namespace::DEFAULT_NAMESPACE.to_string();
                }
                validate_context_name("job", &self.job)?;
                validate_context_name("namespace", &self.namespace)?;
                validate_context_name("task", &self.task)?;
                if present(&self.group) {
                    validate_context_name("group", &self.group)?;
                }
            }
            Slot::Node => {
                if [&self.namespace, &self.job, &self.group, &self.task]
                    .iter()
                    .any(|value| present(value))
                {
                    return Err("the node slot's context is a node".into());
                }
                validate_context_name("node", &self.node)?;
            }
        }
        Ok(self)
    }

    /// The canonical id of the context's job, when it has one.
    pub fn job_id(&self) -> Option<String> {
        (!self.job.is_empty())
            .then(|| crate::structs::namespace::job_id(&self.namespace, &self.job))
    }

    /// The scope a namespaced permission is checked on for a job or task
    /// context.
    pub fn job_scope(&self) -> Option<AclScope> {
        self.job_id().map(|job| AclScope::job(&job))
    }

    /// The query string a view request carries: the context's non-empty
    /// fields, in a fixed order.
    pub fn to_query(&self) -> String {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in self.pairs() {
            if !value.is_empty() {
                query.append_pair(key, value);
            }
        }
        query.finish()
    }

    fn pairs(&self) -> [(&'static str, &str); 6] {
        [
            ("slot", &self.slot),
            ("namespace", &self.namespace),
            ("job", &self.job),
            ("group", &self.group),
            ("task", &self.task),
            ("node", &self.node),
        ]
    }

    /// Parse a view request's query string. Unknown or repeated keys are
    /// refused, so a request cannot carry a second context the app might
    /// read instead.
    pub fn from_query(query: &str) -> Result<ViewContext, String> {
        let mut context = ViewContext::default();
        let mut seen = std::collections::BTreeSet::new();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if !seen.insert(key.to_string()) {
                return Err(format!("the query repeats {key:?}"));
            }
            let field = match key.as_ref() {
                "slot" => &mut context.slot,
                "namespace" => &mut context.namespace,
                "job" => &mut context.job,
                "group" => &mut context.group,
                "task" => &mut context.task,
                "node" => &mut context.node,
                other => return Err(format!("the query has an unknown key {other:?}")),
            };
            *field = value.into_owned();
        }
        Ok(context)
    }
}

/// The context a page's assertion carries: the `nav` slot's, which is
/// empty.
pub fn page_context() -> ViewContext {
    ViewContext {
        slot: Slot::Nav.as_str().to_string(),
        ..Default::default()
    }
}

/// What a viewer assertion was signed for.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AssertionKind {
    View,
    Action,
    /// A sandboxed page's bundle (`ui.pages`). Its context is always the
    /// `nav` slot's: pages live in the sidebar.
    Page,
    /// A submit asking the app to check its jobspec extension values
    /// (`POST /legion/v1/extensions/validate`; docs/apps.md, "Jobspec
    /// extensions"). Its context is the job's (`job` slot), its viewer the
    /// submitter, and its inputs digest is the request body's.
    Validate,
}

impl AssertionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssertionKind::View => "view",
            AssertionKind::Action => "action",
            AssertionKind::Page => "page",
            AssertionKind::Validate => "validate",
        }
    }
}

/// How the assertion's permission was checked.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AssertionScope {
    /// On the context (job, task, node slots).
    Context,
    /// On any scope: the viewer holds it somewhere (`nav`, `overview`).
    /// `namespaces` says where, for namespaced resources.
    Any,
    /// No permission was declared; `app:read` on the app sufficed.
    None,
}

/// The `legion.io/viewer` claim of a viewer assertion (apps plan §6.5): who
/// is looking, at what, through which check. The JWT around it carries
/// `aud = app:<name>`, a 60-second `exp`, a `jti`, and the cluster's `iss`,
/// signed with the workload-identity key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ViewerAssertion {
    pub kind: AssertionKind,
    /// The app the request is for (also the audience).
    pub app: String,
    /// The view or action.
    pub name: String,
    /// The viewer's principal (`user:alice`, a token name, a JWT subject).
    pub principal: String,
    /// The viewer's ACL token accessor, when they used one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accessor: Option<String>,
    /// The permission the manager checked (`scale:read`), when one was
    /// declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<String>,
    pub scope: AssertionScope,
    /// For a namespaced permission checked on `any` scope: the namespace
    /// globs in which the viewer holds it (`["*"]` for a management token).
    /// An app showing cluster-wide rows filters by these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<String>,
    /// Where the request was made, exactly as the request carries it.
    pub context: ViewContext,
    /// For an action: [`inputs_digest`] of the inputs the request carries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<String>,
    /// For an action: what the app may do as the viewer when it presents
    /// this assertion back in [`ON_BEHALF_OF_HEADER`]. Never on a view,
    /// page, or validate assertion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<Delegation>,
}

/// What an action's assertion lets its app do as the viewer
/// (docs/auth-and-acls.md, "Delegated identities"): one resource, the
/// action's declared action (and `read` on the same resource when the
/// viewer held it too), on exactly the scope the manager checked.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Delegation {
    /// The ACL resource (`scale`).
    pub resource: String,
    /// The actions: the declared one first, then `read` when it differs and
    /// the viewer holds it on the scope.
    pub actions: Vec<String>,
    /// The scope the declared permission was checked on.
    pub scope: DelegationScope,
}

/// A delegation's scope: the concrete [`AclScope`] the manager checked,
/// attribute by attribute. Empty for a cluster-scoped resource checked
/// cluster-wide; `namespace = "*"` alone for a namespaced resource checked
/// on every namespace (a node slot's `unowned` scope).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DelegationScope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// The job's bare name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The job template the job's latest version came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// The node pool (a node slot's `node` permission).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
}

impl DelegationScope {
    /// The scope as checked. Refuses the `any` scope (list gating, never
    /// delegated) and a value with a `*` in it: rule selectors are globs, so
    /// a literal `web*` would become a wildcard. The `unowned` namespace
    /// (`*` with nothing else) is the one exception: it means every
    /// namespace, which is what was checked.
    pub fn of(scope: &AclScope) -> Result<DelegationScope, String> {
        if scope.any {
            return Err("a permission checked on any scope is not delegated".into());
        }
        let delegated = DelegationScope {
            namespace: scope.namespace.clone(),
            name: scope.name.clone(),
            template: scope.template.clone(),
            pool: scope.pool.clone(),
        };
        delegated.validate()?;
        Ok(delegated)
    }

    /// See [`DelegationScope::of`].
    pub fn validate(&self) -> Result<(), String> {
        let unowned = self.namespace.as_deref() == Some("*")
            && self.name.is_none()
            && self.template.is_none()
            && self.pool.is_none();
        for (what, value) in [
            ("namespace", &self.namespace),
            ("job", &self.name),
            ("template", &self.template),
            ("pool", &self.pool),
        ] {
            let Some(value) = value else { continue };
            if value.is_empty() {
                return Err(format!("the delegation's {what} is empty"));
            }
            if value.contains('*') && !(unowned && what == "namespace") {
                return Err(format!(
                    "the {what} {value:?} has a `*`, which a delegation cannot name exactly"
                ));
            }
        }
        Ok(())
    }

    /// The scope, as the ACL evaluator reads it.
    pub fn acl_scope(&self) -> AclScope {
        AclScope {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            template: self.template.clone(),
            pool: self.pool.clone(),
            ..Default::default()
        }
    }

    /// `default/web`, `pool gpu`, `every namespace`, `the cluster`.
    pub fn label(&self) -> String {
        match (&self.namespace, &self.name, &self.pool) {
            (Some(namespace), Some(name), _) => format!("{namespace}/{name}"),
            (Some(namespace), None, _) if namespace == "*" => "every namespace".to_string(),
            (Some(namespace), None, _) => format!("namespace {namespace}"),
            (None, _, Some(pool)) => format!("pool {pool}"),
            _ => "the cluster".to_string(),
        }
    }
}

impl Delegation {
    /// The delegation of `resource` with `actions` on `scope`.
    pub fn new(
        resource: AclResource,
        actions: &[AclAction],
        scope: &AclScope,
    ) -> Result<Delegation, String> {
        if actions.is_empty() {
            return Err("a delegation names at least one action".into());
        }
        Ok(Delegation {
            resource: resource.as_str().to_string(),
            actions: actions
                .iter()
                .map(|action| action.as_str().to_string())
                .collect(),
            scope: DelegationScope::of(scope)?,
        })
    }

    /// The resource and actions, validated, and the scope checked.
    pub fn parsed(&self) -> Result<(AclResource, Vec<AclAction>), String> {
        let resource = AclResource::parse(&self.resource).ok_or_else(|| {
            format!(
                "the delegation names {:?}, not an ACL resource",
                self.resource
            )
        })?;
        let mut actions = Vec::new();
        for action in &self.actions {
            let action = AclAction::parse(action)
                .ok_or_else(|| format!("the delegation names action {action:?}"))?;
            if !actions.contains(&action) {
                actions.push(action);
            }
        }
        if actions.is_empty() {
            return Err("the delegation names no action".into());
        }
        self.scope.validate()?;
        Ok((resource, actions))
    }

    /// The one allow rule a delegated identity holds: `resource` with
    /// `actions`, and an exact selector for every attribute of the scope.
    /// A rule with no `host` matches no host-level job write.
    pub fn rule(&self, actions: &[AclAction]) -> crate::structs::acl::AclRule {
        let exact = |value: &Option<String>| value.clone().map(|value| vec![value]);
        crate::structs::acl::AclRule {
            resources: vec![self.resource.clone()],
            actions: actions
                .iter()
                .map(|action| action.as_str().to_string())
                .collect(),
            namespaces: exact(&self.scope.namespace),
            names: exact(&self.scope.name),
            pools: exact(&self.scope.pool),
            templates: exact(&self.scope.template),
            ..Default::default()
        }
    }

    /// `scale:write,read on default/web`.
    pub fn label(&self) -> String {
        format!(
            "{}:{} on {}",
            self.resource,
            self.actions.join(","),
            self.scope.label()
        )
    }
}

/// Why an assertion does not match the request it rides on.
pub fn assertion_mismatch(
    assertion: &ViewerAssertion,
    app: &str,
    kind: AssertionKind,
    name: &str,
    context: &ViewContext,
    inputs: Option<&Value>,
) -> Option<String> {
    if assertion.app != app {
        return Some(format!(
            "the assertion is for app {:?}, not {app:?}",
            assertion.app
        ));
    }
    if assertion.kind != kind {
        return Some(format!(
            "the assertion is for a {}, not a {}",
            assertion.kind.as_str(),
            kind.as_str()
        ));
    }
    if assertion.name != name {
        return Some(format!(
            "the assertion is for {} {:?}, not {name:?}",
            kind.as_str(),
            assertion.name
        ));
    }
    if &assertion.context != context {
        return Some(
            "the assertion's context does not match the request's (slot, namespace, job, group, \
             task, node)"
                .to_string(),
        );
    }
    if kind != AssertionKind::Action && assertion.delegation.is_some() {
        return Some(format!(
            "a {} assertion carries no delegation",
            kind.as_str()
        ));
    }
    match (kind, inputs, &assertion.inputs) {
        (AssertionKind::Action | AssertionKind::Validate, Some(inputs), Some(digest))
            if inputs_digest(inputs) == *digest =>
        {
            None
        }
        (AssertionKind::Action | AssertionKind::Validate, _, _) => {
            Some("the assertion's inputs digest does not match the request's inputs".into())
        }
        (AssertionKind::View | AssertionKind::Page, _, None) => None,
        (AssertionKind::View | AssertionKind::Page, _, Some(_)) => {
            Some(format!("a {} assertion carries no inputs", kind.as_str()))
        }
    }
}

/// How a value is shaded.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Tone {
    #[default]
    Neutral,
    Info,
    Ok,
    Warn,
    Danger,
}

impl Tone {
    pub fn as_str(&self) -> &'static str {
        match self {
            Tone::Neutral => "neutral",
            Tone::Info => "info",
            Tone::Ok => "ok",
            Tone::Warn => "warn",
            Tone::Danger => "danger",
        }
    }
}

/// A text value apps may also send as a number or a boolean.
fn de_text<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::String(text) => Ok(text),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(flag) => Ok(flag.to_string()),
        Value::Null => Ok(String::new()),
        _ => Err(serde::de::Error::custom(
            "expected text (a string, number, or boolean)",
        )),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Stat {
    pub label: String,
    #[serde(deserialize_with = "de_text")]
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tone: Option<Tone>,
    /// A smaller line under the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatsBlock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub items: Vec<Stat>,
}

/// Which y axis a line is drawn against.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Axis {
    #[default]
    Left,
    Right,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Series {
    pub name: String,
    #[serde(default, skip_serializing_if = "is_left")]
    pub axis: Axis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// `[unix seconds, value]` pairs, oldest first.
    pub points: Vec<(f64, f64)>,
}

fn is_left(axis: &Axis) -> bool {
    *axis == Axis::Left
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SeriesBlock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub series: Vec<Series>,
}

/// How a table column's values are shown.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    #[default]
    Text,
    Number,
    /// Unix seconds, shown as a time.
    Time,
    /// Seconds, shown as a duration.
    Duration,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Column {
    pub key: String,
    pub label: String,
    #[serde(rename = "type", default, skip_serializing_if = "is_text_column")]
    pub kind: ColumnType,
}

fn is_text_column(kind: &ColumnType) -> bool {
    *kind == ColumnType::Text
}

/// A table cell, a stat, or a key-value with a link or a tone.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RichText {
    #[serde(deserialize_with = "de_text")]
    pub text: String,
    /// An in-app route (`/jobs/default/web`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    /// An `http(s)` URL, opened in a new tab.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tone: Option<Tone>,
}

/// One table cell.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Cell {
    Rich(RichText),
    Number(f64),
    Bool(bool),
    Text(String),
}

impl Cell {
    /// The cell as plain text.
    pub fn text(&self) -> String {
        match self {
            Cell::Rich(rich) => rich.text.clone(),
            Cell::Number(number) => format_number(*number),
            Cell::Bool(flag) => flag.to_string(),
            Cell::Text(text) => text.clone(),
        }
    }
}

/// A number without a trailing `.0`.
pub fn format_number(number: f64) -> String {
    if number.fract() == 0.0 && number.abs() < 1e15 {
        format!("{}", number as i64)
    } else {
        format!("{number}")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableBlock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub columns: Vec<Column>,
    /// Rows keyed by column key; a missing key is an empty cell.
    pub rows: Vec<BTreeMap<String, Cell>>,
    /// Shown when there are no rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimelineItem {
    /// Unix seconds.
    pub at: i64,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tone: Option<Tone>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimelineBlock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Newest first.
    pub items: Vec<TimelineItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyValueItem {
    pub key: String,
    #[serde(deserialize_with = "de_text")]
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tone: Option<Tone>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyValueBlock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub items: Vec<KeyValueItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoticeBlock {
    #[serde(default)]
    pub tone: Tone,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub text: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarkdownBlock {
    /// CommonMark without raw HTML or images; links to `http(s)` or in-app
    /// routes only.
    pub text: String,
}

/// A button bound to a manifest-declared action.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionButton {
    pub action: String,
    /// The button's label (default the action's title).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Input values the button presets; the rest come from the action's
    /// defaults or the form.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub inputs: Map<String, Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionsBlock {
    pub items: Vec<ActionButton>,
}

/// One block of a view (apps plan §6.5). The set is closed: growing it is a
/// Legion release.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Block {
    Stats(StatsBlock),
    Series(SeriesBlock),
    Table(TableBlock),
    Timeline(TimelineBlock),
    KeyValue(KeyValueBlock),
    Notice(NoticeBlock),
    Markdown(MarkdownBlock),
    Actions(ActionsBlock),
}

impl Block {
    pub fn kind(&self) -> &'static str {
        match self {
            Block::Stats(_) => "stats",
            Block::Series(_) => "series",
            Block::Table(_) => "table",
            Block::Timeline(_) => "timeline",
            Block::KeyValue(_) => "keyValue",
            Block::Notice(_) => "notice",
            Block::Markdown(_) => "markdown",
            Block::Actions(_) => "actions",
        }
    }
}

/// A task row's badge (`task` slot `badge` views).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Badge {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tone: Option<Tone>,
}

/// A view document (`legion-view/1`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppView {
    /// Always [`VIEW_FORMAT`]; may be left out of `{ "hidden": true }`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    /// "Nothing here for this context": the slot shows nothing.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hidden: bool,
    /// Overrides the entry's title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub badge: Option<Badge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<Block>,
}

impl AppView {
    /// A view with these blocks.
    pub fn new(blocks: Vec<Block>) -> AppView {
        AppView {
            view: Some(VIEW_FORMAT.to_string()),
            blocks,
            ..Default::default()
        }
    }

    /// "Nothing here for this context."
    pub fn hidden() -> AppView {
        AppView {
            view: Some(VIEW_FORMAT.to_string()),
            hidden: true,
            ..Default::default()
        }
    }

    /// The action names its `actions` blocks reference.
    pub fn referenced_actions(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for block in &self.blocks {
            if let Block::Actions(actions) = block {
                for button in &actions.items {
                    if !names.contains(&button.action) {
                        names.push(button.action.clone());
                    }
                }
            }
        }
        names
    }
}

/// An in-app route: an absolute path on the SPA, never another origin.
pub fn validate_route(route: &str) -> Result<(), String> {
    let valid = route.starts_with('/')
        && !route.starts_with("//")
        && route.len() <= 2048
        && !route
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || c == '\\');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "route {route:?} must be an in-app path starting with a single '/'"
        ))
    }
}

/// An external link: `http` or `https`, nothing else.
pub fn validate_href(href: &str) -> Result<(), String> {
    let url = url::Url::parse(href).map_err(|err| format!("link {href:?}: {err}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "link {href:?} must be http(s); other schemes are refused"
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(format!("link {href:?} must not carry credentials"));
    }
    Ok(())
}

fn validate_link_target(target: &str) -> Result<(), String> {
    if target.starts_with('/') {
        validate_route(target)
    } else if target.starts_with('#') {
        Ok(())
    } else {
        validate_href(target)
    }
}

/// Markdown is CommonMark without raw HTML (no tags, comments, or
/// autolinks in angle brackets) and without images, whose links go only to
/// `http(s)` or in-app routes.
pub fn validate_markdown(text: &str) -> Result<(), String> {
    if text.len() > MAX_MARKDOWN_BYTES {
        return Err(format!(
            "markdown is longer than {MAX_MARKDOWN_BYTES} bytes"
        ));
    }
    let bytes = text.as_bytes();
    for (index, window) in bytes.windows(2).enumerate() {
        if window[0] == b'<' && (window[1].is_ascii_alphabetic() || b"/!?".contains(&window[1])) {
            let end = (index + 24).min(text.len());
            let snippet: String = text[index..].chars().take(end - index).collect::<String>();
            return Err(format!(
                "markdown contains raw HTML ({snippet:?}); views carry CommonMark only, and \
                 links are written [text](url)"
            ));
        }
    }
    if text.contains("![") {
        return Err("markdown images are refused (a view loads nothing from elsewhere)".into());
    }
    // Inline links: `](target)`, and reference definitions: `[x]: target`.
    static INLINE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static REFERENCE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let inline =
        INLINE.get_or_init(|| regex::Regex::new(r"\]\(\s*([^)\s]*)").expect("static regex"));
    for capture in inline.captures_iter(text) {
        let target = capture[1].trim_matches(|c| c == '<' || c == '>');
        if !target.is_empty() {
            validate_link_target(target)?;
        }
    }
    let reference = REFERENCE.get_or_init(|| {
        regex::Regex::new(r"(?m)^\s{0,3}\[[^\]]+\]:\s*(\S+)").expect("static regex")
    });
    for capture in reference.captures_iter(text) {
        validate_link_target(&capture[1])?;
    }
    Ok(())
}

fn check_title(title: &Option<String>) -> Result<(), String> {
    if let Some(title) = title {
        validate_text("title", title, 200)?;
    }
    Ok(())
}

fn check_rich(rich: &RichText) -> Result<(), String> {
    validate_text("text", &rich.text, MAX_TEXT_LEN)?;
    if let Some(route) = &rich.route {
        validate_route(route)?;
    }
    if let Some(href) = &rich.href {
        validate_href(href)?;
    }
    if rich.route.is_some() && rich.href.is_some() {
        return Err("a link has a route or an href, not both".into());
    }
    Ok(())
}

fn validate_block(
    block: &Block,
    actions: Option<&[ActionDecl]>,
    slot: Option<Slot>,
) -> Result<(), String> {
    match block {
        Block::Stats(stats) => {
            check_title(&stats.title)?;
            if stats.items.is_empty() || stats.items.len() > MAX_ITEMS {
                return Err(format!("stats needs 1 to {MAX_ITEMS} items"));
            }
            for stat in &stats.items {
                validate_text("label", &stat.label, 200)?;
                validate_text("value", &stat.value, 200)?;
                if let Some(hint) = &stat.hint {
                    validate_text("hint", hint, 400)?;
                }
            }
        }
        Block::Series(series) => {
            check_title(&series.title)?;
            if series.series.is_empty() || series.series.len() > MAX_SERIES {
                return Err(format!("series draws 1 to {MAX_SERIES} lines"));
            }
            for line in &series.series {
                validate_text("name", &line.name, 200)?;
                if let Some(unit) = &line.unit {
                    validate_text("unit", unit, 32)?;
                }
                if line.points.len() > MAX_POINTS {
                    return Err(format!(
                        "line {:?} has more than {MAX_POINTS} points",
                        line.name
                    ));
                }
                if line
                    .points
                    .iter()
                    .any(|(at, value)| !at.is_finite() || !value.is_finite())
                {
                    return Err(format!(
                        "line {:?} has a point that is not a finite number",
                        line.name
                    ));
                }
            }
        }
        Block::Table(table) => {
            check_title(&table.title)?;
            if table.columns.is_empty() || table.columns.len() > MAX_COLUMNS {
                return Err(format!("table needs 1 to {MAX_COLUMNS} columns"));
            }
            let mut keys = std::collections::BTreeSet::new();
            for column in &table.columns {
                validate_text("column key", &column.key, 64)?;
                validate_text("column label", &column.label, 200)?;
                if !keys.insert(column.key.as_str()) {
                    return Err(format!("table has two columns keyed {:?}", column.key));
                }
            }
            if table.rows.len() > MAX_ROWS {
                return Err(format!("table has more than {MAX_ROWS} rows"));
            }
            for (index, row) in table.rows.iter().enumerate() {
                for (key, cell) in row {
                    if !keys.contains(key.as_str()) {
                        return Err(format!(
                            "table row {index} has a cell {key:?} no column declares"
                        ));
                    }
                    match cell {
                        Cell::Rich(rich) => check_rich(rich)?,
                        Cell::Text(text) => validate_text("cell", text, MAX_TEXT_LEN)?,
                        Cell::Number(number) if !number.is_finite() => {
                            return Err(format!("table row {index}: {key:?} is not finite"))
                        }
                        _ => {}
                    }
                }
            }
            if let Some(empty) = &table.empty {
                validate_text("empty", empty, 400)?;
            }
        }
        Block::Timeline(timeline) => {
            check_title(&timeline.title)?;
            if timeline.items.len() > MAX_ROWS {
                return Err(format!("timeline has more than {MAX_ROWS} entries"));
            }
            for item in &timeline.items {
                validate_text("title", &item.title, 400)?;
                if let Some(detail) = &item.detail {
                    validate_text("detail", detail, MAX_TEXT_LEN)?;
                }
            }
        }
        Block::KeyValue(key_value) => {
            check_title(&key_value.title)?;
            if key_value.items.len() > MAX_ITEMS {
                return Err(format!("keyValue has more than {MAX_ITEMS} items"));
            }
            for item in &key_value.items {
                validate_text("key", &item.key, 200)?;
                validate_text("value", &item.value, MAX_TEXT_LEN)?;
                if let Some(route) = &item.route {
                    validate_route(route)?;
                }
            }
        }
        Block::Notice(notice) => {
            check_title(&notice.title)?;
            validate_text("text", &notice.text, MAX_TEXT_LEN)?;
        }
        Block::Markdown(markdown) => validate_markdown(&markdown.text)?,
        Block::Actions(block) => {
            if block.items.is_empty() || block.items.len() > MAX_ITEMS {
                return Err(format!("actions needs 1 to {MAX_ITEMS} buttons"));
            }
            if let Some(slot) = slot {
                if !slot.has_scope() {
                    return Err(format!(
                        "actions are refused in the {slot} slot: an action's permission is \
                         checked on the job, task, or node it is pressed for, and {slot} has \
                         none"
                    ));
                }
            }
            for button in &block.items {
                if let Some(label) = &button.label {
                    validate_text("label", label, 80)?;
                }
                if let Some(actions) = actions {
                    let Some(action) = actions.iter().find(|a| a.name == button.action) else {
                        return Err(format!(
                            "button for action {:?}, which the manifest does not declare",
                            button.action
                        ));
                    };
                    for key in button.inputs.keys() {
                        if !action.inputs.contains_key(key) {
                            return Err(format!(
                                "button for {:?} presets input {key:?}, which it does not take",
                                button.action
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Parse and check a view document an app answered with.
///
/// - The document is `legion-view/1` (or `{ "hidden": true }`), at most
///   [`MAX_VIEW_BYTES`], with at most [`MAX_BLOCKS`] blocks.
/// - Every block is one of the closed set, with no unknown fields; limits
///   hold (four lines per series, left and right axes only, ...).
/// - Markdown carries no raw HTML or images, and every link is `http(s)` or
///   an in-app route.
/// - With `actions`, every button names a declared action and presets only
///   inputs it takes; with `slot`, an `actions` block appears only in a slot
///   that has a scope.
pub fn validate_view(
    raw: &[u8],
    actions: Option<&[ActionDecl]>,
    slot: Option<Slot>,
) -> Result<AppView, String> {
    if raw.len() > MAX_VIEW_BYTES {
        return Err(format!(
            "the view is {} bytes; the limit is {MAX_VIEW_BYTES}",
            raw.len()
        ));
    }
    let value: Value =
        serde_json::from_slice(raw).map_err(|err| format!("the view is not JSON: {err}"))?;
    validate_view_value(&value, actions, slot)
}

/// [`validate_view`] for a parsed document.
pub fn validate_view_value(
    value: &Value,
    actions: Option<&[ActionDecl]>,
    slot: Option<Slot>,
) -> Result<AppView, String> {
    let Some(object) = value.as_object() else {
        return Err("the view must be a JSON object".into());
    };
    const FIELDS: &[&str] = &["view", "hidden", "title", "badge", "blocks"];
    if let Some(unknown) = object.keys().find(|key| !FIELDS.contains(&key.as_str())) {
        return Err(format!(
            "the view has an unknown field {unknown:?} (it has {})",
            FIELDS.join(", ")
        ));
    }
    let hidden = match object.get("hidden") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(_) => return Err("`hidden` must be true or false".into()),
    };
    let format = object.get("view").and_then(Value::as_str);
    match format {
        Some(VIEW_FORMAT) => {}
        None if hidden => {}
        Some(other) => {
            return Err(format!(
                "the view is {other:?}; this Legion renders {VIEW_FORMAT}"
            ))
        }
        None => return Err(format!("the view has no `view: \"{VIEW_FORMAT}\"`")),
    }
    let title = match object.get("title") {
        None | Some(Value::Null) => None,
        Some(Value::String(title)) => Some(title.clone()),
        Some(_) => return Err("`title` must be a string".into()),
    };
    check_title(&title)?;
    let badge = match object.get("badge") {
        None | Some(Value::Null) => None,
        Some(badge) => {
            let badge: Badge =
                serde_json::from_value(badge.clone()).map_err(|err| format!("badge: {err}"))?;
            validate_text("badge", &badge.text, 64)?;
            Some(badge)
        }
    };
    let raw_blocks = match object.get("blocks") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(blocks)) => blocks.clone(),
        Some(_) => return Err("`blocks` must be a list".into()),
    };
    if raw_blocks.len() > MAX_BLOCKS {
        return Err(format!("the view has more than {MAX_BLOCKS} blocks"));
    }
    let mut blocks = Vec::with_capacity(raw_blocks.len());
    for (index, raw) in raw_blocks.into_iter().enumerate() {
        let kind = raw
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("(no type)")
            .to_string();
        let block: Block = serde_json::from_value(raw).map_err(|err| {
            format!(
                "block {index} ({kind}): {err} (the blocks are stats, series, table, timeline, \
                 keyValue, notice, markdown, and actions)"
            )
        })?;
        validate_block(&block, actions, slot)
            .map_err(|err| format!("block {index} ({kind}): {err}"))?;
        blocks.push(block);
    }
    Ok(AppView {
        view: format.map(str::to_string),
        hidden,
        title,
        badge,
        blocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(slot: &str, namespace: &str, job: &str) -> ViewContext {
        ViewContext {
            slot: slot.into(),
            namespace: namespace.into(),
            job: job.into(),
            ..Default::default()
        }
    }

    fn pause() -> ActionDecl {
        serde_json::from_value(json!({
            "name": "pause",
            "title": "Pause autoscaling",
            "confirm": "Pause autoscaling for {job}?",
            "inputs": { "for": { "type": "duration", "default": "1h" } },
            "permission": { "resource": "scale", "action": "write" },
        }))
        .expect("action")
    }

    /// The plan's example view (§6.5) validates as written.
    #[test]
    fn the_plans_example_view_validates() {
        let raw = json!({ "view": "legion-view/1", "blocks": [
            { "type": "stats", "items": [
                { "label": "Now", "value": "6" }, { "label": "Bounds", "value": "2–8" },
                { "label": "CPU", "value": "91%", "tone": "warn" } ] },
            { "type": "series", "title": "Replicas vs CPU (1h)", "series": [
                { "name": "replicas", "points": [[1792412400, 4], [1792412460, 6]] },
                { "name": "cpu", "axis": "right", "unit": "%", "points": [[1792412400, 91]] } ] },
            { "type": "timeline", "items": [
                { "at": 1792412545, "title": "4 → 6", "detail": "cpu 0.91 vs 0.70 (4 ready)" } ] },
            { "type": "actions", "items": [{ "action": "pause", "label": "Pause 1h" }] }
        ] });
        let view = validate_view(
            raw.to_string().as_bytes(),
            Some(&[pause()]),
            Some(Slot::Job),
        )
        .expect("valid");
        assert_eq!(view.blocks.len(), 4);
        assert_eq!(view.referenced_actions(), vec!["pause".to_string()]);
        // It round-trips.
        let again = serde_json::to_value(&view).expect("serializes");
        validate_view_value(&again, Some(&[pause()]), Some(Slot::Job)).expect("round trip");
    }

    /// Unknown blocks, unknown fields, and raw HTML are refused (plan §9,
    /// A4).
    #[test]
    fn unknown_blocks_fields_and_raw_html_are_refused() {
        let refuse = |view: Value, needle: &str| {
            let err = validate_view_value(&view, None, None).expect_err("refused");
            assert!(err.contains(needle), "{err}");
        };
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "iframe", "src": "x" }] }),
            "unknown variant",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "notice", "text": "x", "html": "<b>" }] }),
            "unknown field",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "markdown", "text": "hi <script>alert(1)</script>" }] }),
            "raw HTML",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "markdown", "text": "<!-- x -->" }] }),
            "raw HTML",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "markdown", "text": "[x](javascript:alert(1))" }] }),
            "http(s)",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "markdown", "text": "![t](https://x/p.png)" }] }),
            "images",
        );
        refuse(
            json!({ "view": "legion-view/1", "extra": 1 }),
            "unknown field",
        );
        refuse(json!({ "view": "legion-view/2" }), "legion-view/2");
        refuse(json!({ "blocks": [] }), "legion-view/1");
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "table",
                "columns": [{ "key": "a", "label": "A" }],
                "rows": [{ "a": { "text": "x", "href": "file:///etc/passwd" } }] }] }),
            "http(s)",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "table",
                "columns": [{ "key": "a", "label": "A" }],
                "rows": [{ "a": { "text": "x", "route": "//evil.example" } }] }] }),
            "in-app path",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "series", "series": [
                { "name": "a", "points": [] }, { "name": "b", "points": [] },
                { "name": "c", "points": [] }, { "name": "d", "points": [] },
                { "name": "e", "points": [] } ] }] }),
            "1 to 4 lines",
        );
        refuse(
            json!({ "view": "legion-view/1", "blocks": [{ "type": "series", "series": [
                { "name": "a", "axis": "middle", "points": [] } ] }] }),
            "unknown variant",
        );
        // Markdown with in-app and https links is fine.
        validate_view_value(
            &json!({ "view": "legion-view/1", "blocks": [{ "type": "markdown",
                "text": "See [the job](/jobs/default/web) and [docs](https://example.com/x). 2 < 3." }] }),
            None,
            None,
        )
        .expect("plain markdown");
        // `{ "hidden": true }` needs no format.
        assert!(
            validate_view_value(&json!({ "hidden": true }), None, None)
                .expect("hidden")
                .hidden
        );
    }

    /// An `actions` block names declared actions only, presets only their
    /// inputs, and only appears where there is a scope.
    #[test]
    fn action_buttons_are_checked_against_the_manifest() {
        let view = |button: Value| json!({ "view": "legion-view/1", "blocks": [{ "type": "actions", "items": [button] }] });
        let actions = [pause()];
        validate_view_value(
            &view(json!({ "action": "pause", "inputs": { "for": "2h" } })),
            Some(&actions),
            Some(Slot::Job),
        )
        .expect("declared");
        let err = validate_view_value(
            &view(json!({ "action": "drain" })),
            Some(&actions),
            Some(Slot::Job),
        )
        .expect_err("undeclared");
        assert!(err.contains("does not declare"), "{err}");
        let err = validate_view_value(
            &view(json!({ "action": "pause", "inputs": { "job": "other" } })),
            Some(&actions),
            Some(Slot::Job),
        )
        .expect_err("unknown preset");
        assert!(err.contains("does not take"), "{err}");
        let err = validate_view_value(
            &view(json!({ "action": "pause" })),
            Some(&actions),
            Some(Slot::Nav),
        )
        .expect_err("no scope");
        assert!(err.contains("nav slot"), "{err}");
    }

    #[test]
    fn the_ui_block_validates_entries_actions_and_the_port() {
        let ports = vec!["api".to_string()];
        let ui = UiSpec::parse(&json!({
            "nav": [{ "title": "Autoscaling", "icon": "gauge", "view": "overview" }],
            "job": [{ "title": "Autoscaling", "view": "job",
                       "permission": { "resource": "scale", "action": "read" } }],
            "task": [{ "badge": "task-badge", "permission": { "resource": "scale", "action": "read" } }],
            "node": [{ "title": "Node group", "view": "node", "permission": { "resource": "node", "action": "read" } }],
            "actions": [{ "name": "pause", "title": "Pause autoscaling",
                "confirm": "Pause autoscaling for {job}?",
                "inputs": { "for": { "type": "duration", "default": "1h" } },
                "permission": { "resource": "scale", "action": "write" } }],
            "refresh": "10s",
        }))
        .expect("the plan's block parses");
        ui.validate(&ports).expect("valid");
        assert_eq!(ui.refresh_secs().unwrap(), Some(10));
        assert_eq!(ui.port(), "api");
        assert_eq!(
            ui.slots(),
            vec![Slot::Nav, Slot::Job, Slot::Task, Slot::Node]
        );
        // No port to serve on.
        let err = ui.validate(&[]).expect_err("no port");
        assert!(err.contains("no task"), "{err}");
        // Unknown fields, unknown resources, bad names, reserved inputs.
        assert!(
            UiSpec::parse(&json!({ "nav": [{ "title": "x", "view": "x", "colour": 1 }] })).is_err()
        );
        let bad = |ui: Value| {
            UiSpec::parse(&ui)
                .expect("parses")
                .validate(&ports)
                .expect_err("invalid")
        };
        assert!(bad(json!({ "job": [{ "title": "x", "view": "x", "permission": { "resource": "pods", "action": "read" } }] })).contains("not an ACL resource"));
        assert!(
            bad(json!({ "job": [{ "title": "x", "view": "Bad Name" }] })).contains("view name")
        );
        assert!(bad(json!({ "job": [{ "title": "x" }] })).contains("`view` is required"));
        assert!(bad(json!({ "actions": [{ "name": "go", "title": "Go",
            "inputs": { "job": { "type": "string" } },
            "permission": { "resource": "job", "action": "write" } }] }))
        .contains("names the context"));
        assert!(bad(json!({ "actions": [{ "name": "go", "title": "Go",
            "inputs": { "mode": { "type": "enum" } },
            "permission": { "resource": "job", "action": "write" } }] }))
        .contains("needs `values`"));
        assert!(bad(json!({ "refresh": "1s" })).contains("between"));
        // An action without a permission does not parse.
        assert!(UiSpec::parse(&json!({ "actions": [{ "name": "go", "title": "Go" }] })).is_err());
    }

    /// `ui.pages` is typed and checked at install (apps plan §6.5, tier 2):
    /// names, titles, bundles under `/legion/v1/`, and an `rpcs` allowlist
    /// of Manager RPCs pages may call, never one that mints a credential.
    #[test]
    fn pages_are_typed_and_their_rpcs_checked() {
        let ports = vec!["api".to_string()];
        let ui = UiSpec::parse(&json!({
            "pages": [
                { "name": "console", "title": "Console", "icon": "terminal",
                  "rpcs": ["GetJobInfo", "StopJob"],
                  "permission": { "resource": "job", "action": "read" } },
                { "name": "graph", "title": "Graph", "bundle": "/legion/v1/graph.html" },
            ],
        }))
        .expect("parses");
        ui.validate(&ports).expect("valid");
        assert!(ui.serves_anything(), "pages are served on the UI port");
        assert!(ui.validate(&[]).is_err(), "pages need the UI port");
        let console = ui.page("console").expect("console");
        assert_eq!(console.bundle_path(), "/legion/v1/pages/console");
        assert!(console.allows_rpc("GetJobInfo") && console.allows_rpc("StopJob"));
        assert!(!console.allows_rpc("DestroyJob"), "not listed");
        assert_eq!(
            ui.page("graph").unwrap().bundle_path(),
            "/legion/v1/graph.html"
        );
        assert_eq!(page_rpc_kind("StopJob"), Some(PageRpcKind::Write));
        assert_eq!(page_rpc_kind("GetJobInfo"), Some(PageRpcKind::Read));
        // `RestoreInstance` needs only snapshot:read, but it creates an
        // instance: it is confirmed like any write.
        assert_eq!(page_rpc_kind("RestoreInstance"), Some(PageRpcKind::Write));

        let bad = |page: Value| {
            UiSpec::parse(&json!({ "pages": [page] }))
                .and_then(|ui| ui.validate(&ports))
                .expect_err("invalid")
        };
        assert!(
            bad(json!({ "name": "c", "title": "C", "rpcs": ["CreateAclToken"] }))
                .contains("credential")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "rpcs": ["StreamEvents"] })).contains("unary")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "rpcs": ["GetAppView"] }))
                .contains("legion.view")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "rpcs": ["Heartbeat"] })).contains("workers")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "rpcs": ["DropTables"] }))
                .contains("not a Manager RPC")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "rpcs": ["GetJobInfo", "GetJobInfo"] }))
                .contains("twice")
        );
        assert!(bad(json!({ "name": "Console", "title": "C" })).contains("page name"));
        assert!(bad(json!({ "name": "c", "title": " " })).contains("title"));
        assert!(
            bad(json!({ "name": "c", "title": "C", "bundle": "/admin" })).contains("/legion/v1/")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "bundle": "/legion/v1/../x" }))
                .contains("plain path")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "bundle": "/legion/v1/p?x=1" }))
                .contains("plain path")
        );
        assert!(
            bad(json!({ "name": "c", "title": "C", "bundle": "/legion/v1/views/x" }))
                .contains("views")
        );
        assert!(
            UiSpec::parse(&json!({ "pages": [{ "name": "c", "title": "C", "html": "<b>" }] }))
                .is_err()
        );
        let twice = UiSpec::parse(&json!({ "pages": [
            { "name": "c", "title": "C" }, { "name": "c", "title": "D" } ] }))
        .unwrap()
        .validate(&ports)
        .unwrap_err();
        assert!(twice.contains("two pages"), "{twice}");
    }

    /// Every RPC pages may call is a unary Manager RPC of this build's
    /// proto, and none is also on the refused list.
    #[test]
    fn page_rpcs_are_unary_manager_rpcs() {
        let proto = include_str!("../../proto/legion.proto");
        let manager = proto
            .split("service Manager {")
            .nth(1)
            .and_then(|rest| rest.split("\nservice ").next())
            .expect("Manager service in proto");
        // Declarations may wrap: join each `rpc` with what follows until `;`.
        let mut declarations: BTreeMap<String, String> = BTreeMap::new();
        let mut current: Option<(String, String)> = None;
        for line in manager.lines().map(str::trim) {
            if let Some(rest) = line.strip_prefix("rpc ") {
                let name = rest
                    .split(|c: char| c == '(' || c.is_whitespace())
                    .next()
                    .unwrap_or_default()
                    .to_string();
                current = Some((name, rest.to_string()));
            } else if let Some((_, text)) = current.as_mut() {
                text.push(' ');
                text.push_str(line);
            }
            if line.ends_with(';') {
                if let Some((name, text)) = current.take() {
                    declarations.insert(name, text);
                }
            }
        }
        assert!(
            declarations.len() > 100,
            "parsed {} RPCs",
            declarations.len()
        );
        for (method, _) in PAGE_RPCS {
            let declaration = declarations
                .get(*method)
                .unwrap_or_else(|| panic!("{method} is not a Manager RPC"));
            assert!(
                !declaration.contains("stream "),
                "{method} streams: {declaration}"
            );
            assert!(
                page_rpc_refusal(method).contains("not a Manager RPC"),
                "{method} is both allowed and refused"
            );
        }
        // Standalone scaling policies (autoscaler Phases 3 and 4) arrived
        // on a branch that predated the allowlist.
        assert_eq!(
            page_rpc_kind("ApplyScalingPolicy"),
            Some(PageRpcKind::Write)
        );
        assert_eq!(
            page_rpc_kind("DeleteScalingPolicy"),
            Some(PageRpcKind::Write)
        );
        assert_eq!(
            page_rpc_kind("ListScalingPolicies"),
            Some(PageRpcKind::Read)
        );
        for refused in [
            "CreateAclToken",
            "StreamEvents",
            "GetAppView",
            "Heartbeat",
            "ResetKeyring",
        ] {
            assert!(declarations.contains_key(refused), "{refused}");
            assert!(page_rpc_kind(refused).is_none(), "{refused}");
        }
    }

    #[test]
    fn inputs_are_typed_defaulted_and_closed() {
        let action = pause();
        assert_eq!(
            resolve_inputs(&action, &Value::Null).unwrap(),
            json!({ "for": "1h" })
        );
        assert_eq!(
            resolve_inputs(&action, &json!({ "for": "15m" })).unwrap(),
            json!({ "for": "15m" })
        );
        assert!(resolve_inputs(&action, &json!({ "for": 3 })).is_err());
        assert!(resolve_inputs(&action, &json!({ "for": "soon" })).is_err());
        assert!(resolve_inputs(&action, &json!({ "target": "x" }))
            .unwrap_err()
            .contains("takes no input"));
        // The digest ignores key order.
        assert_eq!(
            inputs_digest(&json!({ "a": 1, "b": 2 })),
            inputs_digest(&json!({ "b": 2, "a": 1 }))
        );
        assert_eq!(
            action.confirmation(&context("job", "payments", "api")),
            Some("Pause autoscaling for payments/api?".into())
        );
    }

    #[test]
    fn contexts_normalize_per_slot_and_round_trip_through_the_query() {
        let job = ViewContext {
            slot: "job".into(),
            job: "payments/api".into(),
            ..Default::default()
        }
        .normalized()
        .expect("job");
        assert_eq!(job.namespace, "payments");
        assert_eq!(job.job, "api");
        assert_eq!(job.job_id().as_deref(), Some("payments/api"));
        let query = job.to_query();
        assert_eq!(query, "slot=job&namespace=payments&job=api");
        assert_eq!(ViewContext::from_query(&query).unwrap(), job);
        assert_eq!(
            context("job", "", "web").normalized().unwrap().namespace,
            "default"
        );
        assert!(context("nav", "", "web").normalized().is_err());
        assert!(ViewContext {
            slot: "node".into(),
            ..Default::default()
        }
        .normalized()
        .is_err());
        assert!(ViewContext::from_query("slot=job&job=a&job=b").is_err());
        assert!(ViewContext::from_query("slot=job&evil=1").is_err());
        assert!(Slot::parse("sidebar").is_err());
    }

    #[test]
    fn an_assertion_matches_only_its_own_request() {
        let ctx = context("job", "default", "web");
        let inputs = json!({ "for": "1h" });
        let assertion = ViewerAssertion {
            kind: AssertionKind::Action,
            app: "autoscaler".into(),
            name: "pause".into(),
            principal: "user:alice".into(),
            accessor: None,
            permission: Some("scale:write".into()),
            scope: AssertionScope::Context,
            namespaces: Vec::new(),
            context: ctx.clone(),
            inputs: Some(inputs_digest(&inputs)),
            delegation: None,
        };
        let check = |app, kind, name, ctx: &ViewContext, inputs: Option<&Value>| {
            assertion_mismatch(&assertion, app, kind, name, ctx, inputs)
        };
        assert_eq!(
            check(
                "autoscaler",
                AssertionKind::Action,
                "pause",
                &ctx,
                Some(&inputs)
            ),
            None
        );
        assert!(check(
            "gitops",
            AssertionKind::Action,
            "pause",
            &ctx,
            Some(&inputs)
        )
        .is_some());
        assert!(check("autoscaler", AssertionKind::View, "pause", &ctx, None).is_some());
        assert!(check(
            "autoscaler",
            AssertionKind::Action,
            "resume",
            &ctx,
            Some(&inputs)
        )
        .is_some());
        assert!(check(
            "autoscaler",
            AssertionKind::Action,
            "pause",
            &context("job", "default", "other"),
            Some(&inputs)
        )
        .is_some());
        assert!(check(
            "autoscaler",
            AssertionKind::Action,
            "pause",
            &ctx,
            Some(&json!({ "for": "9h" }))
        )
        .is_some());
    }

    /// A delegation names exactly the scope checked, round-trips as the
    /// claim, refuses `any` and literal globs, and only an action's
    /// assertion may carry one.
    #[test]
    fn a_delegation_is_the_checked_scope_exactly_test() {
        let scope = AclScope::job("payments/web").with_template(Some("svc".into()));
        let delegation = Delegation::new(
            AclResource::Scale,
            &[AclAction::Write, AclAction::Read],
            &scope,
        )
        .expect("a job scope");
        assert_eq!(
            serde_json::to_value(&delegation).unwrap(),
            json!({ "resource": "scale", "actions": ["write", "read"],
                    "scope": { "namespace": "payments", "name": "web", "template": "svc" } })
        );
        assert_eq!(delegation.label(), "scale:write,read on payments/web");
        let (resource, actions) = delegation.parsed().expect("valid");
        assert_eq!(resource, AclResource::Scale);
        assert_eq!(actions, vec![AclAction::Write, AclAction::Read]);
        let rule = delegation.rule(&actions);
        assert_eq!(rule.namespaces, Some(vec!["payments".to_string()]));
        assert_eq!(rule.names, Some(vec!["web".to_string()]));
        assert_eq!(rule.templates, Some(vec!["svc".to_string()]));
        assert!(rule.pools.is_none() && rule.host.is_none() && !rule.is_deny());

        let pool = Delegation::new(
            AclResource::Node,
            &[AclAction::Write],
            &AclScope::pool("gpu"),
        )
        .expect("a pool scope");
        assert_eq!(pool.scope.label(), "pool gpu");
        assert_eq!(
            pool.rule(&[AclAction::Write]).pools,
            Some(vec!["gpu".into()])
        );
        let unowned = Delegation::new(AclResource::Job, &[AclAction::Read], &AclScope::unowned())
            .expect("every namespace is a scope");
        assert_eq!(unowned.scope.label(), "every namespace");
        assert_eq!(
            Delegation::new(AclResource::Node, &[AclAction::Read], &AclScope::none())
                .expect("cluster-wide")
                .scope,
            DelegationScope::default()
        );

        assert!(
            Delegation::new(AclResource::Scale, &[AclAction::Write], &AclScope::any())
                .unwrap_err()
                .contains("any scope")
        );
        assert!(Delegation::new(
            AclResource::Scale,
            &[AclAction::Write],
            &AclScope::job("web*")
        )
        .unwrap_err()
        .contains('*'));
        assert!(Delegation::new(AclResource::Scale, &[], &scope).is_err());
        let mut forged = delegation.clone();
        forged.scope.namespace = Some("*".into());
        assert!(forged.parsed().is_err(), "`*` with a job is not `unowned`");
        let mut forged = delegation.clone();
        forged.actions = vec!["admin".into()];
        assert!(forged.parsed().is_err());
        assert!(serde_json::from_value::<Delegation>(json!({
            "resource": "scale", "actions": ["write"], "scope": {}, "extra": 1
        }))
        .is_err());

        // Only an action's assertion carries one.
        let ctx = context("job", "payments", "web");
        let view = ViewerAssertion {
            kind: AssertionKind::View,
            app: "autoscaler".into(),
            name: "job".into(),
            principal: "alice".into(),
            accessor: None,
            permission: None,
            scope: AssertionScope::None,
            namespaces: Vec::new(),
            context: ctx.clone(),
            inputs: None,
            delegation: Some(delegation),
        };
        let mismatch =
            assertion_mismatch(&view, "autoscaler", AssertionKind::View, "job", &ctx, None)
                .expect("refused");
        assert!(mismatch.contains("no delegation"), "{mismatch}");
    }
}
