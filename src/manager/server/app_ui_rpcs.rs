//! App UI extensions (docs/apps.md, "Views and actions" and "Sandboxed
//! pages"; apps plan §6.5, calls entry 11): `ListUiExtensions`, `GetAppView`,
//! and `InvokeAppAction`, the one data path the SPA, `legion app view`, and
//! the `legion_app_view` MCP tool share (tier 1), and `GetAppPage`, which
//! fetches a sandboxed page's bundle for the SPA (tier 2).
//!
//! The manager authorizes before anything reaches an app:
//!
//! 1. `app:read` on the app (`names` selector);
//! 2. the view must be registered in the context's slot, or the action
//!    declared and pressed in a slot that has a scope;
//! 3. the entry's or action's declared `permission`, checked as the viewer
//!    on the context's scope ([`ManagerDaemonService::ui_permission_scope`]).
//!
//! Only then does it sign a viewer assertion (`aud = app:<name>`, 60
//! seconds, the workload-identity key) carrying who is looking, what was
//! checked, and the context exactly as the request carries it, and forward
//! the request to one allocation of the app's job over the worker plane. The
//! answer is validated against the view schema before anyone sees it, and
//! every action is recorded as an `app.action` event.
//!
//! An action's assertion also carries a [`Delegation`]: the declared
//! permission (and `read` on the same resource, when the viewer holds it)
//! on exactly the scope checked. The app presents the assertion back in
//! `legion-on-behalf-of` to act as the viewer, within its own grant
//! (`delegation.rs`; docs/auth-and-acls.md, "Delegated identities").

use super::*;
use crate::storage::app_store::AppUiAllocation;
use crate::structs::app::AppRecord;
use crate::structs::app_ui::{
    inputs_digest, page_context, page_rpc_kind, resolve_inputs, validate_view, validate_view_value,
    ActionDecl, AppView, AssertionKind, AssertionScope, Delegation, PageRpcKind, Slot, SlotEntry,
    UiPermission, UiSpec, ViewContext, ViewerAssertion, ACTION_PATH_PREFIX, ASSERTION_CLAIM,
    ASSERTION_TTL_SECS, MAX_PAGE_BYTES, VIEWER_HEADER, VIEW_PATH_PREFIX,
};
use crate::structs::{
    AppActionInfo, AppHttpHeader, AppPageRpc, AppViewContext, ForwardAppRequestReply,
    ForwardAppRequestRequest, GetAppPageReply, GetAppPageRequest, GetAppViewReply,
    GetAppViewRequest, InvokeAppActionReply, InvokeAppActionRequest, ListUiExtensionsReply,
    ListUiExtensionsRequest, UiExtension,
};

/// Allocations a request is tried on before giving up (only when one could
/// not be reached at all).
const FORWARD_ATTEMPTS: usize = 3;

/// The longest app message an error or event repeats.
const MAX_APP_MESSAGE: usize = 500;

fn context_from_proto(context: Option<AppViewContext>) -> Result<ViewContext, Status> {
    let Some(context) = context else {
        return Err(Status::invalid_argument(
            "a context is required: at least its slot (nav, overview, job, task, or node)",
        ));
    };
    ViewContext {
        slot: context.slot,
        namespace: context.namespace,
        job: context.job,
        group: context.group,
        task: context.task,
        node: context.node,
    }
    .normalized()
    .map_err(Status::invalid_argument)
}

fn context_to_proto(context: &ViewContext) -> AppViewContext {
    AppViewContext {
        slot: context.slot.clone(),
        namespace: context.namespace.clone(),
        job: context.job.clone(),
        group: context.group.clone(),
        task: context.task.clone(),
        node: context.node.clone(),
    }
}

fn permission_to_proto(permission: &UiPermission) -> crate::structs::UiPermission {
    crate::structs::UiPermission {
        resource: permission.resource.trim().to_string(),
        action: permission.action.trim().to_string(),
    }
}

/// An app's answer, cut to a length an error message or an event can carry.
fn app_message(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| text.trim().to_string());
    let mut text: String = text.chars().filter(|c| !c.is_control()).collect();
    if text.len() > MAX_APP_MESSAGE {
        let mut end = MAX_APP_MESSAGE;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
    if text.is_empty() {
        "(no message)".to_string()
    } else {
        text
    }
}

/// The status an app's non-2xx answer becomes. Never PermissionDenied or
/// Unauthenticated: the caller was authorized, and an app that refuses the
/// assertion is misbehaving, not judging the caller.
fn app_error(app: &str, what: &str, reply: &ForwardAppRequestReply) -> Status {
    let message = app_message(&reply.body);
    match reply.status {
        404 => Status::not_found(format!("app {app} does not serve {what} ({message})")),
        401 | 403 => Status::failed_precondition(format!(
            "app {app} refused the viewer assertion for {what}: {message}. The manager \
             authorized you; the app could not verify the assertion (does it run with workload \
             identity, and the cluster's keys?)"
        )),
        400..=499 => Status::failed_precondition(format!("app {app}: {what}: {message}")),
        503 => Status::unavailable(format!("app {app} is not ready to serve {what}: {message}")),
        _ => Status::unavailable(format!(
            "app {app} failed to serve {what} (HTTP {}): {message}",
            reply.status
        )),
    }
}

/// A declared permission, checked as the viewer: how the assertion
/// describes the check, and the concrete scope it was checked on.
struct CheckedPermission {
    scope: AssertionScope,
    /// For a namespaced permission checked on any scope: where the viewer
    /// holds it.
    namespaces: Vec<String>,
    /// The resource, action, and concrete scope checked; `None` when no
    /// permission was declared or it was checked on any scope.
    checked: Option<(AclResource, AclAction, AclScope)>,
}

/// What a view request resolved to.
struct ResolvedView<'a> {
    entry: &'a SlotEntry,
    view: String,
}

impl ManagerDaemonService {
    /// The scope a declared permission on `resource` is checked on for
    /// `context` (docs/apps.md, "Who sees what"):
    ///
    /// - `nav`, `overview`: any scope (the viewer holds it somewhere);
    /// - `job`, `task`: the job (namespace and name, with its template
    ///   provenance) for a namespaced resource; cluster-wide otherwise;
    /// - `node`: the node's pool for `node`; every namespace (`unowned`) for
    ///   a namespaced resource; cluster-wide otherwise.
    ///
    /// A node is looked up only after a coarse check on any scope, so a
    /// caller without the permission anywhere cannot learn which node ids
    /// exist. `None` means the node does not exist.
    fn ui_permission_scope(
        &self,
        identity: &AclIdentity,
        resource: AclResource,
        action: AclAction,
        context: &ViewContext,
    ) -> Result<Option<AclScope>, Status> {
        let slot = context.slot().map_err(Status::invalid_argument)?;
        Ok(Some(match slot {
            Slot::Nav | Slot::Overview => AclScope::any(),
            Slot::Job | Slot::Task if resource.is_namespaced() => {
                let job = context
                    .job_id()
                    .ok_or_else(|| Status::invalid_argument("the context names no job"))?;
                self.job_read_scope(&job)
            }
            Slot::Job | Slot::Task => AclScope::none(),
            Slot::Node if resource == AclResource::Node => {
                if !identity.allows(resource, action, &AclScope::any()) {
                    return Ok(None);
                }
                match self.store.get_node(&context.node) {
                    Ok(node) => AclScope::pool(&node.pool),
                    Err(_) => return Ok(None),
                }
            }
            Slot::Node if resource.is_namespaced() => AclScope::unowned(),
            Slot::Node => AclScope::none(),
        }))
    }

    /// Whether `identity` holds `permission` on `context` (no audit log; for
    /// filtering lists and marking buttons).
    fn ui_permission_allows(
        &self,
        identity: &AclIdentity,
        permission: Option<&UiPermission>,
        context: Option<&ViewContext>,
    ) -> bool {
        let Some(permission) = permission else {
            return true;
        };
        let Ok((resource, action)) = permission.parsed() else {
            return false;
        };
        let scope = match context {
            None => Some(AclScope::any()),
            Some(context) => self
                .ui_permission_scope(identity, resource, action, context)
                .ok()
                .flatten(),
        };
        scope.is_some_and(|scope| identity.allows(resource, action, &scope))
    }

    /// Enforce `permission` on `context` as the caller, the way the request
    /// will be described to the app: the assertion's scope, and for a
    /// namespaced permission checked on any scope, where the caller holds
    /// it.
    fn enforce_ui_permission(
        &self,
        identity: &AclIdentity,
        permission: Option<&UiPermission>,
        context: &ViewContext,
    ) -> Result<CheckedPermission, Status> {
        let Some(permission) = permission else {
            return Ok(CheckedPermission {
                scope: AssertionScope::None,
                namespaces: Vec::new(),
                checked: None,
            });
        };
        let (resource, action) = permission.parsed().map_err(|err| {
            Status::failed_precondition(format!("the app's manifest is invalid: {err}"))
        })?;
        let scope = match self.ui_permission_scope(identity, resource, action, context)? {
            Some(scope) => scope,
            None if context.slot == Slot::Node.as_str() => {
                // Coarse first: without the permission anywhere, the caller
                // is refused before learning whether the node exists.
                acl::enforce(identity, resource, action, &AclScope::any())?;
                return Err(Status::not_found(format!(
                    "node {:?} not found",
                    context.node
                )));
            }
            None => return Err(Status::not_found("the context's target was not found")),
        };
        acl::enforce(identity, resource, action, &scope)?;
        if scope.any {
            let namespaces = if !resource.is_namespaced() {
                Vec::new()
            } else if identity.is_management {
                vec!["*".to_string()]
            } else {
                identity.namespaces_granting(resource, action)
            };
            Ok(CheckedPermission {
                scope: AssertionScope::Any,
                namespaces,
                checked: None,
            })
        } else {
            Ok(CheckedPermission {
                scope: AssertionScope::Context,
                namespaces: Vec::new(),
                checked: Some((resource, action, scope)),
            })
        }
    }

    /// What an action's assertion lets its app do as the viewer: the
    /// declared permission on the scope it was checked on, and `read` on
    /// the same resource and scope when the viewer holds that too
    /// (app-actions-as-viewer calls entry 8). A scope value with a `*` in
    /// it is refused: a delegation names its scope exactly.
    fn delegation_for(
        identity: &AclIdentity,
        resource: AclResource,
        action: AclAction,
        scope: &AclScope,
    ) -> Result<Delegation, Status> {
        let mut actions = vec![action];
        if action != AclAction::Read && identity.would_allow(resource, AclAction::Read, scope) {
            actions.push(AclAction::Read);
        }
        Delegation::new(resource, &actions, scope).map_err(|err| {
            Status::invalid_argument(format!("this action cannot be delegated: {err}"))
        })
    }

    /// Sign a viewer assertion with the cluster's workload-identity key.
    pub(super) fn sign_viewer_assertion(
        &self,
        identity: &AclIdentity,
        assertion: &ViewerAssertion,
    ) -> Result<String, Status> {
        let Some(issuer) = self.workload_identity.as_ref() else {
            return Err(Status::failed_precondition(
                "app views and actions need workload identity: the manager signs every request \
                 it forwards to an app with the cluster's workload-identity key (a viewer \
                 assertion), and workloadIdentity.enabled is off on this manager",
            ));
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock is before 1970"))?
            .as_secs();
        let mut claims = Map::new();
        claims.insert(
            ASSERTION_CLAIM.to_string(),
            serde_json::to_value(assertion)
                .map_err(|err| Status::internal(format!("encoding the assertion: {err}")))?,
        );
        let jti = format!("va_{}", crate::util::random_string(24).to_lowercase());
        let subject = crate::structs::exec_ticket::subject_for(
            identity.accessor_id.as_deref(),
            &identity.principal,
        );
        issuer
            .issue_viewer_assertion(
                &assertion.app,
                &jti,
                &subject,
                claims,
                now,
                now + ASSERTION_TTL_SECS,
            )
            .map(|issued| issued.token)
            .map_err(|err| {
                if crate::manager::workload_identity::SigningKeyUnavailable::in_chain(&err) {
                    Status::unavailable(format!("cannot sign the viewer assertion yet: {err:#}"))
                } else {
                    Status::internal(format!("failed to sign the viewer assertion: {err:#}"))
                }
            })
    }

    /// An enabled app's record and parsed `ui` block.
    fn load_ui_app(&self, name: &str) -> Result<(AppRecord, UiSpec), Status> {
        let record = self
            .store
            .get_app(name)
            .map_err(|e| map_store_error("Error loading the app", e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "no app named {name:?} is installed (`legion app list` shows the installed \
                     apps)"
                ))
            })?;
        if !record.enabled {
            return Err(Status::failed_precondition(format!(
                "app {name} is disabled, so it serves no views (`legion app enable {name}`)"
            )));
        }
        let ui = record.manifest.ui_spec().map_err(|err| {
            Status::failed_precondition(format!("app {name}'s ui block is invalid: {err}"))
        })?;
        Ok((record, ui))
    }

    /// Deliver one request to an allocation of `record`'s job: healthy ones
    /// first. A request that never reached an allocation (it is gone, or its
    /// port cannot be dialed) moves on to the next, at most
    /// [`FORWARD_ATTEMPTS`] in all; one that reached the app is never sent
    /// twice.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn forward_to_app(
        &self,
        record: &AppRecord,
        ui: &UiSpec,
        method: &str,
        path: String,
        assertion: String,
        body: Vec<u8>,
        accept: &str,
    ) -> Result<(ForwardAppRequestReply, AppUiAllocation), Status> {
        let job_id = record.job_id();
        let allocations = self
            .store
            .app_ui_allocations(&job_id)
            .map_err(|e| map_store_error("Error loading the app's allocations", e))?;
        if allocations.is_empty() {
            return Err(Status::unavailable(format!(
                "app {} has no running allocation to serve the request (`legion app status {}`)",
                record.name, record.name
            )));
        }
        let mut last = None;
        for allocation in allocations.into_iter().take(FORWARD_ATTEMPTS) {
            let request = ForwardAppRequestRequest {
                allocation_id: allocation.id,
                job_id: job_id.clone(),
                port: ui.port().to_string(),
                method: method.to_string(),
                path: path.clone(),
                headers: vec![
                    AppHttpHeader {
                        name: VIEWER_HEADER.to_string(),
                        value: assertion.clone(),
                    },
                    AppHttpHeader {
                        name: "accept".to_string(),
                        value: accept.to_string(),
                    },
                    AppHttpHeader {
                        name: "content-type".to_string(),
                        value: "application/json".to_string(),
                    },
                ],
                body: body.clone(),
            };
            match self
                .app_forwarder
                .forward(&self.store, &allocation.node_id, request)
                .await
            {
                Ok(reply) => return Ok((reply, allocation)),
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::NotFound
                    ) =>
                {
                    debug!(
                        app = %record.name,
                        allocation_id = allocation.id,
                        node_id = %allocation.node_id,
                        error = %status.message(),
                        "App request did not reach this allocation; trying the next"
                    );
                    last = Some(status);
                }
                Err(status) => return Err(status),
            }
        }
        let status = last.expect("at least one attempt");
        Err(Status::unavailable(format!(
            "app {} could not be reached: {}",
            record.name,
            status.message()
        )))
    }

    /// The `AppActionInfo` of each action `view` references, for `context`.
    fn action_infos(
        &self,
        identity: &AclIdentity,
        ui: &UiSpec,
        view: &AppView,
        context: &ViewContext,
    ) -> Vec<AppActionInfo> {
        view.referenced_actions()
            .iter()
            .filter_map(|name| ui.action(name))
            .map(|action| AppActionInfo {
                name: action.name.clone(),
                title: action.title.clone(),
                confirm: action.confirmation(context).unwrap_or_default(),
                inputs_json: serde_json::to_string(&action.inputs)
                    .unwrap_or_else(|_| "{}".to_string()),
                permission: Some(permission_to_proto(&action.permission)),
                allowed: self.ui_permission_allows(
                    identity,
                    Some(&action.permission),
                    Some(context),
                ),
                mutating: action.mutating(),
            })
            .collect()
    }

    pub(super) async fn list_ui_extensions_rpc(
        &self,
        request: Request<ListUiExtensionsRequest>,
    ) -> Result<Response<ListUiExtensionsReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::App, AclAction::Read, AclScope::any())?;
        let inner = request.into_inner();
        let mut slot = match inner.slot.trim() {
            "" => None,
            raw => Some(Slot::parse(raw).map_err(Status::invalid_argument)?),
        };
        let context = match inner.context {
            None => None,
            Some(context) => {
                let context = context_from_proto(Some(context))?;
                let context_slot = context.slot().map_err(Status::invalid_argument)?;
                match slot {
                    Some(slot) if slot != context_slot => {
                        return Err(Status::invalid_argument(format!(
                            "the context is for the {context_slot} slot, not {slot}"
                        )))
                    }
                    _ => slot = Some(context_slot),
                }
                Some(context)
            }
        };
        let app_filter = inner.app.trim();
        let records = self
            .store
            .list_apps()
            .map_err(|e| map_store_error("Error listing apps", e))?;
        let mut extensions = Vec::new();
        for record in records {
            if !record.enabled
                || (!app_filter.is_empty() && record.name != app_filter)
                || !identity.allows(
                    AclResource::App,
                    AclAction::Read,
                    &AclScope::app(&record.name),
                )
            {
                continue;
            }
            let ui = match record.manifest.ui_spec() {
                Ok(ui) => ui,
                Err(err) => {
                    warn!(app = %record.name, error = %err, "Skipping an app with an invalid ui block");
                    continue;
                }
            };
            let refresh = ui.refresh_secs().ok().flatten().unwrap_or(0) as u32;
            for each in Slot::ALL {
                if slot.is_some_and(|slot| slot != each) {
                    continue;
                }
                for entry in ui.entries(each) {
                    if !self.ui_permission_allows(
                        &identity,
                        entry.permission.as_ref(),
                        context.as_ref(),
                    ) {
                        continue;
                    }
                    extensions.push(UiExtension {
                        app: record.name.clone(),
                        slot: each.as_str().to_string(),
                        title: entry.title.clone(),
                        icon: entry.icon.clone().unwrap_or_default(),
                        view: entry.view.clone().unwrap_or_default(),
                        badge: entry.badge.clone().unwrap_or_default(),
                        permission: entry.permission.as_ref().map(permission_to_proto),
                        refresh_seconds: refresh,
                        app_description: record.manifest.description.clone(),
                        page: String::new(),
                    });
                }
            }
            // Sandboxed pages live in the sidebar, after the app's views.
            // Their permission is checked on any scope, as GetAppPage does.
            if slot.is_none_or(|slot| slot == Slot::Nav) {
                for page in &ui.pages {
                    if !self.ui_permission_allows(&identity, page.permission.as_ref(), None) {
                        continue;
                    }
                    extensions.push(UiExtension {
                        app: record.name.clone(),
                        slot: Slot::Nav.as_str().to_string(),
                        title: page.title.clone(),
                        icon: page.icon.clone().unwrap_or_default(),
                        view: String::new(),
                        badge: String::new(),
                        permission: page.permission.as_ref().map(permission_to_proto),
                        refresh_seconds: 0,
                        app_description: record.manifest.description.clone(),
                        page: page.name.clone(),
                    });
                }
            }
        }
        Ok(Response::new(ListUiExtensionsReply { extensions }))
    }

    /// The entry and view a GetAppView names: the named view, registered in
    /// the context's slot, or the first entry there the caller may see.
    fn resolve_view<'a>(
        &self,
        identity: &AclIdentity,
        app: &str,
        ui: &'a UiSpec,
        slot: Slot,
        view: &str,
        context: &ViewContext,
    ) -> Result<ResolvedView<'a>, Status> {
        if !view.is_empty() {
            let entry = ui.entry_for_view(slot, view).ok_or_else(|| {
                Status::not_found(format!(
                    "app {app} registers no view {view:?} in the {slot} slot"
                ))
            })?;
            return Ok(ResolvedView {
                entry,
                view: view.to_string(),
            });
        }
        let entries = ui.entries(slot);
        if entries.is_empty() {
            return Err(Status::not_found(format!(
                "app {app} registers nothing in the {slot} slot"
            )));
        }
        let entry = entries
            .iter()
            .find(|entry| {
                self.ui_permission_allows(identity, entry.permission.as_ref(), Some(context))
            })
            .unwrap_or(&entries[0]);
        let view = entry
            .view
            .clone()
            .or_else(|| entry.badge.clone())
            .unwrap_or_default();
        Ok(ResolvedView { entry, view })
    }

    pub(super) async fn get_app_view_rpc(
        &self,
        request: Request<GetAppViewRequest>,
    ) -> Result<Response<GetAppViewReply>, Status> {
        let name = request.get_ref().app.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Read,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let context = context_from_proto(inner.context)?;
        let slot = context.slot().map_err(Status::invalid_argument)?;
        let (record, ui) = self.load_ui_app(&name)?;
        let resolved =
            self.resolve_view(&identity, &name, &ui, slot, inner.view.trim(), &context)?;
        let checked =
            self.enforce_ui_permission(&identity, resolved.entry.permission.as_ref(), &context)?;
        let assertion = ViewerAssertion {
            kind: AssertionKind::View,
            app: name.clone(),
            name: resolved.view.clone(),
            principal: identity.principal.clone(),
            accessor: identity.accessor_id.clone(),
            permission: resolved.entry.permission.as_ref().map(UiPermission::label),
            scope: checked.scope,
            namespaces: checked.namespaces,
            context: context.clone(),
            inputs: None,
            delegation: None,
        };
        let token = self.sign_viewer_assertion(&identity, &assertion)?;
        let path = format!("{VIEW_PATH_PREFIX}{}?{}", resolved.view, context.to_query());
        let (reply, allocation) = self
            .forward_to_app(
                &record,
                &ui,
                "GET",
                path,
                token,
                Vec::new(),
                "application/json",
            )
            .await?;
        let what = format!("view {:?}", resolved.view);
        if !(200..300).contains(&reply.status) {
            return Err(app_error(&name, &what, &reply));
        }
        let view = validate_view(&reply.body, Some(&ui.actions), Some(slot)).map_err(|err| {
            Status::failed_precondition(format!(
                "app {name} answered {what} with something Legion does not render: {err}"
            ))
        })?;
        debug!(
            app = %name,
            view = %resolved.view,
            slot = %slot,
            principal = %identity.principal,
            allocation_id = allocation.id,
            "Rendered an app view"
        );
        let actions = self.action_infos(&identity, &ui, &view, &context);
        Ok(Response::new(GetAppViewReply {
            app: name,
            view: resolved.view,
            context: Some(context_to_proto(&context)),
            view_json: serde_json::to_string(&view).unwrap_or_default(),
            title: resolved.entry.title.clone(),
            actions,
            refresh_seconds: ui.refresh_secs().ok().flatten().unwrap_or(0) as u32,
            allocation_id: allocation.id,
            node_id: allocation.node_id,
        }))
    }

    pub(super) async fn invoke_app_action_rpc(
        &self,
        request: Request<InvokeAppActionRequest>,
    ) -> Result<Response<InvokeAppActionReply>, Status> {
        let name = request.get_ref().app.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Read,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let action_name = inner.action.trim().to_string();
        let context = context_from_proto(inner.context.clone())?;
        let slot = context.slot().map_err(Status::invalid_argument)?;
        if !slot.has_scope() {
            return Err(Status::invalid_argument(format!(
                "actions are pressed on a job, a task, or a node, never in the {slot} slot: an \
                 action's permission is checked on what it is pressed for"
            )));
        }
        let (record, ui) = self.load_ui_app(&name)?;
        let action: &ActionDecl = ui.action(&action_name).ok_or_else(|| {
            Status::not_found(format!("app {name} declares no action {action_name:?}"))
        })?;
        let given: Value = match inner.inputs_json.trim() {
            "" => Value::Null,
            raw => serde_json::from_str(raw).map_err(|err| {
                Status::invalid_argument(format!("inputs_json is not JSON: {err}"))
            })?,
        };
        let inputs = resolve_inputs(action, &given).map_err(Status::invalid_argument)?;
        let checked =
            match self.enforce_ui_permission(&identity, Some(&action.permission), &context) {
                Ok(checked) => checked,
                Err(status) => {
                    if status.code() == tonic::Code::PermissionDenied {
                        self.store.events().app_action(
                            &name,
                            &action_name,
                            &identity.principal,
                            &context,
                            "denied",
                            &format!("lacks {}", action.permission.label()),
                        );
                    }
                    return Err(status);
                }
            };

        let idempotency_key = scoped_idempotency_key(
            &identity,
            "invoke_app_action",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(stored) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let mut reply: InvokeAppActionReply =
                serde_json::from_str(&stored).unwrap_or_else(|_| InvokeAppActionReply {
                    message: stored.clone(),
                    ..Default::default()
                });
            reply.deduplicated = true;
            return Ok(Response::new(reply));
        }

        let delegation = match &checked.checked {
            Some((resource, action, scope)) => {
                Some(Self::delegation_for(&identity, *resource, *action, scope)?)
            }
            None => None,
        };
        let delegated = delegation
            .as_ref()
            .map(Delegation::label)
            .unwrap_or_else(|| "-".to_string());
        let assertion = ViewerAssertion {
            kind: AssertionKind::Action,
            app: name.clone(),
            name: action_name.clone(),
            principal: identity.principal.clone(),
            accessor: identity.accessor_id.clone(),
            permission: Some(action.permission.label()),
            scope: checked.scope,
            namespaces: checked.namespaces,
            context: context.clone(),
            inputs: Some(inputs_digest(&inputs)),
            delegation,
        };
        let token = self.sign_viewer_assertion(&identity, &assertion)?;
        let body = json!({ "context": context, "inputs": inputs }).to_string();
        let path = format!("{ACTION_PATH_PREFIX}{action_name}");
        let what = format!("action {action_name:?}");
        let outcome = self
            .forward_to_app(
                &record,
                &ui,
                "POST",
                path,
                token,
                body.into_bytes(),
                "application/json",
            )
            .await
            .and_then(|(reply, allocation)| {
                if !(200..300).contains(&reply.status) {
                    return Err(app_error(&name, &what, &reply));
                }
                let answer: Value = serde_json::from_slice(&reply.body).map_err(|err| {
                    Status::failed_precondition(format!(
                        "app {name} answered {what} with something that is not JSON: {err}"
                    ))
                })?;
                let Some(object) = answer.as_object() else {
                    return Err(Status::failed_precondition(format!(
                        "app {name} answered {what} with something other than an object"
                    )));
                };
                if let Some(unknown) = object
                    .keys()
                    .find(|key| !matches!(key.as_str(), "message" | "view"))
                {
                    return Err(Status::failed_precondition(format!(
                        "app {name} answered {what} with an unknown field {unknown:?} (an action \
                         answers {{ \"message\", \"view\" }})"
                    )));
                }
                let message = object
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("done")
                    .to_string();
                let view = match object.get("view") {
                    None | Some(Value::Null) => None,
                    Some(view) => Some(
                        validate_view_value(view, Some(&ui.actions), Some(slot)).map_err(
                            |err| {
                                Status::failed_precondition(format!(
                                    "app {name} answered {what} with a view Legion does not \
                                     render: {err}"
                                ))
                            },
                        )?,
                    ),
                };
                Ok((message, view, allocation))
            });
        let (message, view, allocation) = match outcome {
            Ok(done) => done,
            Err(status) => {
                self.store.events().app_action(
                    &name,
                    &action_name,
                    &identity.principal,
                    &context,
                    "failed",
                    status.message(),
                );
                return Err(status);
            }
        };
        let message = app_message(message.as_bytes());
        self.store.events().app_action(
            &name,
            &action_name,
            &identity.principal,
            &context,
            "ok",
            &message,
        );
        info!(
            target: "app_audit",
            app = %name,
            action = %action_name,
            principal = %identity.principal,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            slot = %context.slot,
            job = context.job_id().as_deref().unwrap_or("-"),
            task = if context.task.is_empty() { "-" } else { context.task.as_str() },
            node = if context.node.is_empty() { "-" } else { context.node.as_str() },
            allocation_id = allocation.id,
            delegation = %delegated,
            "App action ran"
        );
        let reply = InvokeAppActionReply {
            message,
            view_json: view
                .map(|view| serde_json::to_string(&view).unwrap_or_default())
                .unwrap_or_default(),
            deduplicated: false,
            inputs_json: inputs.to_string(),
        };
        if !idempotency_key.is_empty() {
            self.store
                .idempotency_record(
                    &idempotency_key,
                    "invoke_app_action",
                    &serde_json::to_string(&reply).unwrap_or_default(),
                )
                .map_err(|e| map_store_error("Error recording idempotency", e))?;
        }
        Ok(Response::new(reply))
    }

    pub(super) async fn get_app_page_rpc(
        &self,
        request: Request<GetAppPageRequest>,
    ) -> Result<Response<GetAppPageReply>, Status> {
        let name = request.get_ref().app.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Read,
            AclScope::app(&name),
        )?;
        let page_name = request.into_inner().page.trim().to_string();
        let (record, ui) = self.load_ui_app(&name)?;
        let page = ui.page(&page_name).ok_or_else(|| {
            Status::not_found(format!(
                "app {name} declares no page {page_name:?} (`legion app view {name}` lists \
                 what it shows you)"
            ))
        })?;
        let context = page_context();
        let checked = self.enforce_ui_permission(&identity, page.permission.as_ref(), &context)?;
        let assertion = ViewerAssertion {
            kind: AssertionKind::Page,
            app: name.clone(),
            name: page.name.clone(),
            principal: identity.principal.clone(),
            accessor: identity.accessor_id.clone(),
            permission: page.permission.as_ref().map(UiPermission::label),
            scope: checked.scope,
            namespaces: checked.namespaces,
            context,
            inputs: None,
            delegation: None,
        };
        let token = self.sign_viewer_assertion(&identity, &assertion)?;
        let (reply, allocation) = self
            .forward_to_app(
                &record,
                &ui,
                "GET",
                page.bundle_path(),
                token,
                Vec::new(),
                "text/html",
            )
            .await?;
        let what = format!("page {:?}", page.name);
        if !(200..300).contains(&reply.status) {
            return Err(app_error(&name, &what, &reply));
        }
        let media_type = reply
            .content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if media_type != "text/html" {
            return Err(Status::failed_precondition(format!(
                "app {name} answered {what} with {:?}; a page is served as text/html",
                reply.content_type
            )));
        }
        if reply.body.len() > MAX_PAGE_BYTES {
            return Err(Status::failed_precondition(format!(
                "app {name} answered {what} with {} bytes; a page is at most {MAX_PAGE_BYTES}",
                reply.body.len()
            )));
        }
        let html = String::from_utf8(reply.body).map_err(|_| {
            Status::failed_precondition(format!(
                "app {name} answered {what} with something that is not UTF-8"
            ))
        })?;
        debug!(
            app = %name,
            page = %page.name,
            principal = %identity.principal,
            allocation_id = allocation.id,
            bytes = html.len(),
            "Served an app page"
        );
        let rpcs = page
            .rpcs
            .iter()
            .filter_map(|method| {
                page_rpc_kind(method).map(|kind| AppPageRpc {
                    method: method.clone(),
                    mutating: kind == PageRpcKind::Write,
                })
            })
            .collect();
        Ok(Response::new(GetAppPageReply {
            app: name,
            page: page.name.clone(),
            title: page.title.clone(),
            html,
            rpcs,
            allocation_id: allocation.id,
            node_id: allocation.node_id,
        }))
    }
}
