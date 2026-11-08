//! The endpoint index's store half (mesh-control-path plan §6.1): reading a
//! service's set through the index, re-reading consumed services for the
//! refresher, and marking sets dirty from committed changesets.
//!
//! The marking pass reads the changeset a second time, only while some
//! service is loaded ([`EndpointIndex::is_active`]), so a cluster that uses
//! no `connect` pays nothing. It never fails its caller: it runs inside the
//! raft state machine's `apply`, like the rest of the projection, and an
//! error marks every set dirty instead.

use super::*;
use crate::storage::endpoint_index::{ConsumerView, SetKey};

impl Store {
    /// One consumer's endpoint set, from the index, re-read from the catalog
    /// when it is dirty or old.
    pub fn endpoint_view(&self, key: &SetKey) -> R<Arc<ConsumerView>> {
        self.endpoint_index
            .view(key, |service| self.service_endpoints(Some(service)))
    }

    /// Re-read every consumed service that is dirty or old, so the work
    /// streams of the nodes that consume one push its new set. Blocking.
    pub fn refresh_endpoint_sets(&self) -> usize {
        self.endpoint_index
            .refresh(|service| self.service_endpoints(Some(service)))
    }

    /// Mark the endpoint sets a committed changeset may have changed.
    pub(crate) fn note_endpoint_changes(&self, bytes: &[u8]) {
        if bytes.is_empty() || !self.endpoint_index.is_active() {
            return;
        }
        if let Err(err) = self.try_note_endpoint_changes(bytes) {
            warn!(
                error = ?err,
                "Could not tell which endpoint sets a committed changeset touched; re-reading every one"
            );
            self.endpoint_index.mark_all();
        }
    }

    fn try_note_endpoint_changes(&self, bytes: &[u8]) -> R<()> {
        use fallible_streaming_iterator::FallibleStreamingIterator;
        use rusqlite::hooks::Action;
        use rusqlite::session::ChangesetIter;

        let mut allocations: Vec<i64> = Vec::new();
        let mut placed: Vec<i64> = Vec::new(); // task config ids of new allocations
        let mut nodes: HashSet<String> = HashSet::new();
        let mut task_config_rewritten = false;
        let mut column_cache: HashMap<String, Vec<String>> = HashMap::new();

        let reader: &mut dyn std::io::Read = &mut &*bytes;
        let mut iter = ChangesetIter::start_strm(&reader)
            .context("failed opening changeset iterator for the endpoint index")?;
        while let Some(item) = iter
            .next()
            .context("failed iterating changeset for the endpoint index")?
        {
            let op = item.op()?;
            let table = op.table_name().to_string();
            let action = op.code();
            let text = |value: Option<rusqlite::types::ValueRef<'_>>| {
                value.and_then(|value| value.as_str().ok().map(str::to_owned))
            };
            match table.as_str() {
                "allocation" => {
                    let id_idx =
                        self.changeset_column_index(&mut column_cache, "allocation", "id")?;
                    match action {
                        Action::SQLITE_INSERT => {
                            let tc_idx = self.changeset_column_index(
                                &mut column_cache,
                                "allocation",
                                "task_config_id",
                            )?;
                            if let Some(tc) =
                                item.new_value(tc_idx).ok().and_then(|v| v.as_i64().ok())
                            {
                                placed.push(tc);
                            }
                        }
                        _ => {
                            if let Some(id) =
                                item.old_value(id_idx).ok().and_then(|v| v.as_i64().ok())
                            {
                                allocations.push(id);
                            }
                        }
                    }
                }
                "allocation_health" => {
                    let idx = self.changeset_column_index(
                        &mut column_cache,
                        "allocation_health",
                        "allocation_id",
                    )?;
                    let id = match action {
                        Action::SQLITE_INSERT => item.new_value(idx).ok(),
                        _ => item.old_value(idx).ok(),
                    }
                    .and_then(|v| v.as_i64().ok());
                    allocations.extend(id);
                }
                "node" => {
                    // A node's advertised addresses live in `config_attrs`;
                    // an insert has no allocations yet.
                    let idx = self.changeset_column_index(&mut column_cache, "node", "id")?;
                    let relevant = match action {
                        Action::SQLITE_DELETE => true,
                        Action::SQLITE_UPDATE => {
                            let attrs = self.changeset_column_index(
                                &mut column_cache,
                                "node",
                                "config_attrs",
                            )?;
                            item.new_value(attrs).is_ok()
                        }
                        _ => false,
                    };
                    if relevant {
                        nodes.extend(text(item.old_value(idx).ok()));
                    }
                }
                "node_health" => {
                    // Liveness (`status`, `lost_at`), not heartbeat
                    // timestamps: an update record carries only the columns
                    // that changed.
                    let idx =
                        self.changeset_column_index(&mut column_cache, "node_health", "node_id")?;
                    let relevant = match action {
                        Action::SQLITE_UPDATE => {
                            let status = self.changeset_column_index(
                                &mut column_cache,
                                "node_health",
                                "status",
                            )?;
                            let lost_at = self.changeset_column_index(
                                &mut column_cache,
                                "node_health",
                                "lost_at",
                            )?;
                            item.new_value(status).is_ok() || item.new_value(lost_at).is_ok()
                        }
                        _ => true,
                    };
                    if relevant {
                        let id = match action {
                            Action::SQLITE_INSERT => item.new_value(idx).ok(),
                            _ => item.old_value(idx).ok(),
                        };
                        nodes.extend(text(id));
                    }
                }
                // A new task config has no allocations yet; a rewritten or
                // removed one may change any set its rows are in.
                "task_config" if action != Action::SQLITE_INSERT => {
                    task_config_rewritten = true;
                }
                _ => {}
            }
        }
        drop(iter);

        if task_config_rewritten {
            self.endpoint_index.mark_all();
            return Ok(());
        }
        self.endpoint_index.mark_allocations(allocations);
        self.endpoint_index
            .mark_nodes(nodes.iter().map(String::as_str));
        // A placement: the new allocation's service, by its task config.
        let mut seen: HashSet<i64> = HashSet::new();
        for task_config_id in placed {
            if !seen.insert(task_config_id) {
                continue;
            }
            let service = match self.endpoint_index.task_config_service(task_config_id) {
                Some(service) => service,
                None => {
                    let service = self.task_config_service_ref(task_config_id)?;
                    self.endpoint_index
                        .remember_task_config(task_config_id, service.clone());
                    service
                }
            };
            if let Some(service) = service {
                self.endpoint_index.mark_service(&service);
            }
        }
        Ok(())
    }

    /// The canonical service reference a task config declares
    /// (`payments-prod/web`, or `web` in `default`), or `None` when it
    /// declares no service. Matches the key [`Store::service_endpoints`]
    /// filters on.
    fn task_config_service_ref(&self, task_config_id: i64) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let row: Option<(String, Option<String>, String)> = conn
            .prepare_cached("SELECT name, service, job_id FROM task_config WHERE id = ?1;")?
            .query_row([task_config_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;
        drop(conn);
        let Some((task_name, Some(service), job_id)) = row else {
            return Ok(None);
        };
        // A stored service that does not parse cannot be resolved by name
        // either; the filtered catalog query skips it too.
        let Ok(service) = serde_json::from_str::<crate::structs::ServiceConfig>(&service) else {
            return Ok(None);
        };
        Ok(Some(crate::structs::namespace::job_id(
            crate::structs::namespace::job_namespace(&job_id),
            service.service_name(&task_name),
        )))
    }
}
