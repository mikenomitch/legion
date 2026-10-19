//! Storage and admission for namespace policies (docs/namespaces.md,
//! "Namespace policies").
//!
//! Policies are control-lane rows next to `job` and `namespace`, and the
//! checks that need cluster state run inside the control-lane entries they
//! guard: the quota and `maxJobs` inside the job submit, `maxNamespaces`
//! inside namespace creation. Two racing submits therefore serialize, and
//! the second sees the first.
//!
//! **How a job is charged.** Per band, a job is charged the larger of what
//! it declares and what it runs, dimension by dimension:
//!
//! - *declared*: for each count-driven task of the job's current config,
//!   the effective count (a `job scale` override included) times its
//!   reservation (`resources`, after `taskResourceDefaults`). A stopped job
//!   declares nothing. Keyed and per-node (`count.per`) tasks declare
//!   nothing either: their cardinality is not in the spec.
//! - *live*: the reservations of the job's allocations not yet `complete` or
//!   `failed`, the same set the scheduler counts against node capacity,
//!   summed in SQL (`live_reservations_on`).
//!
//! Declared demand is what closes the race: a job is charged from the
//! moment it is admitted, not from the moment it places. Live allocations
//! are what charge keyed instances, per-node tasks, and the old version
//! still running during a rollout.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::namespace_store::{NamespaceUsage, Reservation};
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::namespace::job_namespace;
use crate::structs::namespace_policy::{governing, NamespacePolicy};
use crate::structs::JobConfig;

/// One stored policy plus bookkeeping.
#[derive(Debug, Clone, PartialEq)]
pub struct NamespacePolicyRecord {
    pub policy: NamespacePolicy,
    pub created_at: String,
    pub updated_at: String,
}

/// What one job is charged, by band.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobCharge {
    pub declared: BTreeMap<String, Reservation>,
    pub live: BTreeMap<String, Reservation>,
}

impl JobCharge {
    /// The charge: per band and dimension, the larger of declared and live.
    pub fn charged(&self) -> BTreeMap<String, Reservation> {
        let mut out = self.declared.clone();
        for (band, live) in &self.live {
            let entry = out.entry(band.clone()).or_default();
            *entry = entry.max_with(live);
        }
        out
    }
}

/// Reservation of one task's `resources` under the manager's defaults, or
/// `None` when nothing can parse them (the scheduler's own walk warns).
fn task_reservation(
    policy: &crate::resources::ResourcePolicy,
    resources: &serde_json::Value,
) -> Option<Reservation> {
    let normalized = policy.normalize_task_resources(resources, false).ok()?;
    let request = crate::resources::ResourceRequest::from_value(&normalized).ok()?;
    Some(Reservation {
        cpu: request.amount("cpu").unwrap_or(0.0),
        memory: request.amount("memory").unwrap_or(0.0),
        cores: match request.values.get("cores") {
            Some(crate::resources::ResourceValue::Amount(cores)) => *cores,
            Some(crate::resources::ResourceValue::Items(cores)) => cores.len() as f64,
            None => 0.0,
        },
        allocations: 0,
    })
}

/// Whether a task's count comes from its spec: not keyed (instances drive
/// it) and not per-node.
fn count_driven(per: Option<&str>, stateful: Option<&crate::structs::StatefulConfig>) -> bool {
    per.is_none()
        && stateful.is_none_or(|stateful| stateful.mode != crate::structs::InstanceMode::Keyed)
}

/// `(job, group, task) -> (override, base count)` for every applied scale.
type ScaleOverrides = BTreeMap<(String, String, String), (u32, u32)>;

fn scale_overrides_on(conn: &Connection) -> R<ScaleOverrides> {
    let mut stmt = conn.prepare_cached(
        "SELECT job_id, task_group, task_name, count, base_count FROM task_scale
         WHERE count IS NOT NULL;",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            (row.get(0)?, row.get(1)?, row.get(2)?),
            (row.get(3)?, row.get(4)?),
        ))
    })?;
    rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
}

fn effective_count(
    overrides: &ScaleOverrides,
    job: &str,
    group: &str,
    task: &str,
    spec: u32,
) -> u32 {
    overrides
        .get(&(job.to_string(), group.to_string(), task.to_string()))
        .filter(|(_, base)| *base == spec)
        .map_or(spec, |(count, _)| *count)
}

/// The declared half of a config's charge (see the module docs).
fn declared_for_config(
    config: &JobConfig,
    overrides: &ScaleOverrides,
    policy: &crate::resources::ResourcePolicy,
) -> BTreeMap<String, Reservation> {
    let band = config.priority.as_str().to_string();
    let mut out: BTreeMap<String, Reservation> = BTreeMap::new();
    for task in &config.tasks {
        if !count_driven(task.count.per.as_deref(), task.stateful.as_ref()) {
            continue;
        }
        let count = effective_count(
            overrides,
            &config.name,
            &task.group,
            &task.name,
            task.count.num,
        );
        if let Some(each) = task_reservation(policy, &task.resources) {
            out.entry(band.clone()).or_default().add(&each.times(count));
        }
    }
    out
}

/// Every job's charge, keyed by canonical job id.
pub(crate) fn job_charges_on(
    conn: &Connection,
    policy: &crate::resources::ResourcePolicy,
) -> R<BTreeMap<String, JobCharge>> {
    let overrides = scale_overrides_on(conn)?;
    let mut charges: BTreeMap<String, JobCharge> = BTreeMap::new();
    let mut declared = conn.prepare_cached(
        "SELECT j.id, j.status, jc.priority, tc.group_name, tc.name, tc.count, tc.count_per,
                tc.stateful, tc.resources
         FROM job j
         JOIN job_config jc ON jc.id = (
             SELECT latest.id FROM job_config latest WHERE latest.job_id = j.id
             ORDER BY latest.version DESC LIMIT 1)
         JOIN task_config tc ON tc.job_config_id = jc.id;",
    )?;
    let rows = declared.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, u32>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, serde_json::Value>(8)?,
        ))
    })?;
    for row in rows {
        let (job, status, band, group, task, count, per, stateful, resources) = row?;
        let entry = charges.entry(job.clone()).or_default();
        let stateful: Option<crate::structs::StatefulConfig> = stateful
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok());
        if status == "stopped" || !count_driven(per.as_deref(), stateful.as_ref()) {
            continue;
        }
        let count = effective_count(&overrides, &job, &group, &task, count);
        if let Some(each) = task_reservation(policy, &resources) {
            let band = crate::structs::Band::from_stored(&band)
                .as_str()
                .to_string();
            entry
                .declared
                .entry(band)
                .or_default()
                .add(&each.times(count));
        }
    }
    for ((job, band), reservation) in crate::storage::namespace_store::live_reservations_on(conn)? {
        charges
            .entry(job)
            .or_default()
            .live
            .entry(band)
            .or_default()
            .add(&reservation);
    }
    Ok(charges)
}

/// Sum charges into per-namespace usage (`jobs` counts every job row).
pub(crate) fn namespace_charges_on(
    conn: &Connection,
    policy: &crate::resources::ResourcePolicy,
) -> R<BTreeMap<String, NamespaceUsage>> {
    let mut usage: BTreeMap<String, NamespaceUsage> = BTreeMap::new();
    for (job, charge) in job_charges_on(conn, policy)? {
        let entry = usage.entry(job_namespace(&job).to_string()).or_default();
        entry.jobs += 1;
        for (band, reservation) in charge.charged() {
            entry.bands.entry(band).or_default().add(&reservation);
        }
    }
    Ok(usage)
}

/// Every stored policy, by name.
pub(crate) fn policies_on(conn: &Connection) -> R<Vec<NamespacePolicy>> {
    let mut stmt = conn.prepare_cached("SELECT object FROM namespace_policy ORDER BY name;")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut policies = Vec::new();
    for raw in rows {
        let raw = raw?;
        match serde_json::from_str::<NamespacePolicy>(&raw) {
            Ok(policy) => policies.push(policy),
            // A row this build cannot read governs nothing rather than
            // failing every submit; apply re-validates on the next write.
            Err(err) => warn!(error = %err, "Skipping an unreadable namespace policy row"),
        }
    }
    Ok(policies)
}

fn fmt_amount(dimension: &str, value: f64) -> String {
    match dimension {
        "cpu" => format!("{value:.0} MHz CPU"),
        "memory" => format!("{value:.0} MiB memory"),
        _ => format!("{value} cores"),
    }
}

/// The quota and `maxJobs` half of admission, for `config` about to be
/// written in `conn`'s transaction. A job never loses ground it holds: a
/// resubmit whose charge does not grow in a dimension is admitted in that
/// dimension even over the cap, so lowering a quota never breaks a job that
/// was already admitted.
pub(crate) fn enforce_admission_on(
    conn: &Connection,
    resource_policy: &crate::resources::ResourcePolicy,
    config: &JobConfig,
) -> R<()> {
    let policies = policies_on(conn)?;
    let namespace = job_namespace(&config.name);
    let governing = governing(&policies, namespace);
    if governing.is_empty() {
        return Ok(());
    }
    let charges = job_charges_on(conn, resource_policy)?;
    let overrides = scale_overrides_on(conn)?;
    let existing = charges.get(&config.name).cloned().unwrap_or_default();
    let before = existing.charged();
    let after = JobCharge {
        declared: declared_for_config(config, &overrides, resource_policy),
        live: existing.live.clone(),
    }
    .charged();
    let is_new = !charges.contains_key(&config.name);
    for policy in governing {
        let others = charges
            .iter()
            .filter(|(job, _)| *job != &config.name && policy.matches(job_namespace(job)));
        if let (Some(max), true) = (policy.max_jobs, is_new) {
            let jobs = others.clone().count() as u64;
            if jobs + 1 > max {
                bail!(StorageError::failed_precondition(format!(
                    "namespace policy '{}' caps jobs at {max} across {}; {jobs} exist\nHint: \
                     destroy a job there, or raise maxJobs",
                    policy.name,
                    policy.selector()
                )));
            }
        }
        for (band, limit) in &policy.quota {
            let mut in_use = Reservation::default();
            for (_, charge) in others.clone() {
                if let Some(reservation) = charge.charged().get(band) {
                    in_use.add(reservation);
                }
            }
            let adds = after.get(band).copied().unwrap_or_default();
            let held = before.get(band).copied().unwrap_or_default();
            for (dimension, cap, used, adding, had) in [
                ("cpu", limit.cpu, in_use.cpu, adds.cpu, held.cpu),
                (
                    "memory",
                    limit.memory,
                    in_use.memory,
                    adds.memory,
                    held.memory,
                ),
                ("cores", limit.cores, in_use.cores, adds.cores, held.cores),
            ] {
                let Some(cap) = cap else { continue };
                if used + adding > cap && adding > had {
                    bail!(StorageError::failed_precondition(format!(
                        "namespace policy '{}' caps band '{band}' at {} across {}; in use {}, \
                         this job adds {}\nHint: `legion namespace get {namespace}` shows \
                         usage; lower the band or the count",
                        policy.name,
                        fmt_amount(dimension, cap),
                        policy.selector(),
                        fmt_amount(dimension, used),
                        fmt_amount(dimension, adding),
                    )));
                }
            }
        }
    }
    Ok(())
}

/// `maxNamespaces`, for `name` about to be created in `conn`'s transaction.
pub(crate) fn enforce_namespace_count_on(conn: &Connection, name: &str) -> R<()> {
    let policies = policies_on(conn)?;
    let governing = governing(&policies, name);
    if governing
        .iter()
        .all(|policy| policy.max_namespaces.is_none())
    {
        return Ok(());
    }
    let mut stmt = conn.prepare_cached("SELECT name FROM namespace;")?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for policy in governing {
        let Some(max) = policy.max_namespaces else {
            continue;
        };
        let count = names
            .iter()
            .filter(|existing| policy.matches(existing))
            .count() as u64;
        if count + 1 > max {
            bail!(StorageError::failed_precondition(format!(
                "namespace policy '{}' caps namespaces matching {} at {max}; {count} exist\n\
                 Hint: delete one, or ask for maxNamespaces to be raised",
                policy.name,
                policy.selector()
            )));
        }
    }
    Ok(())
}

impl Store {
    fn namespace_policy_from_row(row: &Row<'_>) -> rusqlite::Result<NamespacePolicyRecord> {
        let raw: String = row.get(0)?;
        let policy = serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })?;
        Ok(NamespacePolicyRecord {
            policy,
            created_at: row.get(1)?,
            updated_at: row.get(2)?,
        })
    }

    /// Every namespace policy, by name.
    #[instrument(err, skip_all)]
    pub fn list_namespace_policies(&self) -> R<Vec<NamespacePolicyRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT object, created_at, updated_at FROM namespace_policy ORDER BY name;",
        )?;
        let rows = stmt.query_map([], Self::namespace_policy_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(policy = name))]
    pub fn get_namespace_policy(&self, name: &str) -> R<Option<NamespacePolicyRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT object, created_at, updated_at FROM namespace_policy WHERE name = ?;",
            [name],
            Self::namespace_policy_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Create or replace a policy. Returns the stored record and whether it
    /// was created. Does not re-judge jobs already admitted.
    #[instrument(err, skip_all, fields(policy = %policy.name))]
    pub fn apply_namespace_policy(
        &self,
        policy: NamespacePolicy,
    ) -> R<(NamespacePolicyRecord, bool)> {
        self.replicated(Lane::Control, "apply_namespace_policy", move |s| {
            s.apply_namespace_policy_apply(policy.clone())
        })
    }

    fn apply_namespace_policy_apply(
        &self,
        mut policy: NamespacePolicy,
    ) -> R<(NamespacePolicyRecord, bool)> {
        policy
            .normalize()
            .map_err(|message| anyhow::Error::new(StorageError::InvalidInput { message }))?;
        let conn = self.write_connection()?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let object = serde_json::to_string(&policy)?;
        let created = conn.execute(
            "INSERT INTO namespace_policy (name, object, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(name) DO NOTHING;",
            rusqlite::params![policy.name, object, now],
        )? > 0;
        if !created {
            conn.execute(
                "UPDATE namespace_policy SET object = ?2, updated_at = ?3 WHERE name = ?1;",
                rusqlite::params![policy.name, object, now],
            )?;
        }
        let record = conn.query_row(
            "SELECT object, created_at, updated_at FROM namespace_policy WHERE name = ?;",
            [policy.name.as_str()],
            Self::namespace_policy_from_row,
        )?;
        Ok((record, created))
    }

    /// Delete a policy. `false` when none had the name.
    #[instrument(err, skip_all, fields(policy = name))]
    pub fn delete_namespace_policy(&self, name: &str) -> R<bool> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_namespace_policy", move |s| {
            let conn = s.write_connection()?;
            Ok(conn.execute(
                "DELETE FROM namespace_policy WHERE name = ?;",
                [name.as_str()],
            )? > 0)
        })
    }

    /// Every namespace's charged usage (see the module docs): the figure a
    /// quota is checked against.
    #[instrument(err, skip_all)]
    pub fn namespace_charges(&self) -> R<BTreeMap<String, NamespaceUsage>> {
        let conn = self.read_connection()?;
        namespace_charges_on(&conn, &self.resource_policy)
    }

    /// Every job's charge, for a policy dry run.
    pub fn job_charges(&self) -> R<BTreeMap<String, JobCharge>> {
        let conn = self.read_connection()?;
        job_charges_on(&conn, &self.resource_policy)
    }

    /// The quota and `maxJobs` checks as a submit of `config` would run them
    /// now, without writing: for plan previews and scale-out pre-checks.
    pub fn check_namespace_admission(&self, config: &JobConfig) -> R<()> {
        let conn = self.read_connection()?;
        enforce_admission_on(&conn, &self.resource_policy, config)
    }

    /// Whether `count` more of a task with `resources` fit every quota
    /// governing `job_id`'s namespace in `band`: the instance-create
    /// pre-check. Keyed instances declare nothing, so they are charged by
    /// what they run; this adds them before they do.
    pub fn check_namespace_quota_growth(
        &self,
        job_id: &str,
        band: crate::structs::Band,
        resources: &serde_json::Value,
        count: u32,
    ) -> R<()> {
        let conn = self.read_connection()?;
        let policies = policies_on(&conn)?;
        let namespace = job_namespace(job_id);
        let governing = governing(&policies, namespace);
        if governing.iter().all(|policy| policy.quota.is_empty()) {
            return Ok(());
        }
        let Some(each) = task_reservation(&self.resource_policy, resources) else {
            return Ok(());
        };
        let adds = each.times(count);
        let band = band.as_str();
        let charges = job_charges_on(&conn, &self.resource_policy)?;
        for policy in governing {
            let Some(limit) = policy.quota.get(band) else {
                continue;
            };
            let mut in_use = Reservation::default();
            for (job, charge) in &charges {
                if policy.matches(job_namespace(job)) {
                    if let Some(reservation) = charge.charged().get(band) {
                        in_use.add(reservation);
                    }
                }
            }
            for (dimension, cap, used, adding) in [
                ("cpu", limit.cpu, in_use.cpu, adds.cpu),
                ("memory", limit.memory, in_use.memory, adds.memory),
                ("cores", limit.cores, in_use.cores, adds.cores),
            ] {
                let Some(cap) = cap else { continue };
                if adding > 0.0 && used + adding > cap {
                    bail!(StorageError::failed_precondition(format!(
                        "namespace policy '{}' caps band '{band}' at {} across {}; in use {}, \
                         {count} more instance(s) add {}\nHint: `legion namespace get \
                         {namespace}` shows usage; destroy idle instances first",
                        policy.name,
                        fmt_amount(dimension, cap),
                        policy.selector(),
                        fmt_amount(dimension, used),
                        fmt_amount(dimension, adding),
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod namespace_policy_store_test {
    use super::*;
    use crate::structs::namespace::Namespace;
    use crate::structs::ManagerConfigFull;
    use serde_json::json;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-namespace-policy-store-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        Store::start_storage(&ManagerConfigFull {
            store_path: store_dir.to_str().context("path")?.to_string(),
            ..Default::default()
        })
    }

    fn job(name: &str, count: u32, cpu: u32, band: crate::structs::Band) -> JobConfig {
        let mut task = crate::structs::TaskConfig::new("main", "noop");
        task.config = json!({});
        task.count = crate::structs::TaskCount::new(count);
        task.resources = json!({ "cpu": cpu, "memory": 128 });
        JobConfig {
            name: name.to_string(),
            tasks: vec![task],
            priority: band,
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        }
    }

    fn policy(value: serde_json::Value) -> NamespacePolicy {
        NamespacePolicy::from_json(&value.to_string()).unwrap()
    }

    fn create(store: &Store, name: &str) -> R<()> {
        store.apply_namespace(Namespace {
            name: name.into(),
            ..Default::default()
        })?;
        Ok(())
    }

    #[test]
    fn policies_round_trip_and_delete() -> R<()> {
        let store = setup_store()?;
        let (record, created) = store.apply_namespace_policy(policy(json!({
            "name": "payments", "namespaces": ["payments-*"], "maxJobs": 3
        })))?;
        assert!(created);
        assert_eq!(record.policy.max_jobs, Some(3));
        let (updated, created) = store.apply_namespace_policy(policy(json!({
            "name": "payments", "namespaces": ["payments-*"], "maxJobs": 4
        })))?;
        assert!(!created);
        assert_eq!(updated.policy.max_jobs, Some(4));
        assert_eq!(updated.created_at, record.created_at);
        assert_eq!(store.list_namespace_policies()?.len(), 1);
        assert!(store.delete_namespace_policy("payments")?);
        assert!(!store.delete_namespace_policy("payments")?);
        Ok(())
    }

    #[test]
    fn quota_aggregates_across_matching_namespaces_and_spares_held_ground() -> R<()> {
        let store = setup_store()?;
        create(&store, "payments-prod")?;
        create(&store, "payments-loadtest")?;
        create(&store, "shared")?;
        store.apply_namespace_policy(policy(json!({
            "name": "payments",
            "namespaces": ["payments-*"],
            "quota": { "high": { "cpu": 2000 } },
        })))?;
        let high = crate::structs::Band::High;
        // 3 x 500 in prod: 1500 of 2000.
        store.handle_job_submit(&job("payments-prod/web", 3, 500, high), &[])?;
        // A sibling namespace shares the cap: 1500 + 1000 > 2000.
        let err = store
            .handle_job_submit(&job("payments-loadtest/load", 2, 500, high), &[])
            .unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("caps band 'high' at 2000 MHz CPU across payments-*"),
            "{message}"
        );
        assert!(message.contains("in use 1500 MHz CPU"), "{message}");
        // Another band, and another namespace, are not capped.
        store.handle_job_submit(
            &job("payments-loadtest/load", 2, 500, Default::default()),
            &[],
        )?;
        store.handle_job_submit(&job("shared/auth", 10, 500, high), &[])?;
        // Lowering the cap does not break the admitted job: a resubmit that
        // does not grow still goes through.
        store.apply_namespace_policy(policy(json!({
            "name": "payments",
            "namespaces": ["payments-*"],
            "quota": { "high": { "cpu": 1000 } },
        })))?;
        let mut same = job("payments-prod/web", 3, 500, high);
        same.tasks[0].config = json!({ "env": { "A": "1" } });
        store.handle_job_submit(&same, &[])?;
        // Growing it does not.
        assert!(store
            .handle_job_submit(&job("payments-prod/web", 4, 500, high), &[])
            .is_err());
        let usage = store.namespace_charges()?;
        assert_eq!(usage["payments-prod"].bands["high"].cpu, 1500.0);
        Ok(())
    }

    #[test]
    fn max_jobs_and_max_namespaces_cap_counts() -> R<()> {
        let store = setup_store()?;
        store.apply_namespace_policy(policy(json!({
            "name": "team",
            "namespaces": ["team-*"],
            "maxJobs": 1,
            "maxNamespaces": 1,
        })))?;
        create(&store, "team-a")?;
        let err = create(&store, "team-b").unwrap_err();
        assert!(format!("{err:#}").contains("caps namespaces matching team-* at 1"));
        store.handle_job_submit(&job("team-a/one", 1, 100, Default::default()), &[])?;
        let err = store
            .handle_job_submit(&job("team-a/two", 1, 100, Default::default()), &[])
            .unwrap_err();
        assert!(format!("{err:#}").contains("caps jobs at 1 across team-*"));
        // Resubmitting the job that exists is not a new job.
        store.handle_job_submit(&job("team-a/one", 2, 100, Default::default()), &[])?;
        Ok(())
    }
}
