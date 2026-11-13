//! The scheduler-lane leader's gang sweep (docs/gang-scheduling.md): the
//! gang work that is driven by time rather than by an eval, run once a
//! second by the blocked-eval waker beside the placement-timeout pass.
//!
//! - **Views.** A `job scale` changes a gang's size without placing or
//!   removing anything the gang's own rows see (a scale-out that has to wait,
//!   a scale-in's surplus removal), so the sweep brings every live member's
//!   view to the gang's current size and quorum
//!   ([`Store::sync_gang_view_tx`]). A change raises the gang's epoch, and
//!   workers restart the members in place with the new values.
//! - **Readiness** (`gang.readyTimeout`). A member is *ready* once it has
//!   reported healthy (running, and passing its checks if it has any). The
//!   sweep marks members ready as their health arrives, and a member still
//!   not ready `readyTimeout` after it was placed is a lost member: it is
//!   removed as failed (spending the move budget, like any failure) and its
//!   gang's `onMemberLoss` applies.
//! - **Reservations** (`schedulerSettings.gangReservation`). A gang that has
//!   waited past `after` reserves room for the members its quorum needs on a
//!   set of nodes; every plan of its band or below then sees that much less
//!   free capacity there, so smaller work stops taking the room the gang is
//!   waiting for (it can still use whatever is beyond the reservation:
//!   backfill). A reservation ends when the gang places, its wait ends, or
//!   `hold` passes; a wait gets at most one. See [`GangReservation`].
//!
//! Every step reads first and writes only what changed, so a quiet cluster's
//! sweep is a handful of reads and no raft entry.

use super::*;
use crate::manager::blocked::node_could_help;
use crate::resources::{ResourcePool, ResourceRequest};

/// `schedulerSettings.gangReservation`: whether and when a waiting gang
/// reserves capacity (docs/gang-scheduling.md, "Reservations").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GangReservationSettings {
    /// False when the block is `false`: no gang ever reserves.
    pub enabled: bool,
    /// How long a gang waits, short of its quorum, before it reserves.
    pub after: Duration,
    /// How long a reservation holds before it is released, placed or not.
    pub hold: Duration,
}

/// Default `gangReservation.after`.
pub const DEFAULT_GANG_RESERVATION_AFTER: Duration = Duration::from_secs(5 * 60);
/// Default `gangReservation.hold`.
pub const DEFAULT_GANG_RESERVATION_HOLD: Duration = Duration::from_secs(30 * 60);

impl Default for GangReservationSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            after: DEFAULT_GANG_RESERVATION_AFTER,
            hold: DEFAULT_GANG_RESERVATION_HOLD,
        }
    }
}

impl GangReservationSettings {
    /// Parse `schedulerSettings.gangReservation`: `true` / absent (the
    /// defaults), `false` (off), or `{ after = "5m", hold = "30m" }`.
    /// Unknown fields and bad durations are errors, like the rest of
    /// `schedulerSettings`.
    pub fn from_scheduler_settings(settings: Option<&Value>) -> R<Self> {
        let Some(block) = settings.and_then(|settings| settings.get("gangReservation")) else {
            return Ok(Self::default());
        };
        let duration = |field: &str, value: &Value| -> R<Duration> {
            let parsed = match value {
                Value::String(raw) => humantime::parse_duration(raw).with_context(|| {
                    format!("schedulerSettings.gangReservation.{field} {raw:?} is not a duration")
                })?,
                Value::Number(seconds) => {
                    Duration::from_secs(seconds.as_u64().with_context(|| {
                        format!("schedulerSettings.gangReservation.{field} must be >= 0")
                    })?)
                }
                _ => bail!("schedulerSettings.gangReservation.{field} must be a duration"),
            };
            if parsed < Duration::from_secs(1) {
                bail!("schedulerSettings.gangReservation.{field} must be at least 1s");
            }
            Ok(parsed)
        };
        match block {
            Value::Bool(enabled) => Ok(Self {
                enabled: *enabled,
                ..Self::default()
            }),
            Value::Object(fields) => {
                let mut settings = Self::default();
                for (field, value) in fields {
                    match field.as_str() {
                        "after" => settings.after = duration("after", value)?,
                        "hold" => settings.hold = duration("hold", value)?,
                        other => bail!(
                            "unknown field `{other}` in schedulerSettings.gangReservation, \
                             expected `after` or `hold`"
                        ),
                    }
                }
                Ok(settings)
            }
            _ => {
                bail!("schedulerSettings.gangReservation must be true, false, or {{ after, hold }}")
            }
        }
    }
}

/// One gang's capacity reservation, as its row records it.
#[derive(Debug, Clone, PartialEq)]
pub struct GangReservation {
    pub job_config_id: String,
    pub job_id: String,
    pub task_config_id: i64,
    pub task_name: String,
    /// The reserving gang's band: plans of this band or below honor it.
    pub band: crate::structs::Band,
    /// One member's request, normalized.
    pub request: Value,
    /// Members reserved per node.
    pub nodes: std::collections::BTreeMap<String, u32>,
    pub created_unix: i64,
    pub expires_unix: i64,
}

impl GangReservation {
    /// Members reserved in total.
    pub fn members(&self) -> u32 {
        self.nodes.values().sum()
    }

    /// Whether a plan for a job of `band` must leave this reservation's room
    /// alone: everything except a band that outranks the gang's.
    pub fn binds(&self, band: crate::structs::Band) -> bool {
        !band.outranks(self.band)
    }
}

/// What one sweep did, for its log line and tests.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct GangSweep {
    /// Gangs whose members' view changed (a new epoch).
    pub views_synced: usize,
    /// Members marked ready.
    pub ready: usize,
    /// Members past `readyTimeout`, removed as failed.
    pub not_ready: Vec<Allocation>,
    /// Reservations made.
    pub reserved: usize,
    /// Reservations released (placed, ended, or expired).
    pub released: usize,
}

impl Store {
    /// `schedulerSettings.gangReservation`, as this manager was started
    /// with; defaults when it does not parse (startup validates it).
    pub fn gang_reservation_settings(&self) -> GangReservationSettings {
        GangReservationSettings::from_scheduler_settings(self.scheduler_settings.as_ref())
            .unwrap_or_default()
    }

    /// The sweep (module docs). Leader only; the caller checks.
    #[instrument(err, skip_all)]
    pub fn gang_sweep(&self) -> R<GangSweep> {
        let mut sweep = GangSweep {
            views_synced: self.sync_gang_views()?,
            ..Default::default()
        };
        let (ready, not_ready) = self.gang_readiness()?;
        sweep.ready = ready;
        sweep.not_ready = not_ready;
        let (reserved, released) = self.sweep_gang_reservations()?;
        sweep.reserved = reserved;
        sweep.released = released;
        if sweep.views_synced
            + sweep.ready
            + sweep.not_ready.len()
            + sweep.reserved
            + sweep.released
            > 0
        {
            debug!(
                views = sweep.views_synced,
                ready = sweep.ready,
                not_ready = sweep.not_ready.len(),
                reserved = sweep.reserved,
                released = sweep.released,
                "Gang sweep"
            );
        }
        Ok(sweep)
    }

    /// Every gang task with live members: `(job_id, task_config_id,
    /// job_config_id)`.
    fn live_gang_tasks(&self) -> R<Vec<(String, i64, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT g.job_id, a.task_config_id, a.job_config_id
             FROM gang_member g JOIN allocation a ON a.id = g.allocation_id;",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The views step: bring each gang's members to its current size and
    /// quorum. Returns how many gangs got a new epoch.
    fn sync_gang_views(&self) -> R<usize> {
        let mut stale: Vec<(i64, u32, u32)> = Vec::new();
        for (job_id, task_config_id, job_config_id) in self.live_gang_tasks()? {
            let Some(task) = self
                .shared_task_configs_for_job_config(&job_config_id)?
                .iter()
                .find(|(id, _)| *id == task_config_id)
                .map(|(_, task)| task.clone())
            else {
                continue;
            };
            let Some(gang) = task.gang.as_ref() else {
                continue;
            };
            let size =
                self.effective_task_count(&job_id, &task.group, &task.name, task.count.num)?;
            let quorum = gang.quorum(size);
            let conn = self.read_connection()?;
            let members = Self::live_gang_members_tx(&conn, task_config_id, None)?;
            let ceiling = size.max(members.len() as u32);
            let mut ranks: HashSet<u32> = HashSet::new();
            let off = members.iter().any(|member| {
                let member = &member.member;
                member.size != size
                    || member.quorum != quorum
                    || member.rank >= ceiling
                    || !ranks.insert(member.rank)
            });
            if off {
                stale.push((task_config_id, size, quorum));
            }
        }
        if stale.is_empty() {
            return Ok(0);
        }
        self.replicated(Lane::Scheduler, "sync_gang_views", move |s| {
            let conn = s.write_connection()?;
            let mut synced = 0;
            for (task_config_id, size, quorum) in &stale {
                if Self::sync_gang_view_tx(&conn, *task_config_id, *size, *quorum, None)? {
                    synced += 1;
                }
            }
            Ok(synced)
        })
    }

    /// The readiness step: mark members that reported healthy ready, and
    /// return those past their gang's `readyTimeout` (still unready), which
    /// the caller removes as failed. Returns `(marked ready, timed out)`.
    fn gang_readiness(&self) -> R<(usize, Vec<Allocation>)> {
        let unready: Vec<(i64, String, i64, i64, bool)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare_cached(
                "SELECT g.allocation_id, a.job_config_id, a.task_config_id, g.placed_unix,
                        COALESCE(h.healthy, 0)
                 FROM gang_member g
                 JOIN allocation a ON a.id = g.allocation_id
                 LEFT JOIN allocation_health h ON h.allocation_id = g.allocation_id
                 WHERE g.ready = 0;",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get::<_, i64>(4)? != 0,
                ))
            })?;
            rows.collect::<Result<_, _>>()?
        };
        if unready.is_empty() {
            return Ok((0, Vec::new()));
        }
        let now = Self::now_unix();
        let mut ready: Vec<i64> = Vec::new();
        let mut late: Vec<Allocation> = Vec::new();
        for (allocation_id, job_config_id, task_config_id, placed_unix, healthy) in unready {
            if healthy {
                ready.push(allocation_id);
                continue;
            }
            let timeout = self
                .shared_task_configs_for_job_config(&job_config_id)?
                .iter()
                .find(|(id, _)| *id == task_config_id)
                .and_then(|(_, task)| task.gang.as_ref())
                .and_then(|gang| gang.ready_timeout);
            let Some(timeout) = timeout else {
                continue;
            };
            let deadline = placed_unix.saturating_add(timeout.as_secs() as i64);
            // Whole seconds, so never early (as for `placementTimeout`).
            if placed_unix > 0 && deadline < now {
                if let Some(allocation) = self.get_allocation_by_id(allocation_id)? {
                    late.push(allocation);
                }
            }
        }
        let marked = ready.len();
        if !ready.is_empty() {
            self.replicated(Lane::Scheduler, "gang_members_ready", move |s| {
                let conn = s.write_connection()?;
                for allocation_id in &ready {
                    conn.prepare_cached(
                        "UPDATE gang_member SET ready = 1 WHERE allocation_id = ?1;",
                    )?
                    .execute([allocation_id])?;
                }
                Ok(())
            })?;
        }
        Ok((marked, late))
    }

    /// The reservations step: release what has ended or expired, then
    /// reserve for gangs that have waited past `after` and have not had a
    /// reservation in this wait. Returns `(reserved, released)`.
    fn sweep_gang_reservations(&self) -> R<(usize, usize)> {
        let settings = self.gang_reservation_settings();
        let now = Self::now_unix();
        let existing = self.gang_reservations()?;
        let waits: HashMap<String, (i64, Option<i64>)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare_cached(
                "SELECT job_config_id, since_unix, reserved_unix FROM gang_wait;",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, (row.get(1)?, row.get(2)?)))
            })?;
            rows.collect::<Result<_, _>>()?
        };
        let release: Vec<String> = existing
            .iter()
            .filter(|reservation| {
                !settings.enabled
                    || reservation.expires_unix < now
                    || !waits.contains_key(&reservation.job_config_id)
            })
            .map(|reservation| reservation.job_config_id.clone())
            .collect();
        let mut kept: Vec<GangReservation> = existing
            .into_iter()
            .filter(|reservation| !release.contains(&reservation.job_config_id))
            .collect();
        let mut made: Vec<GangReservation> = Vec::new();
        if settings.enabled {
            let blocked = self.blocked_evals_by_job()?;
            // Oldest wait first: it reserves before a younger one can take
            // the same nodes.
            let mut due: Vec<(&String, i64)> = waits
                .iter()
                .filter(|(config, (since, reserved))| {
                    reserved.is_none()
                        && now.saturating_sub(*since) >= settings.after.as_secs() as i64
                        && !kept.iter().any(|kept| &kept.job_config_id == *config)
                })
                .map(|(config, (since, _))| (config, *since))
                .collect();
            due.sort_by_key(|(config, since)| (*since, (*config).clone()));
            for (job_config_id, _) in due {
                let Some(eval) = blocked
                    .values()
                    .find(|eval| &eval.job_config_id == job_config_id)
                else {
                    continue;
                };
                let reserving: Vec<&GangReservation> = kept.iter().chain(&made).collect();
                if let Some(reservation) =
                    self.plan_gang_reservation(eval, &reserving, now, settings.hold)?
                {
                    made.push(reservation);
                }
            }
        }
        if release.is_empty() && made.is_empty() {
            crate::telemetry::metrics()
                .gang_reservations
                .set(kept.len() as i64);
            return Ok((0, 0));
        }
        let (reserved, released) = (made.len(), release.len());
        for reservation in &made {
            info!(
                job_id = %reservation.job_id,
                task = %reservation.task_name,
                members = reservation.members(),
                nodes = reservation.nodes.len(),
                "Gang reserved capacity"
            );
        }
        let writes = made.clone();
        self.replicated(Lane::Scheduler, "sweep_gang_reservations", move |s| {
            let conn = s.write_connection()?;
            for job_config_id in &release {
                conn.prepare_cached("DELETE FROM gang_reservation WHERE job_config_id = ?1;")?
                    .execute([job_config_id])?;
            }
            for reservation in &writes {
                conn.prepare_cached(
                    "INSERT OR REPLACE INTO gang_reservation
                         (job_config_id, job_id, task_config_id, task_name, band, request, nodes,
                          created_unix, expires_unix)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
                )?
                .execute(params![
                    reservation.job_config_id,
                    reservation.job_id,
                    reservation.task_config_id,
                    reservation.task_name,
                    reservation.band.as_str(),
                    reservation.request.to_string(),
                    serde_json::to_string(&reservation.nodes)?,
                    reservation.created_unix,
                    reservation.expires_unix,
                ])?;
                conn.prepare_cached(
                    "UPDATE gang_wait SET reserved_unix = ?2 WHERE job_config_id = ?1;",
                )?
                .execute(params![reservation.job_config_id, reservation.created_unix])?;
            }
            Ok(())
        })?;
        kept.extend(made);
        crate::telemetry::metrics()
            .gang_reservations
            .set(kept.len() as i64);
        Ok((reserved, released))
    }

    /// Choose the nodes a blocked gang reserves: among the nodes that could
    /// take its task (not interrupted, in its topology domain when it named
    /// one), the ones with the most free room for it first, each reserving
    /// as many members as its capacity holds (less other gangs'
    /// reservations), until the members its quorum needs are covered. `None` when they cannot be (the gang could never fit
    /// there, and holding room for it would only idle capacity).
    fn plan_gang_reservation(
        &self,
        eval: &BlockedEval,
        reserving: &[&GangReservation],
        now: i64,
        hold: Duration,
    ) -> R<Option<GangReservation>> {
        let Some(report) = eval.gang() else {
            return Ok(None);
        };
        let need = report.min.saturating_sub(report.running);
        if need == 0 {
            return Ok(None);
        }
        let Some(task_config_id) = eval.detail.get("taskConfigId").and_then(Value::as_i64) else {
            return Ok(None);
        };
        let Some(task) = self
            .shared_task_configs_for_job_config(&eval.job_config_id)?
            .iter()
            .find(|(id, _)| *id == task_config_id)
            .map(|(_, task)| task.clone())
        else {
            return Ok(None);
        };
        let normalized = self
            .resource_policy
            .normalize_task_resources(&task.resources, false)?;
        let request = ResourceRequest::from_value(&normalized)?;
        let band = self.cached_band_for_job_config(&eval.job_config_id)?;
        let available = self.available_nodes()?;
        let pools = self.read_model_reader()?.node_pools.clone();
        // (node, free now, could hold)
        let mut candidates: Vec<(String, u32, u32)> = Vec::new();
        for node in available.as_slice() {
            if node.pending_interruption().is_some()
                || !node_could_help(&eval.key, Some(&task), &eval.job_id, node)
            {
                continue;
            }
            if !report.topology_domain.is_empty() && !report.topology_key.is_empty() {
                let value =
                    crate::constraints::node_attr(node, &report.topology_key).map(|value| {
                        match value {
                            Value::String(text) => text.clone(),
                            other => crate::constraints::value_token(other),
                        }
                    });
                if value.as_deref() != Some(report.topology_domain.as_str()) {
                    continue;
                }
            }
            let Some(free) = pools.get(node.id.as_str()) else {
                continue;
            };
            // What the node could hold for the gang once the work on it
            // leaves: its whole capacity. Any work finishes or moves in
            // time; the reservation is what keeps new work from taking the
            // room as it does.
            let Ok(mut room) = ResourcePool::from_node(node) else {
                continue;
            };
            for other in reserving {
                let Some(members) = other.nodes.get(&node.id) else {
                    continue;
                };
                if let Ok(other_request) = ResourceRequest::from_value(&other.request) {
                    for _ in 0..*members {
                        room.consume(&other_request);
                    }
                }
            }
            let holds = crate::manager::scheduler::topology::replicas_fit(&room, &request, need);
            if holds == 0 {
                continue;
            }
            let fits_now = crate::manager::scheduler::topology::replicas_fit(free, &request, need);
            candidates.push((node.id.clone(), fits_now, holds));
        }
        candidates.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then(right.2.cmp(&left.2))
                .then(left.0.cmp(&right.0))
        });
        let mut nodes: std::collections::BTreeMap<String, u32> = Default::default();
        let mut covered = 0u32;
        for (node_id, _, holds) in candidates {
            if covered >= need {
                break;
            }
            let take = holds.min(need - covered);
            nodes.insert(node_id, take);
            covered += take;
        }
        if covered < need {
            return Ok(None);
        }
        Ok(Some(GangReservation {
            job_config_id: eval.job_config_id.clone(),
            job_id: eval.job_id.clone(),
            task_config_id,
            task_name: task.name.clone(),
            band,
            request: normalized,
            nodes,
            created_unix: now,
            expires_unix: now.saturating_add(hold.as_secs() as i64),
        }))
    }

    /// Every standing reservation (docs/gang-scheduling.md,
    /// "Reservations"). One read of a table with a row per reserving gang.
    pub fn gang_reservations(&self) -> R<Vec<GangReservation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT job_config_id, job_id, task_config_id, task_name, band, request, nodes,
                    created_unix, expires_unix
             FROM gang_reservation ORDER BY created_unix, job_config_id;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        let mut reservations = Vec::new();
        for row in rows {
            let (
                job_config_id,
                job_id,
                task_config_id,
                task_name,
                band,
                request,
                nodes,
                created,
                expires,
            ) = row?;
            let (Ok(request), Ok(nodes)) = (
                serde_json::from_str::<Value>(&request),
                serde_json::from_str::<std::collections::BTreeMap<String, u32>>(&nodes),
            ) else {
                continue;
            };
            reservations.push(GangReservation {
                job_config_id,
                job_id,
                task_config_id,
                task_name,
                band: crate::structs::Band::from_stored(&band),
                request,
                nodes,
                created_unix: created,
                expires_unix: expires,
            });
        }
        Ok(reservations)
    }

    /// The reservation of `job_config_id`'s gang, if it holds one.
    pub fn gang_reservation(&self, job_config_id: &str) -> R<Option<GangReservation>> {
        Ok(self
            .gang_reservations()?
            .into_iter()
            .find(|reservation| reservation.job_config_id == job_config_id))
    }
}

#[cfg(test)]
mod gang_sweep_test {
    use super::*;
    use serde_json::json;

    #[test]
    fn reservation_settings_parse() -> R<()> {
        assert_eq!(
            GangReservationSettings::from_scheduler_settings(None)?,
            GangReservationSettings::default()
        );
        let off = json!({ "gangReservation": false });
        assert!(!GangReservationSettings::from_scheduler_settings(Some(&off))?.enabled);
        let tuned = json!({ "gangReservation": { "after": "90s", "hold": 600 } });
        let tuned = GangReservationSettings::from_scheduler_settings(Some(&tuned))?;
        assert_eq!(tuned.after, Duration::from_secs(90));
        assert_eq!(tuned.hold, Duration::from_secs(600));
        for (bad, needle) in [
            (
                json!({ "gangReservation": { "wait": "1m" } }),
                "unknown field `wait`",
            ),
            (
                json!({ "gangReservation": { "after": "soon" } }),
                "is not a duration",
            ),
            (
                json!({ "gangReservation": { "hold": "0s" } }),
                "at least 1s",
            ),
            (json!({ "gangReservation": 3 }), "must be true, false"),
        ] {
            let error = GangReservationSettings::from_scheduler_settings(Some(&bad))
                .expect_err(needle)
                .to_string();
            assert!(error.contains(needle), "{needle}: {error}");
        }
        Ok(())
    }

    #[test]
    fn a_reservation_binds_its_band_and_below() {
        let reservation = GangReservation {
            job_config_id: "c".into(),
            job_id: "j".into(),
            task_config_id: 1,
            task_name: "worker".into(),
            band: crate::structs::Band::Normal,
            request: json!({ "memory": 256 }),
            nodes: [("n".to_string(), 2)].into_iter().collect(),
            created_unix: 0,
            expires_unix: 0,
        };
        assert!(reservation.binds(crate::structs::Band::Normal));
        assert!(reservation.binds(crate::structs::Band::Low));
        assert!(!reservation.binds(crate::structs::Band::High));
        assert_eq!(reservation.members(), 2);
    }
}
