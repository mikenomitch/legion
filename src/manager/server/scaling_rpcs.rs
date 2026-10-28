//! The read half of the scale surface (docs/autoscaling.md): the
//! `GetJobScaleStatus` rows.

use super::*;
use crate::storage::store::TaskScale;
use crate::structs::TaskScaleStatus;

/// The GetJobScaleStatus rows for `config`, the job's latest config: every
/// effective count read through [`Store::effective_task_count`], the one
/// accessor the scheduler, rollouts, and quota use, so the status can never
/// disagree with placement. An override that does not bind to the current
/// spec (`TaskScale::binds_to`) is not reported as the override.
pub(crate) fn task_scale_statuses(
    store: &Store,
    job_id: &str,
    config: &JobConfig,
    scales: &[TaskScale],
) -> R<Vec<TaskScaleStatus>> {
    config
        .tasks
        .iter()
        .map(|task| {
            let spec_count = task.count.num;
            let scale = scales
                .iter()
                .find(|scale| scale.task_group == task.group && scale.task_name == task.name);
            let effective_count =
                store.effective_task_count(job_id, &task.group, &task.name, spec_count)?;
            Ok(TaskScaleStatus {
                task_group: task.group.clone(),
                task_name: task.name.clone(),
                spec_count,
                override_count: scale
                    .filter(|scale| scale.binds_to(spec_count))
                    .and_then(|scale| scale.count),
                pending_count: scale.and_then(|scale| scale.pending_count),
                effective_count,
                source: scale.map(|scale| scale.source.clone()).unwrap_or_default(),
                reason: scale.map(|scale| scale.reason.clone()).unwrap_or_default(),
                updated_at: scale
                    .map(|scale| scale.updated_at.clone())
                    .unwrap_or_default(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::Submission;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    fn job(count: u32) -> JobConfig {
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "web".into(),
            tasks: vec![TaskConfig {
                count: crate::structs::TaskCount::new(count),
                config: json!({ "message": "hi" }),
                ..TaskConfig::new("api".to_string(), "noop")
            }],
            version: None,
        }
    }

    fn submit(store: &Store, count: u32) -> R<()> {
        store.handle_job_submit(
            &job(count),
            &[Submission {
                path: "web.json".into(),
                content: format!("web-{count}"),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    fn status(store: &Store) -> R<TaskScaleStatus> {
        let config = store.get_job_config("web")?;
        let scales = store.task_scales_for_job("web")?;
        let rows = task_scale_statuses(store, "web", &config, &scales)?;
        rows.into_iter().next().context("one task")
    }

    /// GetJobScaleStatus agrees with `effective_task_count` in every override
    /// state: none, applied, pending-only, and one laid over a different
    /// spec count (which must not bind).
    #[test]
    fn scale_status_agrees_with_effective_task_count() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2)?;
        let effective = |store: &Store| store.effective_task_count("web", "default", "api", 2);

        // No override.
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!((row.effective_count, row.override_count), (2, None));

        // An applied override over the current spec.
        store.set_task_scale_override("web", "default", "api", 5, 2, "operator", "", "me")?;
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!((row.effective_count, row.override_count), (5, Some(5)));

        // An override laid over a DIFFERENT spec count does not bind: the
        // old code reported it as effective; the scheduler never used it.
        store.set_task_scale_override("web", "default", "api", 7, 3, "operator", "", "me")?;
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!((row.effective_count, row.override_count), (2, None));

        // Pending scale-in only.
        store.clear_task_scale("web", "default", "api")?;
        store.set_task_scale_pending("web", "default", "api", 1, 2, "operator", "", "me")?;
        let row = status(&store)?;
        assert_eq!(row.effective_count, effective(&store)?);
        assert_eq!(row.effective_count, 2);
        assert_eq!(row.pending_count, Some(1));
        Ok(())
    }
}
