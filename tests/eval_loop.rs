use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::TaskConfig, job::TaskCount,
    ledger::LedgerEntry, Eval, Submission,
};
use tempfile::Builder;

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("khepri-eval-loop-")
        .tempdir()
        .context("Error creating temp store directory")?
        .keep();

    let store_config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("Error parsing store path")?
            .to_string(),
        ..Default::default()
    };

    let store = Store::start_storage(&store_config)?;
    Ok(store)
}

fn make_job_config(name: &str, count: u32) -> JobConfig {
    let task = TaskConfig {
        count: TaskCount::new(count),
        config: json!({ "message": "hi" }),
        resources: json!({ "memory": 128 }),
        ..TaskConfig::new(format!("{name}-task"), "service")
    };

    JobConfig {
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![task],
        version: None,
    }
}

fn primary_submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.json"),
        content: name.to_string(),
        is_primary: true,
    }
}

fn process_next_eval(store: &Store) -> R<Option<Eval>> {
    match store.get_next_eval()? {
        Some(eval) => {
            let result = eval.handle(store);
            match result {
                Ok(_) => store.set_eval_status(&eval.id, "complete")?,
                Err(_) => store.set_eval_status(&eval.id, "failed")?,
            }

            result.map(|_| Some(eval))
        }
        None => Ok(None),
    }
}

fn eval_status(store: &Store, id: &str) -> R<String> {
    let evals = store.get_evals()?;
    evals
        .into_iter()
        .find(|eval| eval.id == id)
        .map(|eval| eval.status)
        .context("missing eval")
}

fn find_node<'a>(ledger: &'a [LedgerEntry], node_id: &str) -> Option<&'a LedgerEntry> {
    ledger.iter().find(|entry| entry.node.id == node_id)
}

#[test]
fn eval_loop_reconciles_ledger_and_evals() -> R<()> {
    let store = setup_store()?;

    let job_name = "job-1";
    let job_config_v1 = make_job_config(job_name, 1);
    let submissions = vec![primary_submission(job_name)];
    store
        .handle_job_submit_with_template(&job_config_v1, &submissions, None)
        .context("first job submit")?;
    let job_config_v1_id = store.get_job_config(job_name)?.id();

    let job_config_v2 = make_job_config(job_name, 2);
    store
        .handle_job_submit_with_template(&job_config_v2, &submissions, None)
        .context("second job submit")?;
    let job_config_v2_id = store.get_job_config(job_name)?.id();

    let evals = vec![
        Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
            priority: 50,
        },
        Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-2".into(),
            priority: 50,
        },
        Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.into(),
            job_config_id: job_config_v1_id.clone(),
            node_name: String::new(),
            priority: 50,
        },
        Eval {
            id: "4".into(),
            status: "pending".into(),
            kind: "job_update".into(),
            job_id: job_name.into(),
            job_config_id: job_config_v2_id.clone(),
            node_name: String::new(),
            priority: 50,
        },
        Eval {
            id: "5".into(),
            status: "pending".into(),
            kind: "node_drop".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
            priority: 50,
        },
        Eval {
            id: "6".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_name.into(),
            job_config_id: job_config_v2_id.clone(),
            node_name: String::new(),
            priority: 50,
        },
    ];

    for eval in evals {
        store.insert_eval(&eval)?;
    }

    process_next_eval(&store)?.context("expected node join eval")?;
    let mut ledger = store.get_ledger()?;
    assert_eq!(ledger.len(), 1);
    let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
    assert!(node_one_entry.allocations.is_empty());
    assert_eq!(eval_status(&store, "1")?, "complete");

    process_next_eval(&store)?.context("expected second node join eval")?;
    ledger = store.get_ledger()?;
    assert_eq!(ledger.len(), 2);
    let node_two_entry = find_node(&ledger, "node-2").context("missing node-2")?;
    assert!(node_two_entry.allocations.is_empty());
    assert_eq!(eval_status(&store, "2")?, "complete");

    process_next_eval(&store)?.context("expected job submit eval")?;
    ledger = store.get_ledger()?;
    let job_one_allocations: Vec<_> = ledger
        .iter()
        .flat_map(|entry| entry.allocations.iter())
        .filter(|alloc| alloc.job_config_id == job_config_v1_id)
        .collect();
    assert_eq!(job_one_allocations.len(), 1);
    assert_eq!(eval_status(&store, "3")?, "complete");

    process_next_eval(&store)?.context("expected job update eval")?;
    ledger = store.get_ledger()?;
    let updated_allocations: Vec<_> = ledger
        .iter()
        .flat_map(|entry| entry.allocations.iter())
        .filter(|alloc| alloc.job_config_id == job_config_v2_id)
        .collect();
    assert_eq!(updated_allocations.len(), 2);
    assert_eq!(eval_status(&store, "4")?, "complete");

    process_next_eval(&store)?.context("expected node drop eval")?;
    ledger = store.get_ledger()?;
    let node_one_entry = find_node(&ledger, "node-1").context("missing node-1 after drop")?;
    assert_eq!(node_one_entry.allocations.len(), 0);
    let node_two_entry = find_node(&ledger, "node-2").context("missing node-2 after drop")?;
    assert_eq!(node_two_entry.allocations.len(), 2);
    assert_eq!(eval_status(&store, "5")?, "complete");

    process_next_eval(&store)?.context("expected job stop eval")?;
    ledger = store.get_ledger()?;
    let total_allocations: usize = ledger.iter().map(|entry| entry.allocations.len()).sum();
    assert_eq!(total_allocations, 0);
    assert_eq!(eval_status(&store, "6")?, "complete");

    assert!(process_next_eval(&store)?.is_none());

    Ok(())
}

#[test]
fn higher_priority_evals_are_processed_first() -> R<()> {
    let store = setup_store()?;

    let evals = vec![
        Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "low-node".into(),
            priority: 10,
        },
        Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "high-node".into(),
            priority: 90,
        },
    ];

    for eval in evals {
        store.insert_eval(&eval)?;
    }

    let first_eval = store.get_next_eval()?.context("expected a pending eval")?;
    assert_eq!(first_eval.id, "2");
    store.set_eval_status(&first_eval.id, "complete")?;

    let second_eval = store.get_next_eval()?.context("expected remaining eval")?;
    assert_eq!(second_eval.id, "1");

    Ok(())
}
