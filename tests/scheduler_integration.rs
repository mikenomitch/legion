use std::collections::{HashMap, HashSet};

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::TaskConfig, job::TaskCount, Eval, Node,
    NodeMsg, Submission,
};
use serde::Serialize;
use serde_json::{Map, Value};
use tempfile::Builder;

#[derive(Clone)]
struct NodeSpec {
    id: String,
    zone: String,
    rack: String,
    tier: String,
    memory_mib: f64,
    cpu_mhz: f64,
    resources: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LedgerJsonNode {
    id: String,
    status: String,
    availability: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LedgerJsonAllocation {
    id: i64,
    status: String,
    resources: Value,
    task_config_id: i64,
    job_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LedgerJsonEntry {
    node: LedgerJsonNode,
    allocations: Vec<LedgerJsonAllocation>,
}

struct TestCluster {
    store: Store,
    eval_seq: u64,
    job_config_ids: HashMap<String, String>,
}

impl TestCluster {
    fn new(policy: &str) -> R<Self> {
        Self::new_with_settings(policy, Some(json!({ "nodes_considered": "100%" })))
    }

    fn new_with_settings(policy: &str, scheduler_settings: Option<Value>) -> R<Self> {
        let store_dir = Builder::new()
            .prefix("legion-scheduler-it-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();

        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            scheduling_policy: policy.to_string(),
            scheduler_settings,
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        Ok(Self {
            store,
            eval_seq: 0,
            job_config_ids: HashMap::new(),
        })
    }

    fn add_nodes(&self, nodes: &[NodeSpec]) -> R<()> {
        for node in nodes {
            let node = build_node(node);
            self.store.insert_node(NodeMsg::from(node))?;
        }
        Ok(())
    }

    fn submit_job(&mut self, job_config: JobConfig) -> R<()> {
        let job_name = job_config.name.clone();
        let submissions = vec![primary_submission(&job_name)];
        self.store
            .handle_job_submit(&job_config, &submissions)
            .with_context(|| format!("submit job {job_name}"))?;
        let job_config_id = self.store.get_job_config(&job_name)?.id();
        self.job_config_ids
            .insert(job_name.clone(), job_config_id.clone());
        self.enqueue_eval("job_submit", Some(&job_name), Some(&job_config_id), None)?;
        self.process_next_eval()?
            .context("expected job_submit eval")?;
        Ok(())
    }

    fn submit_job_expect_failure(&mut self, job_config: JobConfig) -> R<String> {
        let job_name = job_config.name.clone();
        let submissions = vec![primary_submission(&job_name)];
        self.store
            .handle_job_submit(&job_config, &submissions)
            .with_context(|| format!("submit job {job_name}"))?;
        let job_config_id = self.store.get_job_config(&job_name)?.id();
        self.job_config_ids
            .insert(job_name.clone(), job_config_id.clone());
        let eval_id =
            self.enqueue_eval("job_submit", Some(&job_name), Some(&job_config_id), None)?;
        let _ = self.process_next_eval();
        Ok(eval_id)
    }

    fn stop_job(&mut self, job_name: &str) -> R<()> {
        let job_config_id = self.job_config_id(job_name)?;
        self.enqueue_eval("job_stop", Some(job_name), Some(&job_config_id), None)?;
        self.process_next_eval()?
            .context("expected job_stop eval")?;
        Ok(())
    }

    fn reschedule_job(&mut self, job_name: &str) -> R<()> {
        let job_config_id = self.job_config_id(job_name)?;
        self.enqueue_eval("job_reschedule", Some(job_name), Some(&job_config_id), None)?;
        self.process_next_eval()?
            .context("expected job_reschedule eval")?;
        Ok(())
    }

    fn drop_node(&mut self, node_id: &str, reschedule_jobs: &[&str]) -> R<()> {
        self.store.mark_node_lost(node_id)?;
        self.store.remove_allocs_for_node(node_id)?;
        for job in reschedule_jobs {
            self.reschedule_job(job)?;
        }
        Ok(())
    }

    fn join_node(&mut self, node_id: &str) -> R<()> {
        self.enqueue_eval("node_join", None, None, Some(node_id))?;
        self.process_next_eval()?
            .context("expected node_join eval")?;
        Ok(())
    }

    fn add_dynamic_attr(&self, node_id: &str, key: &str, value: Value) -> R<()> {
        let mut diff = Map::new();
        diff.insert(key.to_string(), value);
        self.store.update_node_dyamic(node_id, &diff)?;
        Ok(())
    }

    fn job_config_id(&self, job_name: &str) -> R<String> {
        self.job_config_ids
            .get(job_name)
            .cloned()
            .context("missing job config id")
    }

    fn enqueue_eval(
        &mut self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_name: Option<&str>,
    ) -> R<String> {
        self.eval_seq += 1;
        let id = self.eval_seq.to_string();
        let eval = Eval {
            id: id.clone(),
            status: "pending".into(),
            kind: kind.into(),
            job_id: job_id.unwrap_or_default().to_string(),
            job_config_id: job_config_id.unwrap_or_default().to_string(),
            node_name: node_name.unwrap_or_default().to_string(),
            priority: 50,
        };
        self.store.insert_eval(&eval)?;
        Ok(id)
    }

    fn process_next_eval(&self) -> R<Option<Eval>> {
        match self.store.get_next_eval()? {
            Some(eval) => {
                let result = eval.handle(&self.store);
                match result {
                    Ok(_) => self.store.set_eval_status(&eval.id, "complete")?,
                    Err(_) => self.store.set_eval_status(&eval.id, "failed")?,
                }
                result.map(|_| Some(eval))
            }
            None => Ok(None),
        }
    }

    fn eval_status(&self, id: &str) -> R<String> {
        let evals = self.store.get_evals()?;
        evals
            .into_iter()
            .find(|eval| eval.id == id)
            .map(|eval| eval.status)
            .context("missing eval")
    }

    fn ledger_json(&self) -> R<Value> {
        let mut entries: Vec<LedgerJsonEntry> = self
            .store
            .get_ledger()?
            .into_iter()
            .map(|entry| {
                let mut allocations = entry
                    .allocations
                    .into_iter()
                    .map(|alloc| LedgerJsonAllocation {
                        id: alloc.id,
                        status: alloc.status,
                        resources: alloc.resources,
                        task_config_id: alloc.task_config_id,
                        job_id: alloc.job_id,
                    })
                    .collect::<Vec<_>>();
                allocations.sort_by_key(|alloc| alloc.id);
                LedgerJsonEntry {
                    node: LedgerJsonNode {
                        id: entry.node.id,
                        status: entry.node.status,
                        availability: entry.node.availability,
                    },
                    allocations,
                }
            })
            .collect();
        entries.sort_by(|a, b| a.node.id.cmp(&b.node.id));
        serde_json::to_value(entries).context("serialize ledger to json")
    }

    fn job_nodes(&self, job_id: &str) -> R<Vec<String>> {
        let ledger = self.ledger_json()?;
        let mut nodes = Vec::new();
        let entries = ledger.as_array().context("ledger json expected array")?;
        for entry in entries {
            let node_id = entry
                .get("node")
                .and_then(|node| node.get("id"))
                .and_then(|id| id.as_str())
                .context("ledger json missing node id")?;
            let allocations = entry
                .get("allocations")
                .and_then(|allocs| allocs.as_array())
                .context("ledger json missing allocations")?;
            for alloc in allocations {
                let alloc_job_id = alloc
                    .get("jobId")
                    .and_then(|id| id.as_str())
                    .context("ledger json missing job id")?;
                if alloc_job_id == job_id {
                    nodes.push(node_id.to_string());
                }
            }
        }
        Ok(nodes)
    }

    fn job_node_set(&self, job_id: &str) -> R<HashSet<String>> {
        Ok(self.job_nodes(job_id)?.into_iter().collect())
    }

    fn job_allocations_by_attr(&self, job_id: &str, attr: &str) -> R<HashMap<String, usize>> {
        let ledger = self.store.get_ledger()?;
        let mut counts = HashMap::new();
        for entry in ledger {
            let Some(value) = node_attribute(&entry.node, attr) else {
                continue;
            };
            let alloc_count = entry
                .allocations
                .iter()
                .filter(|alloc| alloc.job_id == job_id)
                .count();
            if alloc_count == 0 {
                continue;
            }
            let key = value_key(value);
            *counts.entry(key).or_default() += alloc_count;
        }
        Ok(counts)
    }

    fn assert_job_nodes(&self, job_id: &str, expected_nodes: &[&str]) -> R<()> {
        let mut actual = self.job_nodes(job_id)?;
        actual.sort();
        let mut expected = expected_nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(
            actual, expected,
            "job {job_id} allocations mismatch in ledger json"
        );
        Ok(())
    }

    fn assert_job_nodes_in(
        &self,
        job_id: &str,
        allowed_nodes: &[&str],
        expected_count: usize,
    ) -> R<()> {
        let actual = self.job_nodes(job_id)?;
        assert_eq!(
            actual.len(),
            expected_count,
            "job {job_id} allocation count mismatch in ledger json"
        );
        let allowed = allowed_nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<HashSet<_>>();
        for node in actual {
            assert!(
                allowed.contains(&node),
                "job {job_id} allocations mismatch in ledger json"
            );
        }
        Ok(())
    }

    fn assert_job_node_set(&self, job_id: &str, expected_nodes: &[&str]) -> R<()> {
        let actual = self.job_node_set(job_id)?;
        let expected = expected_nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<HashSet<_>>();
        assert_eq!(
            actual, expected,
            "job {job_id} allocations mismatch in ledger json"
        );
        Ok(())
    }

    fn assert_job_absent(&self, job_id: &str) -> R<()> {
        let nodes = self.job_nodes(job_id)?;
        assert!(
            nodes.is_empty(),
            "job {job_id} allocations still present in ledger json"
        );
        Ok(())
    }

    fn snapshot_job_nodes(&self, job_ids: &[&str]) -> R<HashMap<String, Vec<String>>> {
        let mut snapshot = HashMap::new();
        for job_id in job_ids {
            let mut nodes = self.job_nodes(job_id)?;
            nodes.sort();
            snapshot.insert(job_id.to_string(), nodes);
        }
        Ok(snapshot)
    }

    fn assert_job_snapshot(&self, snapshot: &HashMap<String, Vec<String>>) -> R<()> {
        for (job_id, expected_nodes) in snapshot {
            let mut actual = self.job_nodes(job_id)?;
            actual.sort();
            assert_eq!(
                &actual, expected_nodes,
                "job {job_id} allocations changed in ledger json"
            );
        }
        Ok(())
    }
}

fn node_attribute<'a>(node: &'a Node, key: &str) -> Option<&'a Value> {
    node.config_attrs
        .as_object()
        .and_then(|attrs| attrs.get(key))
        .or_else(|| {
            node.fingerprint_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
        .or_else(|| {
            node.dynamic_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
}

fn value_key(value: &Value) -> String {
    value
        .as_str()
        .map(|value| value.to_string())
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_default())
}

fn base_node_specs() -> Vec<NodeSpec> {
    vec![
        NodeSpec {
            id: "node-1".into(),
            zone: "east".into(),
            rack: "rack-a".into(),
            tier: "silver".into(),
            memory_mib: 8192.0,
            cpu_mhz: 4800.0,
            resources: json!({ "gpu": 1, "disk": 200, "nic": ["25g"] }),
        },
        NodeSpec {
            id: "node-2".into(),
            zone: "east".into(),
            rack: "rack-a".into(),
            tier: "gold".into(),
            memory_mib: 16384.0,
            cpu_mhz: 6400.0,
            resources: json!({ "gpu": 1, "disk": 300, "nic": ["25g", "100g"] }),
        },
        NodeSpec {
            id: "node-3".into(),
            zone: "east".into(),
            rack: "rack-b".into(),
            tier: "silver".into(),
            memory_mib: 8192.0,
            cpu_mhz: 3600.0,
            resources: json!({ "fpga": 1, "disk": 150 }),
        },
        NodeSpec {
            id: "node-4".into(),
            zone: "east".into(),
            rack: "rack-b".into(),
            tier: "gold".into(),
            memory_mib: 12288.0,
            cpu_mhz: 4000.0,
            resources: json!({ "fpga": 2, "disk": 250 }),
        },
        NodeSpec {
            id: "node-5".into(),
            zone: "central".into(),
            rack: "rack-c".into(),
            tier: "gold".into(),
            memory_mib: 32768.0,
            cpu_mhz: 7200.0,
            resources: json!({ "gpu": 2, "disk": 500 }),
        },
        NodeSpec {
            id: "node-6".into(),
            zone: "central".into(),
            rack: "rack-c".into(),
            tier: "silver".into(),
            memory_mib: 16384.0,
            cpu_mhz: 4800.0,
            resources: json!({ "gpu": 0, "disk": 400 }),
        },
        NodeSpec {
            id: "node-7".into(),
            zone: "west".into(),
            rack: "rack-d".into(),
            tier: "silver".into(),
            memory_mib: 8192.0,
            cpu_mhz: 3200.0,
            resources: json!({ "tpu": 1, "disk": 100, "nic": ["25g"] }),
        },
        NodeSpec {
            id: "node-8".into(),
            zone: "west".into(),
            rack: "rack-d".into(),
            tier: "gold".into(),
            memory_mib: 16384.0,
            cpu_mhz: 5600.0,
            resources: json!({ "tpu": 2, "disk": 200, "nic": ["100g"] }),
        },
        NodeSpec {
            id: "node-9".into(),
            zone: "west".into(),
            rack: "rack-e".into(),
            tier: "silver".into(),
            memory_mib: 8192.0,
            cpu_mhz: 3600.0,
            resources: json!({ "disk": 150, "nic": ["25g"] }),
        },
        NodeSpec {
            id: "node-10".into(),
            zone: "west".into(),
            rack: "rack-e".into(),
            tier: "gold".into(),
            memory_mib: 16384.0,
            cpu_mhz: 5200.0,
            resources: json!({ "disk": 300, "nic": ["100g"] }),
        },
    ]
}

fn scoring_node_specs() -> Vec<NodeSpec> {
    vec![
        NodeSpec {
            id: "score-node-1".into(),
            zone: "score".into(),
            rack: "rack-1".into(),
            tier: "silver".into(),
            memory_mib: 2048.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
        NodeSpec {
            id: "score-node-2".into(),
            zone: "score".into(),
            rack: "rack-1".into(),
            tier: "silver".into(),
            memory_mib: 2048.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
    ]
}

fn extra_node_specs(count: usize, start_index: usize) -> Vec<NodeSpec> {
    (0..count)
        .map(|i| {
            let id = format!("node-extra-{}", start_index + i + 1);
            NodeSpec {
                id,
                zone: "overflow".into(),
                rack: "rack-z".into(),
                tier: "silver".into(),
                memory_mib: 4096.0,
                cpu_mhz: 2000.0,
                resources: json!({ "disk": 50 }),
            }
        })
        .collect()
}

fn build_node(spec: &NodeSpec) -> Node {
    // Workers fingerprint memory in bytes (sysinfo::System::total_memory).
    let memory_total = format!("{}", (spec.memory_mib * 1024.0 * 1024.0) as u64);
    let cpu_per_core = spec.cpu_mhz / 2.0;
    let config_attrs = json!({
        "zone": spec.zone,
        "rack": spec.rack,
        "tier": spec.tier,
        "node_id": spec.id,
        "maxAllocations": 20,
        "resources": spec.resources,
    });
    let fingerprint_attrs = json!({
        "memory_total": memory_total,
        "cpus": {
            "cpu-1": { "frequency": cpu_per_core },
            "cpu-2": { "frequency": cpu_per_core }
        }
    });

    Node {
        id: spec.id.clone(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        config_attrs,
        fingerprint_attrs,
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn primary_submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.json"),
        content: name.to_string(),
        is_primary: true,
    }
}

fn task_config(
    name: &str,
    count: u32,
    resources: Value,
    constraints: Value,
    affinities: Value,
) -> TaskConfig {
    TaskConfig {
        count: TaskCount::new(count),
        config: json!({ "message": "hi" }),
        resources,
        constraints,
        affinities,
        ..TaskConfig::new(name, "service")
    }
}

fn job_alpha() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-alpha".into(),
        tasks: vec![task_config(
            "alpha-task",
            1,
            json!({ "memory": 256, "cpu": 200, "gpu": 1 }),
            json!({ "zone": "east" }),
            json!([{ "key": "tier", "value": "gold", "multiplier": 2 }]),
        )],
        version: None,
    }
}

fn job_bravo() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-bravo".into(),
        tasks: vec![task_config(
            "bravo-task",
            1,
            json!({ "memory": 256, "cpu": 200, "fpga": 1 }),
            json!({ "zone": "east", "rack": "rack-b" }),
            json!([{ "key": "tier", "value": "gold", "multiplier": 2 }]),
        )],
        version: None,
    }
}

fn job_charlie() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-charlie".into(),
        tasks: vec![task_config(
            "charlie-task",
            1,
            json!({ "memory": 512, "cpu": 400, "gpu": 2 }),
            json!({ "zone": "central" }),
            json!([{ "key": "tier", "value": "gold", "multiplier": 2 }]),
        )],
        version: None,
    }
}

fn job_delta() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-delta".into(),
        tasks: vec![task_config(
            "delta-task",
            1,
            json!({ "memory": 128, "cpu": 100, "nic": ["100g"] }),
            json!({ "zone": "west" }),
            json!([{ "key": "rack", "value": "rack-d", "multiplier": 3 }]),
        )],
        version: None,
    }
}

fn job_echo() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-echo".into(),
        tasks: vec![task_config(
            "echo-task",
            1,
            json!({ "memory": 128, "cpu": 100, "tpu": 1 }),
            json!({ "zone": "west" }),
            json!([{ "key": "tier", "value": "silver", "multiplier": 2 }]),
        )],
        version: None,
    }
}

fn job_foxtrot() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-foxtrot".into(),
        tasks: vec![task_config(
            "foxtrot-task",
            1,
            json!({ "memory": 128, "cpu": 100, "fpga": 1 }),
            json!({ "special": "enabled" }),
            json!({}),
        )],
        version: None,
    }
}

fn job_golf() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-golf".into(),
        tasks: vec![task_config(
            "golf-task",
            3,
            json!({ "memory": 128, "cpu": 100 }),
            json!([
                { "key": "zone", "value": "west", "operator": "=" },
                { "key": "node_id", "operator": "distinct" }
            ]),
            json!({}),
        )],
        version: None,
    }
}

fn job_hotel() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-hotel".into(),
        tasks: vec![task_config(
            "hotel-task",
            1,
            json!({ "memory": 128, "cpu": 100 }),
            json!({ "zone": "moon" }),
            json!({}),
        )],
        version: None,
    }
}

fn job_india() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-india".into(),
        tasks: vec![task_config(
            "india-task",
            1,
            json!({ "memory": 128, "cpu": 100, "nic": ["100g"] }),
            json!({ "node_id": "node-11" }),
            json!({}),
        )],
        version: None,
    }
}

fn job_resubmit_config(message: &str, count: u32, node_id: &str) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-resubmit".into(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            config: json!({ "message": message }),
            resources: json!({ "memory": 128, "cpu": 100 }),
            constraints: json!({ "node_id": node_id }),
            ..TaskConfig::new("resubmit-task", "service")
        }],
        version: None,
    }
}

fn job_score_load() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-score-load".into(),
        tasks: vec![task_config(
            "score-load-task",
            1,
            json!({ "memory": 64, "cpu": 50 }),
            json!({ "node_id": "score-node-1" }),
            json!({}),
        )],
        version: None,
    }
}

fn job_score_target() -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-score-target".into(),
        tasks: vec![task_config(
            "score-target-task",
            1,
            json!({ "memory": 64, "cpu": 50 }),
            json!({ "zone": "score" }),
            json!({}),
        )],
        version: None,
    }
}

fn run_scheduler_scenario(policy: &str, node_count: usize) -> R<TestCluster> {
    let mut cluster = TestCluster::new(policy)?;
    let mut nodes = base_node_specs();
    if node_count > nodes.len() {
        let extra = extra_node_specs(node_count - nodes.len(), nodes.len());
        nodes.extend(extra);
    }
    cluster.add_nodes(&nodes)?;

    cluster.submit_job(job_alpha())?;
    cluster.assert_job_nodes("job-alpha", &["node-2"])?;

    cluster.submit_job(job_bravo())?;
    cluster.assert_job_nodes("job-bravo", &["node-4"])?;

    cluster.submit_job(job_charlie())?;
    cluster.assert_job_nodes("job-charlie", &["node-5"])?;

    cluster.submit_job(job_delta())?;
    cluster.assert_job_nodes("job-delta", &["node-8"])?;

    cluster.submit_job(job_echo())?;
    cluster.assert_job_nodes_in("job-echo", &["node-7", "node-8"], 1)?;

    cluster.drop_node("node-7", &["job-echo"])?;
    cluster.assert_job_nodes("job-echo", &["node-8"])?;

    cluster.drop_node("node-2", &["job-alpha"])?;
    cluster.assert_job_nodes("job-alpha", &["node-1"])?;

    cluster.add_dynamic_attr("node-3", "special", json!("enabled"))?;
    cluster.submit_job(job_foxtrot())?;
    cluster.assert_job_nodes("job-foxtrot", &["node-3"])?;

    cluster.stop_job("job-bravo")?;
    cluster.assert_job_absent("job-bravo")?;

    cluster.stop_job("job-charlie")?;
    cluster.assert_job_absent("job-charlie")?;

    cluster.submit_job(job_golf())?;
    cluster.assert_job_nodes_in("job-golf", &["node-7", "node-8", "node-9", "node-10"], 3)?;

    let hotel_eval = cluster.submit_job_expect_failure(job_hotel())?;
    assert_eq!(cluster.eval_status(&hotel_eval)?, "failed");
    cluster.assert_job_absent("job-hotel")?;

    let tracked_jobs = [
        "job-alpha",
        "job-delta",
        "job-echo",
        "job-foxtrot",
        "job-golf",
    ];
    let snapshot = cluster.snapshot_job_nodes(&tracked_jobs)?;
    cluster.add_nodes(&[NodeSpec {
        id: "node-11".into(),
        zone: "west".into(),
        rack: "rack-f".into(),
        tier: "gold".into(),
        memory_mib: 8192.0,
        cpu_mhz: 4800.0,
        resources: json!({ "disk": 100, "nic": ["100g"] }),
    }])?;
    cluster.assert_job_snapshot(&snapshot)?;

    cluster.submit_job(job_india())?;
    cluster.assert_job_nodes("job-india", &["node-11"])?;

    Ok(cluster)
}

fn run_scoring_scenario(policy: &str, scheduler_settings: Option<Value>) -> R<TestCluster> {
    let mut cluster = TestCluster::new_with_settings(policy, scheduler_settings)?;
    cluster.add_nodes(&scoring_node_specs())?;

    cluster.submit_job(job_score_load())?;
    cluster.assert_job_nodes("job-score-load", &["score-node-1"])?;

    cluster.submit_job(job_score_target())?;
    Ok(cluster)
}

#[test]
fn scheduler_integration_spread_policy() -> R<()> {
    let _cluster = run_scheduler_scenario("spread", 10)?;
    Ok(())
}

#[test]
fn scheduler_integration_bin_pack_policy() -> R<()> {
    let _cluster = run_scheduler_scenario("bin_pack", 10)?;
    Ok(())
}

#[test]
fn scheduler_integration_spread_policy_large_cluster() -> R<()> {
    let mut cluster = run_scheduler_scenario("spread", 50)?;
    let resubmit_job = "job-resubmit";

    cluster.submit_job(job_resubmit_config("v1", 1, "node-6"))?;
    cluster.assert_job_nodes(resubmit_job, &["node-6"])?;

    let snapshot = cluster.snapshot_job_nodes(&[resubmit_job])?;
    cluster.submit_job(job_resubmit_config("v2", 1, "node-6"))?;
    cluster.assert_job_snapshot(&snapshot)?;

    cluster.submit_job(job_resubmit_config("v3", 1, "node-8"))?;
    cluster.assert_job_nodes(resubmit_job, &["node-8"])?;

    cluster.submit_job(job_resubmit_config("v4", 0, "node-8"))?;
    cluster.assert_job_absent(resubmit_job)?;

    Ok(())
}

#[test]
fn scheduler_scoring_diverges_between_policies() -> R<()> {
    let spread_cluster =
        run_scoring_scenario("spread", Some(json!({ "nodes_considered": "100%" })))?;
    spread_cluster.assert_job_nodes("job-score-target", &["score-node-2"])?;

    let bin_pack_cluster =
        run_scoring_scenario("bin_pack", Some(json!({ "nodes_considered": "100%" })))?;
    bin_pack_cluster.assert_job_nodes("job-score-target", &["score-node-1"])?;
    Ok(())
}

#[test]
fn scheduler_scoring_weight_override() -> R<()> {
    let settings = json!({
        "nodes_considered": "100%",
        "weights": {
            "affinities": 0.0,
            "spread": 0.0,
            "bin_pack": 2.0
        }
    });
    let cluster = run_scoring_scenario("spread", Some(settings))?;
    cluster.assert_job_nodes("job-score-target", &["score-node-1"])?;
    Ok(())
}

#[test]
fn scheduler_count_per_attribute_reschedules_by_group() -> R<()> {
    let mut cluster = TestCluster::new("spread")?;
    let nodes = vec![
        NodeSpec {
            id: "node-east-1".into(),
            zone: "east".into(),
            rack: "rack-a".into(),
            tier: "gold".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
        NodeSpec {
            id: "node-east-2".into(),
            zone: "east".into(),
            rack: "rack-b".into(),
            tier: "gold".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
        NodeSpec {
            id: "node-west-1".into(),
            zone: "west".into(),
            rack: "rack-c".into(),
            tier: "silver".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
        NodeSpec {
            id: "node-west-2".into(),
            zone: "west".into(),
            rack: "rack-d".into(),
            tier: "silver".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
    ];
    cluster.add_nodes(&nodes)?;
    for node in &nodes {
        cluster.add_dynamic_attr(&node.id, "region", json!(node.zone))?;
    }

    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-per-region".into(),
        tasks: vec![TaskConfig {
            count: TaskCount::per(3, "region"),
            ..TaskConfig::new("region-task", "service")
        }],
        version: None,
    };

    cluster.submit_job(job)?;
    let initial_nodes = cluster.job_nodes("job-per-region")?;
    assert_eq!(initial_nodes.len(), 6);

    let counts = cluster.job_allocations_by_attr("job-per-region", "region")?;
    assert_eq!(counts.get("east"), Some(&3));
    assert_eq!(counts.get("west"), Some(&3));

    cluster.drop_node("node-east-1", &["job-per-region"])?;
    let rescheduled_nodes = cluster.job_nodes("job-per-region")?;
    assert_eq!(rescheduled_nodes.len(), 6);
    assert!(!rescheduled_nodes.contains(&"node-east-1".to_string()));

    let rescheduled_counts = cluster.job_allocations_by_attr("job-per-region", "region")?;
    assert_eq!(rescheduled_counts.get("east"), Some(&3));
    assert_eq!(rescheduled_counts.get("west"), Some(&3));

    Ok(())
}

#[test]
fn scheduler_count_per_id_scales_with_node_changes() -> R<()> {
    let mut cluster = TestCluster::new("spread")?;
    let nodes = vec![
        NodeSpec {
            id: "node-1".into(),
            zone: "east".into(),
            rack: "rack-a".into(),
            tier: "gold".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
        NodeSpec {
            id: "node-2".into(),
            zone: "east".into(),
            rack: "rack-b".into(),
            tier: "gold".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
        NodeSpec {
            id: "node-3".into(),
            zone: "west".into(),
            rack: "rack-c".into(),
            tier: "silver".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 50 }),
        },
    ];
    cluster.add_nodes(&nodes)?;

    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-per-id".into(),
        tasks: vec![TaskConfig {
            count: TaskCount::per(1, "id"),
            ..TaskConfig::new("id-task", "service")
        }],
        version: None,
    };

    cluster.submit_job(job)?;
    cluster.assert_job_node_set("job-per-id", &["node-1", "node-2", "node-3"])?;

    cluster.drop_node("node-2", &["job-per-id"])?;
    let after_drop = cluster.job_nodes("job-per-id")?;
    assert_eq!(after_drop.len(), 2);
    assert!(!after_drop.contains(&"node-2".to_string()));

    cluster.join_node("node-2")?;
    cluster.assert_job_node_set("job-per-id", &["node-1", "node-2", "node-3"])?;

    cluster.join_node("node-4")?;
    let after_add = cluster.job_nodes("job-per-id")?;
    assert_eq!(after_add.len(), 4);
    assert!(after_add.contains(&"node-4".to_string()));

    Ok(())
}

#[test]
fn scheduler_multi_job_churn_constraints_and_count_per_reschedule() -> R<()> {
    let mut cluster = TestCluster::new("spread")?;
    let nodes = vec![
        NodeSpec {
            id: "east-a".into(),
            zone: "east".into(),
            rack: "rack-a".into(),
            tier: "gold".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 80 }),
        },
        NodeSpec {
            id: "east-b".into(),
            zone: "east".into(),
            rack: "rack-b".into(),
            tier: "gold".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 80 }),
        },
        NodeSpec {
            id: "west-a".into(),
            zone: "west".into(),
            rack: "rack-c".into(),
            tier: "silver".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 80 }),
        },
        NodeSpec {
            id: "west-b".into(),
            zone: "west".into(),
            rack: "rack-d".into(),
            tier: "silver".into(),
            memory_mib: 4096.0,
            cpu_mhz: 2400.0,
            resources: json!({ "disk": 80 }),
        },
    ];
    cluster.add_nodes(&nodes)?;
    for node in &nodes {
        cluster.add_dynamic_attr(&node.id, "region", json!(node.zone))?;
    }

    let regional_job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-regional".into(),
        tasks: vec![TaskConfig {
            count: TaskCount::per(2, "region"),
            constraints: json!({}),
            resources: json!({ "memory": 128, "cpu": 100 }),
            ..TaskConfig::new("regional", "service")
        }],
        version: None,
    };

    let constrained_job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "job-constrained".into(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(2),
            constraints: json!({ "zone": "east" }),
            resources: json!({ "memory": 128, "cpu": 100 }),
            ..TaskConfig::new("east-only", "service")
        }],
        version: None,
    };

    cluster.submit_job(regional_job)?;
    cluster.submit_job(constrained_job)?;

    let initial_regional = cluster.job_allocations_by_attr("job-regional", "region")?;
    assert_eq!(initial_regional.get("east"), Some(&2));
    assert_eq!(initial_regional.get("west"), Some(&2));
    cluster.assert_job_nodes_in("job-constrained", &["east-a", "east-b"], 2)?;

    cluster.drop_node("east-a", &["job-regional", "job-constrained"])?;
    cluster.assert_job_nodes_in("job-constrained", &["east-b"], 2)?;

    let after_drop_regional = cluster.job_allocations_by_attr("job-regional", "region")?;
    assert_eq!(after_drop_regional.get("east"), Some(&2));
    assert_eq!(after_drop_regional.get("west"), Some(&2));

    cluster.join_node("east-a")?;
    cluster.reschedule_job("job-regional")?;
    cluster.reschedule_job("job-constrained")?;

    let after_rejoin_regional = cluster.job_allocations_by_attr("job-regional", "region")?;
    assert_eq!(after_rejoin_regional.get("east"), Some(&2));
    assert_eq!(after_rejoin_regional.get("west"), Some(&2));
    cluster.assert_job_nodes_in("job-constrained", &["east-a", "east-b"], 2)?;

    Ok(())
}
