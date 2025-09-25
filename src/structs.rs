use crate::prelude::*;
use tabled::Tabled;

use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string};
use sha2::{Digest, Sha256};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::storage::store::Store;

tonic::include_proto!("legion");

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: Option<u64>,
    pub insecure_port: Option<u64>,
    pub host: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfig {
    pub server: Option<ServerConfig>,
    pub store_path: Option<String>,
    pub worker_config_path: Option<String>,
    pub worker_check_seconds: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfigFull {
    pub port: u64,
    pub insecure_port: u64,
    pub host: String,
}

impl From<ServerConfigFull> for ServerConfig {
    fn from(val: ServerConfigFull) -> Self {
        ServerConfig {
            port: Some(val.port),
            insecure_port: Some(val.insecure_port),
            host: Some(val.host),
        }
    }
}

impl Submission {
    pub fn hash(&self) -> String {
        let content = &self.content;
        let hash = Sha256::digest(content.as_bytes());
        format!("{:x}", hash)
    }
}

impl Eval {
    pub fn handle(&self, store: &Store) -> R<String> {
        match self.kind.as_str() {
            "node_join" => self.handle_node_join(store),
            "node_drop" => self.handle_node_drop(store),
            "job_submit" => self.handle_job_submit(store),
            "job_update" => self.handle_job_update(store),
            "job_stop" => self.handle_job_stop(store),
            other => bail!("Unsupported eval kind: {other}"),
        }
    }

    fn handle_node_join(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.clone().trim().to_owned();

        if node_id.is_empty() {
            bail!("Eval node_join missing node name");
        }

        let last_heartbeat = Self::current_timestamp();
        let nodes = store.get_nodes()?;

        if nodes.iter().any(|node| node.id == node_id) {
            store.mark_node_ready(&node_id, &last_heartbeat)?;
        } else {
            let node = Node {
                id: node_id.clone(),
                status: "ready".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat,
                config_attrs: json!({}),
                fingerprint_attrs: json!({}),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            };

            store.insert_node(node.into())?;
        }

        Ok(format!("Node {node_id} joined"))
    }

    fn handle_node_drop(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.clone().trim().to_owned();

        if node_id.is_empty() {
            bail!("Eval node_drop missing node name");
        }

        store.remove_node(&node_id)?;

        Ok(format!("Node {node_id} dropped"))
    }

    fn handle_job_submit(&self, store: &Store) -> R<String> {
        self.schedule_job_allocations(store)?;
        Ok(format!("Job {} submitted", self.job_id))
    }

    fn handle_job_update(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_update missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;
        self.schedule_job_allocations(store)?;

        Ok(format!("Job {} updated", self.job_id))
    }

    fn handle_job_stop(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_stop missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;

        Ok(format!("Job {} stopped", self.job_id))
    }

    fn schedule_job_allocations(&self, store: &Store) -> R<()> {
        if self.job_config_id.trim().is_empty() {
            bail!("Eval missing job config id");
        }

        let nodes = store.get_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let task_instances = store.get_task_instances_for_job_config(&self.job_config_id)?;
        if task_instances.is_empty() {
            return Ok(());
        }

        let mut rng = thread_rng();

        for (task_id, count) in task_instances {
            for _ in 0..count {
                let node = nodes
                    .choose(&mut rng)
                    .context("No nodes available to place allocations")?;
                // TODO: Add pluggable scheduling logic here
                store.place_alloc(&node.id, task_id)?;
            }
        }

        Ok(())
    }

    fn current_timestamp() -> String {
        OffsetDateTime::now_utc()
            .format(format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
            ))
            .unwrap_or_else(|_| "1970-01-01 00:00:00 +00:00:00".into())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfigFull {
    pub server: ServerConfigFull,
    pub store_path: String,
    pub worker_config_path: String,
    pub worker_check_seconds: u64,
}

impl From<ManagerConfigFull> for ManagerConfig {
    fn from(val: ManagerConfigFull) -> Self {
        ManagerConfig {
            server: Some(val.server.into()),
            store_path: Some(val.store_path),
            worker_config_path: Some(val.worker_config_path),
            worker_check_seconds: Some(val.worker_check_seconds),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfigAlt {
    pub worker_type: String,
    pub additional_node_checks: Option<Vec<String>>,
    pub pool: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub name_prefix: Option<String>,
}

// // TODO: TASK KIND CONFIG SHOULD BE CUSTOMIZABLE
// // AND EVENTUALLY ABLE TO HANDLE DEEP NESTING
// #[derive(Serialize, Deserialize, Debug, Clone, Default)]
// #[serde(rename_all = "camelCase")]
// pub struct TypeConfig {
//     pub message: String,
// }

// // TODO: KIND CONFIG SHOULD BE MORE CUSTOMIZABLE
// #[derive(Serialize, Deserialize, Debug, Clone, Default)]
// #[serde(rename_all = "camelCase")]
// pub struct ResourcesConfig {
//     pub memory: u32,
// }
// #[derive(Serialize, Deserialize, Debug, Clone)]
// #[serde(rename_all = "camelCase")]
// pub struct Job {
//     pub name: String,
// }

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct JobPrint {
    pub name: String,
    pub status: String,
    pub created_at: String,
}

impl From<Job> for JobPrint {
    fn from(job: Job) -> Self {
        JobPrint {
            name: job.name,
            status: job.status,
            created_at: job.created_at.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub name: String,
    pub count: u32,
    pub kind: String,
    pub config: Value,
    pub resources: Value,
    pub constraints: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub id: i64,
    pub status: String,
    pub resources: Value,
    pub node_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LedgerEntry {
    pub node: Node,
    pub allocations: Vec<Allocation>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct LedgerNodePrint {
    pub node_id: String,
    pub status: String,
    pub allocations: String,
}

impl From<&LedgerEntry> for LedgerNodePrint {
    fn from(entry: &LedgerEntry) -> Self {
        let allocations = if entry.allocations.is_empty() {
            "None".to_string()
        } else {
            entry
                .allocations
                .iter()
                .map(|alloc| {
                    format!(
                        "{} (task {}, job {})",
                        alloc.id, alloc.task_config_id, alloc.job_config_id
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        LedgerNodePrint {
            node_id: entry.node.id.clone(),
            status: entry.node.status.clone(),
            allocations,
        }
    }
}

impl From<LedgerEntry> for LedgerNode {
    fn from(entry: LedgerEntry) -> Self {
        let allocations = entry
            .allocations
            .into_iter()
            .map(LedgerAllocation::from)
            .collect();

        LedgerNode {
            node: Some(entry.node.into()),
            allocations,
        }
    }
}

impl LedgerNode {
    pub fn into_entry(self) -> R<LedgerEntry> {
        let node_msg = self.node.context("Ledger node missing node information")?;
        let node: Node = node_msg.into();

        let allocations = self
            .allocations
            .into_iter()
            .map(|allocation| allocation.into_allocation())
            .collect::<R<Vec<_>>>()?;

        Ok(LedgerEntry { node, allocations })
    }
}

impl From<Allocation> for LedgerAllocation {
    fn from(allocation: Allocation) -> Self {
        let Allocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
        } = allocation;

        let resources = to_string(&resources).unwrap_or_else(|_| "{}".into());

        LedgerAllocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
        }
    }
}

impl LedgerAllocation {
    pub fn into_allocation(self) -> R<Allocation> {
        let LedgerAllocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
        } = self;

        let resources: Value =
            serde_json::from_str(&resources).context("failed to parse allocation resources")?;

        Ok(Allocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::store::Store;

    fn setup_store() -> R<Store> {
        let store_config = ManagerConfigFull {
            store_path: "tests/eval".to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn find_node<'a>(ledger: &'a [LedgerEntry], node_id: &str) -> Option<&'a LedgerEntry> {
        ledger.iter().find(|entry| entry.node.id == node_id)
    }

    #[test]
    fn eval_chain_updates_ledger_state() -> R<()> {
        let store = setup_store()?;

        assert!(store.get_ledger()?.is_empty());

        let node_one_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
        };
        node_one_eval.handle(&store)?;

        let mut ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 1);
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert!(node_one_entry.allocations.is_empty());

        let node_two_eval = Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-2".into(),
        };
        node_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 2);

        let drop_eval = Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "node_drop".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-2".into(),
        };
        drop_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 1);
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert!(node_one_entry.allocations.is_empty());

        let job_one_name = "job-1".to_string();
        let job_one_task = TaskConfig {
            name: "task-one".into(),
            kind: "service".into(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
        };
        let job_one_config = JobConfig {
            name: job_one_name.clone(),
            tasks: vec![job_one_task.clone()],
            version: None,
        };
        let job_one_subs = vec![Submission {
            path: "job-one".into(),
            content: "job-one".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_one_config, &job_one_subs)?;
        let job_one_config_id_v1 = store.get_job_config(&job_one_name)?.id();

        let job_submit_eval = Eval {
            id: "4".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v1.clone(),
            node_name: String::new(),
        };
        job_submit_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 1);
        assert_eq!(
            node_one_entry.allocations[0].job_config_id,
            job_one_config_id_v1
        );

        let updated_task = TaskConfig {
            resources: json!({ "memory": 512 }),
            ..job_one_task.clone()
        };
        let job_one_updated_config = JobConfig {
            name: job_one_name.clone(),
            tasks: vec![updated_task],
            version: None,
        };
        store.handle_job_submit(&job_one_updated_config, &job_one_subs)?;
        let job_one_config_id_v2 = store.get_job_config(&job_one_name)?.id();

        let job_update_eval = Eval {
            id: "5".into(),
            status: "pending".into(),
            kind: "job_update".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
        };
        job_update_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 1);
        assert_eq!(
            node_one_entry.allocations[0].job_config_id,
            job_one_config_id_v2
        );

        let job_two_name = "job-2".to_string();
        let job_two_task = TaskConfig {
            name: "task-two".into(),
            kind: "service".into(),
            count: 1,
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({}),
        };
        let job_two_config = JobConfig {
            name: job_two_name.clone(),
            tasks: vec![job_two_task.clone()],
            version: None,
        };
        let job_two_subs = vec![Submission {
            path: "job-two".into(),
            content: "job-two".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_two_config, &job_two_subs)?;
        let job_two_config_id = store.get_job_config(&job_two_name)?.id();

        let job_two_submit_eval = Eval {
            id: "6".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_two_name.clone(),
            job_config_id: job_two_config_id.clone(),
            node_name: String::new(),
        };
        job_two_submit_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 2);

        let stop_job_one_eval = Eval {
            id: "7".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
        };
        stop_job_one_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 1);
        assert!(node_one_entry
            .allocations
            .iter()
            .all(|alloc| alloc.job_config_id == job_two_config_id));

        let stop_job_two_eval = Eval {
            id: "8".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_two_name,
            job_config_id: job_two_config_id,
            node_name: String::new(),
        };
        stop_job_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert!(node_one_entry.allocations.is_empty());

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobConfig {
    pub name: String,
    pub tasks: Vec<TaskConfig>,
    pub version: Option<u32>,
}

impl JobConfig {
    pub fn is_same_as(&self, other: &JobConfig) -> bool {
        self.name == other.name && self.tasks == other.tasks
    }

    pub fn id(&self) -> String {
        match self.version {
            Some(version) => format!("{}-{}", self.name, version),
            None => format!("{}-{}", self.name, 0),
        }
    }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// #[serde(rename_all = "camelCase")]
// pub struct Submission {
//     pub filename: String,
//     pub text: String,
//     pub is_primary: bool,
// }

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinTokenClaims {
    pub exp: usize,
    pub sub: String,
    pub address: String,
    pub server_config: ServerConfig,
    pub worker_type: String,
    pub skip_expiry_validation: bool,
    // pub cert: String
    // pub key: String
    // pub ca_cert: String
}
// aud: String,         // Optional. Audience
// iat: usize,          // Optional. Issued at (as UTC timestamp)
// iss: String,         // Optional. Issuer
// nbf: usize,          // Optional. Not Before (as UTC timestamp)

#[derive(Serialize, Deserialize, Debug, Clone, Tabled)]
pub struct Node {
    pub id: String,
    pub status: String,
    pub pool: String,
    pub availability: String,
    pub last_heartbeat: String,
    pub config_attrs: Value,
    pub fingerprint_attrs: Value,
    pub dynamic_attrs: Value,
    pub health_checks: Value,
}

impl From<Node> for NodeMsg {
    fn from(node: Node) -> Self {
        let config_str = to_string(&node.config_attrs).unwrap_or("{}".to_string());
        let hc_str = to_string(&node.health_checks).unwrap_or("{}".to_string());
        let fp_str = to_string(&node.fingerprint_attrs).unwrap_or("{}".to_string());
        let da_str = to_string(&node.dynamic_attrs).unwrap_or("{}".to_string());

        NodeMsg {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat: node.last_heartbeat,
            config_attrs: config_str,
            fingerprint_attrs: fp_str,
            dynamic_attrs: da_str,
            health_checks: hc_str,
        }
    }
}

impl From<NodeMsg> for Node {
    fn from(node: NodeMsg) -> Self {
        let config_map = from_str::<Value>(&node.config_attrs).unwrap_or(Value::Null);
        let fp_map = from_str::<Value>(&node.fingerprint_attrs).unwrap_or(Value::Null);
        let hc_map = from_str::<Value>(&node.health_checks).unwrap_or(Value::Null);
        let da_map = from_str::<Value>(&node.dynamic_attrs).unwrap_or(Value::Null);

        Node {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat: node.last_heartbeat,
            config_attrs: config_map,
            fingerprint_attrs: fp_map,
            dynamic_attrs: da_map,
            health_checks: hc_map,
        }
    }
}

impl Node {
    pub fn last_heartbeat_too_slow(last_heartbeat: &str) -> R<bool> {
        let fmt = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
        );

        let last_heartbeat = time::OffsetDateTime::parse(last_heartbeat, fmt)?;
        let now = time::OffsetDateTime::now_utc();
        let diff = now - last_heartbeat;
        let too_slow = diff > time::Duration::seconds(DEF_LOST_NODE_AT_SEC);

        Ok(too_slow)
    }

    pub fn print_long(&self) {
        println!("Node ID: {}", self.id);

        let mut attrs = Map::new();
        let default_map = Map::new();
        let ca = self
            .config_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();
        let fa = self
            .fingerprint_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();
        let da = self
            .dynamic_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();

        attrs.extend(ca);
        attrs.extend(fa);
        attrs.extend(da);

        if let Ok(printable) = to_string(&attrs) {
            println!("Attributes: {printable}");
        }
    }

    pub fn print_long_json(&self) {
        if let Ok(json) = to_string(self) {
            println!("{json}");
        };
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct NodePrint {
    pub id: String,
    pub status: String,
    pub pool: String,
    pub availability: String,
    pub last_heartbeat: String,
}

impl From<Node> for NodePrint {
    fn from(node: Node) -> Self {
        NodePrint {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat: node.last_heartbeat,
        }
    }
}

impl ServerConfig {
    pub fn with_defaults(&self) -> ServerConfigFull {
        let cloned = self.clone();

        let config = ServerConfigFull {
            port: self.port.unwrap_or(DEF_MANAGER_SERVER_PORT),
            insecure_port: self.insecure_port.unwrap_or(DEF_MANAGER_INSECURE_PORT),
            host: cloned.host.unwrap_or(DEF_MANAGER_HOSTNAME.to_string()),
        };

        config
    }

    pub fn full_default() -> ServerConfigFull {
        ServerConfigFull {
            port: DEF_MANAGER_SERVER_PORT,
            insecure_port: DEF_MANAGER_INSECURE_PORT,
            host: DEF_MANAGER_HOSTNAME.to_string(),
        }
    }
}

impl ManagerConfig {
    pub fn with_defaults(&self) -> ManagerConfigFull {
        let cloned = self.clone();

        let server_config = match cloned.server {
            Some(config) => config.with_defaults(),
            None => ServerConfig::full_default(),
        };

        let mc = ManagerConfigFull {
            server: server_config,
            store_path: cloned.store_path.unwrap_or(DEF_STORE_PATH.into()),
            worker_config_path: cloned
                .worker_config_path
                .unwrap_or(DEF_WORKER_CONFIG_PATH.into()),
            worker_check_seconds: cloned.worker_check_seconds.unwrap_or(DEF_WORKER_CHECK_SEC),
        };

        mc
    }
}
