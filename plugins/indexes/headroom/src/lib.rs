//! `headroom`: an example scheduling-index kind (docs/scheduling-indexes.md,
//! "Index kinds").
//!
//! Projects every node to one cell: the share of one resource it has free,
//! in percent (`options.resource`, default `memory`). Ranks the cells that
//! can hold the request's amount of that resource emptiest first, and every
//! other cell after them. Over a fleet of mixed node sizes this orders
//! candidates by relative load, which the built-in grid (absolute free
//! capacity, log-bucketed) cannot express.
//!
//! ```nickel
//! schedulerSettings = {
//!   indexes = {
//!     emptiest = { kind = "headroom", partition_by = ["pool"], options = { resource = "cpu" } },
//!   },
//! }
//! ```

use legion::prelude::*;
use legion::resources::{ResourcePool, ResourceValue};
use legion::scheduling_index::{CellRank, IndexKind, IndexKindFactory, IndexRequest};
use legion::structs::Node;

pub struct Headroom;

impl IndexKindFactory for Headroom {
    fn name(&self) -> &'static str {
        "headroom"
    }

    fn build(&self, index_name: &str, options: &Value) -> R<Box<dyn IndexKind>> {
        let resource = match options.get("resource") {
            None | Some(Value::Null) => "memory".to_string(),
            Some(Value::String(resource))
                if matches!(
                    resource.as_str(),
                    "memory" | "cpu" | "cores" | "allocations"
                ) =>
            {
                resource.clone()
            }
            Some(other) => bail!(
                "index {index_name}: options.resource must be memory, cpu, cores, or allocations \
                 (the resources a node's capacity records), got {other}"
            ),
        };
        Ok(Box::new(HeadroomKind { resource }))
    }
}

struct HeadroomKind {
    resource: String,
}

impl HeadroomKind {
    fn free(&self, pool: &ResourcePool) -> f64 {
        match pool.values.get(&self.resource) {
            Some(ResourceValue::Amount(amount)) => *amount,
            _ => 0.0,
        }
    }

    fn total(&self, pool: &ResourcePool) -> f64 {
        match self.resource.as_str() {
            "memory" => pool.capacity.memory,
            "cpu" => pool.capacity.cpu,
            "cores" => pool.capacity.cores,
            _ => pool.capacity.allocations,
        }
    }
}

impl IndexKind for HeadroomKind {
    fn name(&self) -> &'static str {
        "headroom"
    }

    /// `[percent free, log2 of the free amount]`: the first orders, the
    /// second lets `rank` tell whether the request fits at all.
    fn project(&self, _node: &Node, free: &ResourcePool) -> Vec<u16> {
        let available = self.free(free);
        let total = self.total(free);
        let percent = if total.is_finite() && total > 0.0 {
            (available / total * 100.0).clamp(0.0, 100.0)
        } else if available > 0.0 {
            100.0
        } else {
            0.0
        };
        let magnitude = if available.is_infinite() {
            u16::MAX
        } else if available > 0.0 {
            (available + 1.0).log2().floor() as u16
        } else {
            0
        };
        vec![percent.round() as u16, magnitude]
    }

    fn rank(&self, cell: &[u16], request: &IndexRequest) -> CellRank {
        let wanted = request.amounts.get(&self.resource).copied().unwrap_or(0.0);
        let wanted_magnitude = if wanted > 0.0 {
            (wanted + 1.0).log2().floor() as u16
        } else {
            0
        };
        let (percent, magnitude) = (
            cell.first().copied().unwrap_or(0),
            cell.get(1).copied().unwrap_or(0),
        );
        CellRank {
            fits: magnitude >= wanted_magnitude,
            // Emptiest first.
            distance: -f64::from(percent),
        }
    }

    fn coverage(&self, request: &IndexRequest) -> usize {
        usize::from(
            request
                .amounts
                .get(&self.resource)
                .is_some_and(|amount| *amount > 0.0),
        )
    }
}

// Exports the ABI fingerprint, the panic probe, and the contained factory: a
// panic in any method quarantines the kind, and its indexes stop serving
// (planning scans instead).
legion::export_index_kind!(Headroom);
