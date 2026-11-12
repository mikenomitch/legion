//! Plugin-side panic containment and the export macro for scheduling-index
//! kind plugins (`plugins/indexes/<name>/`, docs/scheduling-indexes.md
//! "Index kinds"). The traits live in `legion-common`
//! ([`crate::scheduling_index::IndexKind`]); the contained impls live here,
//! beside [`Contained`], because a trait impl for a foreign type must sit
//! with the type or the trait. The host-side guard and loader are the
//! manager's (`manager::scheduler::index_kinds`).

use super::containment::{contained, Contained};
use crate::prelude::*;
use crate::resources::ResourcePool;
use crate::scheduling_index::{CellRank, IndexKind, IndexKindFactory, IndexRequest};
use crate::structs::Node;

/// A dylib kind's methods run under its own `catch_unwind`. A panic projects
/// to an empty cell and ranks a cell into the tail, and the host's probe
/// sees it and quarantines the kind.
impl<T: IndexKind> IndexKind for Contained<T> {
    fn name(&self) -> &'static str {
        contained("name", || "panicked", || self.0.name())
    }

    fn project(&self, node: &Node, free: &ResourcePool) -> Vec<u16> {
        contained("project", Vec::new, || self.0.project(node, free))
    }

    fn rank(&self, cell: &[u16], request: &IndexRequest) -> CellRank {
        contained(
            "rank",
            || CellRank {
                fits: false,
                distance: f64::INFINITY,
            },
            || self.0.rank(cell, request),
        )
    }

    fn coverage(&self, request: &IndexRequest) -> usize {
        contained("coverage", || 0, || self.0.coverage(request))
    }

    fn healthy(&self) -> bool {
        contained("healthy", || false, || self.0.healthy())
    }
}

impl<T: IndexKindFactory> IndexKindFactory for Contained<T> {
    fn name(&self) -> &'static str {
        contained("name", || "panicked", || self.0.name())
    }

    /// Every geometry the factory builds is contained too.
    fn build(&self, index_name: &str, options: &Value) -> R<Box<dyn IndexKind>> {
        contained(
            "build",
            || {
                Err(anyhow!(
                    "the index kind panicked building index {index_name}"
                ))
            },
            || {
                self.0
                    .build(index_name, options)
                    .map(|kind| Box::new(Contained(kind)) as Box<dyn IndexKind>)
            },
        )
    }
}

/// Export a scheduling-index kind plugin: the ABI fingerprint, the panic
/// probe, and the contained factory constructor the host loads
/// (`legion_contained_get`).
///
/// ```ignore
/// legion::export_index_kind!(MyKindFactory);
/// ```
#[macro_export]
macro_rules! export_index_kind {
    ($value:expr) => {
        $crate::declare_plugin_abi!();

        #[no_mangle]
        pub extern "C" fn legion_contained_get(
        ) -> Box<dyn $crate::scheduling_index::IndexKindFactory> {
            Box::new($crate::plugin_runtime::containment::Contained($value))
        }
    };
}
