use crate::prelude::*;

use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceRole {
    Primary,
    Standby,
    Candidate,
}

impl PersistenceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            PersistenceRole::Primary => "primary",
            PersistenceRole::Standby => "standby",
            PersistenceRole::Candidate => "candidate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceStatus {
    pub backend: String,
    pub role: PersistenceRole,
    pub lease_held: bool,
}

impl PersistenceStatus {
    pub fn local_disk_primary() -> Self {
        Self {
            backend: "local_disk".to_string(),
            role: PersistenceRole::Primary,
            lease_held: true,
        }
    }
}

pub trait PersistenceDriver: Send + Sync + Debug {
    fn start(&self) -> R<()>;

    fn status(&self) -> PersistenceStatus;
}

#[derive(Debug, Default)]
pub struct LocalDiskPersistenceDriver;

impl LocalDiskPersistenceDriver {
    pub fn new() -> Self {
        Self
    }
}

impl PersistenceDriver for LocalDiskPersistenceDriver {
    fn start(&self) -> R<()> {
        Ok(())
    }

    fn status(&self) -> PersistenceStatus {
        PersistenceStatus::local_disk_primary()
    }
}
