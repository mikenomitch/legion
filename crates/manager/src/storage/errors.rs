use std::fmt;

use rusqlite::{ffi, Error as RusqliteError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    NotFound {
        entity: &'static str,
        id: String,
    },
    Conflict {
        message: String,
    },
    ForeignKey {
        message: String,
    },
    InvalidInput {
        message: String,
    },
    /// The request is well-formed but the system is not in a state that
    /// allows it: a running job cannot be destroyed, a stopped instance
    /// cannot be restored, a key is disabled. `message` is the full
    /// operator-facing sentence; the RPC layer maps it to
    /// `FAILED_PRECONDITION` and passes it through unchanged.
    FailedPrecondition {
        message: String,
    },
    /// A compare-and-set write lost to a concurrent change and nothing was
    /// written: the caller reads again and retries (`ABORTED`). The app
    /// reconciler's submit after the app record moved under it.
    Aborted {
        message: String,
    },
    DatabaseBusy,
    DatabaseLocked,
    DatabaseReadOnly,
    DatabaseCorrupt,
    DatabaseIo {
        message: String,
    },
    Sqlite {
        message: String,
    },
    /// A KV value (or a write) needs data key `key_id`, which this manager
    /// cannot unwrap: the root key is missing or wrong here. `message` is
    /// the full operator-facing explanation (docs/kv-encryption.md).
    /// Retryable: it clears the moment the root key is placed.
    KeyringLocked {
        key_id: String,
        message: String,
    },
    /// A stored KV ciphertext failed authentication: modified on disk,
    /// transplanted from another key path, or decrypted with the wrong key.
    KvCiphertextInvalid {
        key: String,
        message: String,
    },
}

impl StorageError {
    pub fn not_found(entity: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            entity,
            id: id.into(),
        }
    }

    /// See [`StorageError::FailedPrecondition`].
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::FailedPrecondition {
            message: message.into(),
        }
    }

    pub fn from_anyhow(error: &anyhow::Error) -> Option<Self> {
        if let Some(storage) = error.downcast_ref::<StorageError>() {
            return Some(storage.clone());
        }

        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<RusqliteError>())
            .map(Self::from_rusqlite)
    }

    pub fn from_rusqlite(error: &RusqliteError) -> Self {
        match error {
            RusqliteError::QueryReturnedNoRows => Self::NotFound {
                entity: "record",
                id: "unknown".to_string(),
            },
            RusqliteError::SqliteFailure(sql_error, message) => {
                let msg = message.as_deref().unwrap_or("sqlite error").to_string();
                match sql_error.code {
                    ffi::ErrorCode::ConstraintViolation => match sql_error.extended_code {
                        ffi::SQLITE_CONSTRAINT_UNIQUE | ffi::SQLITE_CONSTRAINT_PRIMARYKEY => {
                            Self::Conflict { message: msg }
                        }
                        ffi::SQLITE_CONSTRAINT_FOREIGNKEY => Self::ForeignKey { message: msg },
                        ffi::SQLITE_CONSTRAINT_CHECK | ffi::SQLITE_CONSTRAINT_NOTNULL => {
                            Self::InvalidInput { message: msg }
                        }
                        _ => Self::InvalidInput { message: msg },
                    },
                    ffi::ErrorCode::DatabaseBusy => Self::DatabaseBusy,
                    ffi::ErrorCode::DatabaseLocked => Self::DatabaseLocked,
                    ffi::ErrorCode::ReadOnly => Self::DatabaseReadOnly,
                    ffi::ErrorCode::DatabaseCorrupt | ffi::ErrorCode::NotADatabase => {
                        Self::DatabaseCorrupt
                    }
                    ffi::ErrorCode::SystemIoFailure | ffi::ErrorCode::CannotOpen => {
                        Self::DatabaseIo { message: msg }
                    }
                    _ => Self::Sqlite { message: msg },
                }
            }
            RusqliteError::FromSqlConversionFailure(_, _, err)
            | RusqliteError::ToSqlConversionFailure(err) => Self::InvalidInput {
                message: err.to_string(),
            },
            _ => Self::Sqlite {
                message: error.to_string(),
            },
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::NotFound { entity, id } => {
                write!(f, "{entity} not found (id: {id})")
            }
            StorageError::Conflict { message } => write!(f, "conflict: {message}"),
            StorageError::ForeignKey { message } => {
                write!(f, "foreign key constraint failed: {message}")
            }
            StorageError::InvalidInput { message } => write!(f, "invalid data: {message}"),
            StorageError::FailedPrecondition { message } => write!(f, "{message}"),
            StorageError::Aborted { message } => write!(f, "{message}"),
            StorageError::DatabaseBusy => write!(f, "storage is busy; retry the request"),
            StorageError::DatabaseLocked => write!(f, "storage is locked; retry the request"),
            StorageError::DatabaseReadOnly => write!(f, "storage is read-only"),
            StorageError::DatabaseCorrupt => write!(f, "storage is corrupt"),
            StorageError::DatabaseIo { message } => write!(f, "storage I/O error: {message}"),
            StorageError::Sqlite { message } => write!(f, "storage error: {message}"),
            StorageError::KeyringLocked { message, .. } => write!(f, "{message}"),
            StorageError::KvCiphertextInvalid { key, message } => {
                write!(
                    f,
                    "kv value at `{key}` failed authentication ({message}); the stored ciphertext \
                     was modified, moved from another key, or written under a different root key"
                )
            }
        }
    }
}

impl std::error::Error for StorageError {}
