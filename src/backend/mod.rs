use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;

use crate::error::{BackendError, TaskError};
use crate::task::ResultValue;

mod redis;
pub use redis::RedisBackend;

/// Task states that mirror the canonical Celery state machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskState {
    Pending,
    Started,
    Retry,
    Success,
    Failure,
}

impl TaskState {
    pub fn is_ready(self) -> bool {
        matches!(self, TaskState::Success | TaskState::Failure)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let as_str = match self {
            TaskState::Pending => "PENDING",
            TaskState::Started => "STARTED",
            TaskState::Retry => "RETRY",
            TaskState::Success => "SUCCESS",
            TaskState::Failure => "FAILURE",
        };
        write!(f, "{}", as_str)
    }
}

/// Metadata persisted in a result backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskMeta {
    pub task_id: String,
    pub status: TaskState,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub traceback: Option<String>,
    #[serde(default)]
    pub children: Vec<Value>,
    #[serde(default)]
    pub date_done: Option<DateTime<Utc>>,
    #[serde(default)]
    pub retries: Option<u32>,
    #[serde(default)]
    pub eta: Option<DateTime<Utc>>,
    #[serde(default)]
    pub meta: Option<Value>,
}

impl TaskMeta {
    pub fn pending(task_id: &str) -> Self {
        Self {
            task_id: task_id.into(),
            status: TaskState::Pending,
            result: None,
            traceback: None,
            children: Vec::new(),
            date_done: None,
            retries: None,
            eta: None,
            meta: None,
        }
    }

    pub fn started(task_id: &str) -> Self {
        Self {
            task_id: task_id.into(),
            status: TaskState::Started,
            result: None,
            traceback: None,
            children: Vec::new(),
            date_done: None,
            retries: None,
            eta: None,
            meta: None,
        }
    }

    pub fn success<R>(task_id: &str, result: &R) -> Result<Self, serde_json::Error>
    where
        R: ResultValue,
    {
        Ok(Self {
            task_id: task_id.into(),
            status: TaskState::Success,
            result: Some(result.to_json_value()?),
            traceback: None,
            children: Vec::new(),
            date_done: Some(Utc::now()),
            retries: None,
            eta: None,
            meta: None,
        })
    }

    pub fn failure(task_id: &str, error: &TaskError) -> Self {
        let (exc_type, exc_message) = match error {
            TaskError::ExpectedError(msg) => ("ExpectedError", msg.clone()),
            TaskError::UnexpectedError(msg) => ("UnexpectedError", msg.clone()),
            TaskError::TimeoutError => ("TimeoutError", "task timed out".into()),
            TaskError::Retry(_) => ("Retry", "task retry requested".into()),
        };

        Self {
            task_id: task_id.into(),
            status: TaskState::Failure,
            result: Some(Value::String(error.to_string())),
            traceback: None,
            children: Vec::new(),
            date_done: Some(Utc::now()),
            retries: None,
            eta: None,
            meta: Some(json!({
                "exc_type": exc_type,
                "exc_message": exc_message,
            })),
        }
    }

    pub fn retry(
        task_id: &str,
        error: &TaskError,
        eta: Option<DateTime<Utc>>,
        retries: u32,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            status: TaskState::Retry,
            result: Some(Value::String(error.to_string())),
            traceback: None,
            children: Vec::new(),
            date_done: Some(Utc::now()),
            retries: Some(retries),
            eta,
            meta: None,
        }
    }
}

#[async_trait]
pub trait ResultBackend: Send + Sync {
    async fn store_task_meta(&self, meta: TaskMeta) -> Result<(), BackendError>;
    async fn get_task_meta(&self, task_id: &str) -> Result<Option<TaskMeta>, BackendError>;
    async fn forget(&self, task_id: &str) -> Result<(), BackendError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TaskError;

    #[test]
    fn success_meta_contains_result() {
        let meta = TaskMeta::success("abc", &42u32).unwrap();
        assert_eq!(meta.status, TaskState::Success);
        assert_eq!(meta.task_id, "abc");
        assert_eq!(meta.result, Some(Value::from(42u32)));
        assert!(meta.date_done.is_some());
    }

    #[test]
    fn failure_meta_has_message() {
        let err = TaskError::ExpectedError("boom".into());
        let meta = TaskMeta::failure("xyz", &err);
        assert_eq!(meta.status, TaskState::Failure);
        assert_eq!(meta.result, Some(Value::String(err.to_string())));
        assert!(meta.meta.is_some());
    }
}
