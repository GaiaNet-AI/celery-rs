use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

use crate::backend::{ResultBackend, TaskMeta, TaskState};
use crate::error::BackendError;

/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Clone)]
pub struct AsyncResult {
    pub task_id: String,
    backend: Option<Arc<dyn ResultBackend>>,
    poll_interval: Duration,
}

impl std::fmt::Debug for AsyncResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncResult")
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl AsyncResult {
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.into(),
            backend: None,
            poll_interval: Duration::from_millis(200),
        }
    }

    pub(crate) fn with_backend(task_id: &str, backend: Option<Arc<dyn ResultBackend>>) -> Self {
        Self {
            task_id: task_id.into(),
            backend,
            poll_interval: Duration::from_millis(200),
        }
    }

    /// Returns the task identifier.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns the current backend state for this task.
    pub async fn state(&self) -> Result<TaskState, BackendError> {
        Ok(self.fetch_meta().await?.status)
    }

    /// Returns whether the task finished successfully or failed.
    pub async fn ready(&self) -> Result<bool, BackendError> {
        Ok(self.state().await?.is_ready())
    }

    /// Blocks until the task finishes and returns the result serialized as `T`.
    ///
    /// If `timeout` is provided the method returns [`BackendError::Timeout`] when the
    /// interval elapses.
    pub async fn get<T>(&self, timeout: Option<Duration>) -> Result<T, BackendError>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        loop {
            let meta = self.fetch_meta().await?;
            match meta.status {
                TaskState::Success => {
                    let value = meta.result.unwrap_or_default();
                    return Ok(serde_json::from_value(value)?);
                }
                TaskState::Failure => {
                    let message = meta
                        .meta
                        .as_ref()
                        .and_then(|meta| meta.get("exc_message"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| meta.result.and_then(|r| r.as_str().map(|s| s.to_string())))
                        .unwrap_or_else(|| "task failed".into());
                    return Err(BackendError::TaskFailed(message));
                }
                TaskState::Retry | TaskState::Pending | TaskState::Started => {
                    if let Some(duration) = timeout {
                        if start.elapsed() >= duration {
                            return Err(BackendError::Timeout);
                        }
                    }
                    sleep(self.poll_interval).await;
                }
            }
        }
    }

    async fn fetch_meta(&self) -> Result<TaskMeta, BackendError> {
        let backend = self.backend.as_ref().ok_or(BackendError::NotConfigured)?;
        let task_id = self.task_id.clone();
        let meta = backend.get_task_meta(&task_id).await?;
        Ok(meta.unwrap_or_else(|| TaskMeta::pending(&task_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    use crate::backend::{TaskMeta, TaskState};

    struct MockBackend {
        store: Mutex<HashMap<String, TaskMeta>>,
    }

    impl MockBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                store: Mutex::new(HashMap::new()),
            })
        }
    }

    #[async_trait]
    impl ResultBackend for MockBackend {
        async fn store_task_meta(&self, meta: TaskMeta) -> Result<(), BackendError> {
            self.store.lock().await.insert(meta.task_id.clone(), meta);
            Ok(())
        }

        async fn get_task_meta(&self, task_id: &str) -> Result<Option<TaskMeta>, BackendError> {
            Ok(self.store.lock().await.get(task_id).cloned())
        }

        async fn forget(&self, task_id: &str) -> Result<(), BackendError> {
            self.store.lock().await.remove(task_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn state_defaults_to_pending() {
        let backend = MockBackend::new();
        let result = AsyncResult::with_backend("abc", Some(backend as Arc<_>));
        assert_eq!(result.state().await.unwrap(), TaskState::Pending);
    }

    #[tokio::test]
    async fn ready_reflects_success() {
        let backend = MockBackend::new();
        backend
            .store_task_meta(TaskMeta {
                task_id: "abc".into(),
                status: TaskState::Success,
                result: Some(serde_json::json!(123)),
                traceback: None,
                children: vec![],
                date_done: None,
                retries: None,
                eta: None,
                meta: None,
            })
            .await
            .unwrap();
        let result = AsyncResult::with_backend("abc", Some(backend.clone()));
        assert!(result.ready().await.unwrap());
    }

    #[tokio::test]
    async fn get_waits_until_success() {
        let backend = MockBackend::new();
        let result = AsyncResult::with_backend("abc", Some(backend.clone()));
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            backend
                .store_task_meta(TaskMeta {
                    task_id: "abc".into(),
                    status: TaskState::Success,
                    result: Some(serde_json::json!({"value": 10})),
                    traceback: None,
                    children: vec![],
                    date_done: None,
                    retries: None,
                    eta: None,
                    meta: None,
                })
                .await
                .unwrap();
        });

        let value: serde_json::Value = result.get(Some(Duration::from_secs(1))).await.unwrap();
        assert_eq!(value, serde_json::json!({"value": 10}));
        handle.await.unwrap();
    }
}
