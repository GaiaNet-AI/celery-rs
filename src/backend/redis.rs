use async_trait::async_trait;
use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use std::time::Duration;

use super::{ResultBackend, TaskMeta};
use crate::error::BackendError;

/// Redis-backed task result store compatible with Celery's default key layout.
pub struct RedisBackend {
    pool: Pool,
    key_prefix: String,
    result_ttl: Option<Duration>,
}

impl RedisBackend {
    pub fn new(redis_url: &str) -> Result<Self, BackendError> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|err| BackendError::PoolCreationError(err.to_string()))?;
        Ok(Self {
            pool,
            key_prefix: "celery-task-meta".into(),
            result_ttl: None,
        })
    }

    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    pub fn with_result_ttl(mut self, ttl: Duration) -> Self {
        self.result_ttl = Some(ttl);
        self
    }

    fn key_for(&self, task_id: &str) -> String {
        format!("{}-{}", self.key_prefix, task_id)
    }
}

#[async_trait]
impl ResultBackend for RedisBackend {
    async fn store_task_meta(&self, meta: TaskMeta) -> Result<(), BackendError> {
        let payload = serde_json::to_string(&meta)?;
        let mut conn = self.pool.get().await?;
        let key = self.key_for(&meta.task_id);

        if let Some(ttl) = self.result_ttl {
            conn.set_ex::<_, _, ()>(key, payload, ttl.as_secs()).await?;
        } else {
            conn.set::<_, _, ()>(key, payload).await?;
        }
        Ok(())
    }

    async fn get_task_meta(&self, task_id: &str) -> Result<Option<TaskMeta>, BackendError> {
        let mut conn = self.pool.get().await?;
        let key = self.key_for(task_id);
        let raw: Option<String> = conn.get(key).await?;
        match raw {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }

    async fn forget(&self, task_id: &str) -> Result<(), BackendError> {
        let mut conn = self.pool.get().await?;
        let key = self.key_for(task_id);
        let _: () = conn.del(key).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{TaskMeta, TaskState};
    use async_trait::async_trait;

    struct InMemoryBackend {
        store: tokio::sync::Mutex<std::collections::HashMap<String, String>>,
    }

    #[async_trait]
    impl ResultBackend for InMemoryBackend {
        async fn store_task_meta(&self, meta: TaskMeta) -> Result<(), BackendError> {
            let key = format!("celery-task-meta-{}", meta.task_id);
            let json = serde_json::to_string(&meta)?;
            self.store.lock().await.insert(key, json);
            Ok(())
        }

        async fn get_task_meta(&self, task_id: &str) -> Result<Option<TaskMeta>, BackendError> {
            let key = format!("celery-task-meta-{}", task_id);
            Ok(self
                .store
                .lock()
                .await
                .get(&key)
                .map(|json| serde_json::from_str(json).unwrap()))
        }

        async fn forget(&self, task_id: &str) -> Result<(), BackendError> {
            let key = format!("celery-task-meta-{}", task_id);
            self.store.lock().await.remove(&key);
            Ok(())
        }
    }

    #[tokio::test]
    async fn mock_backend_roundtrip() {
        let backend = InMemoryBackend {
            store: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        };

        let meta = TaskMeta {
            task_id: "abc".into(),
            status: TaskState::Success,
            result: None,
            traceback: None,
            children: vec![],
            date_done: None,
            retries: None,
            eta: None,
            meta: None,
        };

        backend.store_task_meta(meta.clone()).await.unwrap();
        let stored = backend.get_task_meta("abc").await.unwrap();
        assert_eq!(stored, Some(meta));
        backend.forget("abc").await.unwrap();
        assert_eq!(backend.get_task_meta("abc").await.unwrap(), None);
    }
}
