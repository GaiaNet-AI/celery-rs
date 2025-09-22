/// RedBeat scheduler backend - Redis-based distributed scheduler
///
/// This is equivalent to Python's `redbeat.RedBeatScheduler`
use super::backend::SchedulerBackend;
use super::scheduled_task::ScheduledTask;
use super::scheduler::RedBeatLock;
use crate::error::BeatError;
use redis::{AsyncCommands, Client, RedisResult};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TASK_LOCK_TIMEOUT: u64 = 30; // 30 seconds for individual task locks

/// RedBeat scheduler backend for distributed scheduling
///
/// Equivalent to Python's `redbeat.RedBeatScheduler`
#[derive(Clone)]
pub struct RedBeatSchedulerBackend {
    redis_client: Client,
    last_sync: SystemTime,
    sync_interval: Duration,
}

impl RedBeatSchedulerBackend {
    /// Create a new RedBeat scheduler backend
    pub fn new(redis_url: String) -> Result<Self, BeatError> {
        let redis_client = Client::open(redis_url.clone())
            .map_err(|e| BeatError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        Ok(Self {
            redis_client,
            last_sync: UNIX_EPOCH,
            sync_interval: Duration::from_secs(30), // Sync every 30 seconds
        })
    }
}

impl SchedulerBackend for RedBeatSchedulerBackend {
    fn should_sync(&self) -> bool {
        // Sync every 30 seconds or if we haven't synced yet
        self.last_sync.elapsed().unwrap_or(Duration::from_secs(0)) >= self.sync_interval
    }

    fn sync(&mut self, _scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        // 简化实现，避免在同步方法中使用异步代码
        // 实际的分布式锁逻辑在任务执行时进行
        log::debug!("RedBeat sync called - distributed locking handled at task level");

        // 更新同步时间
        self.last_sync = SystemTime::now();
        Ok(())
    }

    fn as_redbeat_lock(&self) -> Option<Box<dyn super::scheduler::RedBeatLock>> {
        Some(Box::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl RedBeatLock for RedBeatSchedulerBackend {
    async fn try_acquire_task_lock(&self, task_name: &str) -> Result<bool, BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let lock_key = format!("redbeat:task_lock:{}", task_name);
        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let lock_value = format!("{}:{}", hostname, std::process::id());

        // Try to acquire lock with configurable TTL
        let result: RedisResult<String> = conn
            .set_options(
                &lock_key,
                &lock_value,
                redis::SetOptions::default()
                    .conditional_set(redis::ExistenceCheck::NX)
                    .with_expiration(redis::SetExpiry::EX(TASK_LOCK_TIMEOUT)),
            )
            .await;

        Ok(result.is_ok())
    }

    async fn release_task_lock(&self, task_name: &str) -> Result<(), BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let lock_key = format!("redbeat:task_lock:{}", task_name);
        let _: () = conn
            .del(&lock_key)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to release task lock: {}", e)))?;

        Ok(())
    }
}
