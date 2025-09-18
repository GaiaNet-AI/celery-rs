/// RedBeat scheduler backend - Redis-based distributed scheduler
/// 
/// This is equivalent to Python's `redbeat.RedBeatScheduler`
use super::backend::SchedulerBackend;
use super::scheduled_task::ScheduledTask;
use super::scheduler::RedBeatLock;
use crate::error::BeatError;
use redis::{AsyncCommands, Client, RedisResult};
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const REDBEAT_LOCK_KEY: &str = "redbeat:lock";
const REDBEAT_SCHEDULE_KEY: &str = "redbeat:schedule";
const LOCK_TIMEOUT: u64 = 300; // 5 minutes

#[derive(Debug, Serialize, Deserialize)]
struct RedBeatEntry {
    name: String,
    task: String,
    schedule: String,
    args: Vec<serde_json::Value>,
    kwargs: HashMap<String, serde_json::Value>,
    enabled: bool,
    last_run_at: Option<u64>,
    total_run_count: u64,
}

/// RedBeat scheduler backend for distributed scheduling
/// 
/// Equivalent to Python's `redbeat.RedBeatScheduler`
#[derive(Clone)]
pub struct RedBeatSchedulerBackend {
    redis_client: Client,
    lock_key: String,
    schedule_key: String,
    lock_timeout: Duration,
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
            lock_key: REDBEAT_LOCK_KEY.to_string(),
            schedule_key: REDBEAT_SCHEDULE_KEY.to_string(),
            lock_timeout: Duration::from_secs(LOCK_TIMEOUT),
            last_sync: UNIX_EPOCH,
            sync_interval: Duration::from_secs(30), // Sync every 30 seconds
        })
    }

    /// Try to acquire the distributed lock
    async fn acquire_lock(&self) -> Result<bool, BeatError> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        
        let lock_value = format!("{}:{}", hostname, std::process::id());
        
        // Use SET with NX and EX options for atomic lock acquisition
        let result: RedisResult<String> = conn.set_options(&self.lock_key, &lock_value, 
            redis::SetOptions::default()
                .conditional_set(redis::ExistenceCheck::NX)
                .with_expiration(redis::SetExpiry::EX(self.lock_timeout.as_secs()))
        ).await;

        Ok(result.is_ok())
    }

    /// Release the distributed lock
    async fn release_lock(&self) -> Result<(), BeatError> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let _: () = conn.del(&self.lock_key).await
            .map_err(|e| BeatError::RedisError(format!("Failed to release lock: {}", e)))?;

        Ok(())
    }

    /// Load schedule from Redis
    async fn load_schedule(&self) -> Result<HashMap<String, RedBeatEntry>, BeatError> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let schedule_data: HashMap<String, String> = conn.hgetall(&self.schedule_key).await
            .map_err(|e| BeatError::RedisError(format!("Failed to load schedule: {}", e)))?;

        let mut schedule = HashMap::new();
        for (key, value) in schedule_data {
            match serde_json::from_str::<RedBeatEntry>(&value) {
                Ok(entry) => {
                    schedule.insert(key, entry);
                }
                Err(e) => {
                    log::warn!("Failed to parse schedule entry {}: {}", key, e);
                }
            }
        }

        Ok(schedule)
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
        let mut conn = self.redis_client.get_multiplexed_async_connection().await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let lock_key = format!("redbeat:task_lock:{}", task_name);
        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let lock_value = format!("{}:{}", hostname, std::process::id());

        // Try to acquire lock with 60 second TTL
        let result: RedisResult<String> = conn.set_options(&lock_key, &lock_value, 
            redis::SetOptions::default()
                .conditional_set(redis::ExistenceCheck::NX)
                .with_expiration(redis::SetExpiry::EX(60))
        ).await;

        Ok(result.is_ok())
    }

    async fn release_task_lock(&self, task_name: &str) -> Result<(), BeatError> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let lock_key = format!("redbeat:task_lock:{}", task_name);
        let _: () = conn.del(&lock_key).await
            .map_err(|e| BeatError::RedisError(format!("Failed to release task lock: {}", e)))?;

        Ok(())
    }
}
