/// This module contains the definition of application-provided scheduler backends.
use super::scheduled_task::ScheduledTask;
use crate::error::BeatError;
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod redbeat_config;
pub use redbeat_config::{RedBeatConfig, ResolvedRedBeatConfig};

/// RedBeat scheduler entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedBeatSchedulerEntry {
    pub name: String,
    pub task: String,
    pub args: Vec<serde_json::Value>,
    pub kwargs: HashMap<String, serde_json::Value>,
    pub options: HashMap<String, serde_json::Value>,
    pub schedule: String,
    pub enabled: bool,
    pub last_run_at: Option<u64>,
    pub total_run_count: u64,
    pub next_run_time: Option<u64>,
}

impl RedBeatSchedulerEntry {
    pub fn key(&self, key_prefix: &str) -> String {
        format!("{}:{}", key_prefix, self.name)
    }

    pub fn score(&self) -> f64 {
        match self.next_run_time {
            Some(next_time) => next_time as f64,
            None => self.calculate_next_run_time_from_now() as f64,
        }
    }

    fn parse_schedule_interval(&self) -> Duration {
        if self.schedule.ends_with('s') {
            let seconds: u64 = self.schedule[..self.schedule.len() - 1]
                .parse()
                .unwrap_or(30);
            Duration::from_secs(seconds)
        } else if self.schedule.ends_with('m') {
            let minutes: u64 = self.schedule[..self.schedule.len() - 1]
                .parse()
                .unwrap_or(1);
            Duration::from_secs(minutes * 60)
        } else if self.schedule.ends_with('h') {
            let hours: u64 = self.schedule[..self.schedule.len() - 1]
                .parse()
                .unwrap_or(1);
            Duration::from_secs(hours * 3600)
        } else {
            // Default to treating as seconds
            let seconds: u64 = self.schedule.parse().unwrap_or(30);
            Duration::from_secs(seconds)
        }
    }

    fn calculate_next_run_time_from_now(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let interval = self.parse_schedule_interval().as_secs();
        now + interval
    }
}

/// A `SchedulerBackend` is in charge of keeping track of the internal state of the scheduler
/// according to some source of truth, such as a database.
///
/// The default scheduler backend, [`LocalSchedulerBackend`](struct.LocalSchedulerBackend.html),
/// doesn't do any external synchronization, so the source of truth is just the locally defined
/// schedules.
pub trait SchedulerBackend {
    /// Check whether the internal state of the scheduler should be synchronized.
    /// If this method returns `true`, then `sync` will be called as soon as possible.
    fn should_sync(&self) -> bool;

    /// Synchronize the internal state of the scheduler.
    ///
    /// This method is called in the pauses between scheduled tasks. Synchronization should
    /// be as quick as possible, as it may otherwise delay the execution of due tasks.
    /// If synchronization is slow, it should be done incrementally (i.e., it should span
    /// multiple calls to `sync`).
    ///
    /// This method will not be called if `should_sync` returns `false`.
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError>;
}

/// The default [`SchedulerBackend`](trait.SchedulerBackend.html).
pub struct LocalSchedulerBackend {}

#[allow(clippy::new_without_default)]
impl LocalSchedulerBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl SchedulerBackend for LocalSchedulerBackend {
    fn should_sync(&self) -> bool {
        false
    }

    #[allow(unused_variables)]
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        unimplemented!()
    }
}

/// RedBeat scheduler backend - Redis-based distributed scheduler backend
/// 
/// This implementation follows the original architecture pattern while adding
/// distributed scheduling capabilities through Redis-based coordination.
pub struct RedBeatSchedulerBackend {
    // Configuration (resolved from RedBeatConfig)
    config: ResolvedRedBeatConfig,
    
    // Redis client
    redis_client: Client,
    
    // Derived keys
    lock_key: String,
    
    // Runtime state
    pub is_leader: bool,
    pub last_leader_check: SystemTime,
    pub last_lock_renewal: SystemTime,
    
    // Task management
    pending_tasks: HashMap<String, RedBeatSchedulerEntry>,
}

impl RedBeatSchedulerBackend {
    /// Create a new RedBeat scheduler backend with the given configuration
    /// 
    /// This follows the original architecture pattern of accepting configuration
    /// and resolving it with defaults.
    pub fn new(config: RedBeatConfig) -> Result<Self, BeatError> {
        let resolved_config = config.resolve()
            .map_err(|e| BeatError::RedisError(format!("Configuration error: {}", e)))?;
        
        let redis_client = Client::open(resolved_config.redis_url.as_str())
            .map_err(|e| BeatError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        let lock_key = format!("{}:lock", resolved_config.key_prefix);

        Ok(Self {
            config: resolved_config,
            redis_client,
            lock_key,
            is_leader: false,
            last_leader_check: UNIX_EPOCH,
            last_lock_renewal: UNIX_EPOCH,
            pending_tasks: HashMap::new(),
        })
    }
    
    /// Create a RedBeat scheduler backend with just a Redis URL (convenience method)
    /// 
    /// This provides backward compatibility with the previous API while encouraging
    /// use of the new configuration system.
    pub fn from_redis_url<S: Into<String>>(redis_url: S) -> Result<Self, BeatError> {
        let config = RedBeatConfig::new().redis_url(redis_url);
        Self::new(config)
    }
    
    /// Get the current configuration
    pub fn config(&self) -> &ResolvedRedBeatConfig {
        &self.config
    }

    /// Get instance ID (for backward compatibility)
    pub fn instance_id(&self) -> &str {
        &self.config.instance_id
    }

    /// Get follower check interval (for backward compatibility)
    pub fn follower_check_interval(&self) -> Duration {
        self.config.follower_check_interval
    }

    /// Get lock renewal interval (for backward compatibility)
    pub fn lock_renewal_interval(&self) -> Duration {
        self.config.lock_renewal_interval
    }

    /// Initialize the RedBeat backend and try to acquire leadership
    pub async fn initialize(&mut self) -> Result<(), BeatError> {
        log::info!("🔄 Initializing RedBeat scheduler: {}", self.config.instance_id);
        log::info!("⚙️  Lock timeout: {}s, Renewal interval: {}s, Follower check: {}s", 
                   self.config.lock_timeout, 
                   self.config.lock_renewal_interval.as_secs(),
                   self.config.follower_check_interval.as_secs());
        
        // Try to acquire leadership on startup
        match self.try_acquire_lock().await {
            Ok(acquired) => {
                if acquired {
                    log::info!("🎯 Became LEADER on startup: {}", self.config.instance_id);
                    self.last_lock_renewal = SystemTime::now();
                } else {
                    log::info!("👥 Starting as FOLLOWER: {}", self.config.instance_id);
                }
            }
            Err(e) => {
                log::warn!("Failed to check leadership on startup: {}", e);
            }
        }
        
        Ok(())
    }

    /// Try to acquire the Redis lock
    pub async fn try_acquire_lock(&mut self) -> Result<bool, BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let result: Result<String, redis::RedisError> = redis::cmd("SET")
            .arg(&self.lock_key)
            .arg(&self.config.instance_id)
            .arg("NX")
            .arg("EX")
            .arg(self.config.lock_timeout)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(_) => {
                log::info!("🎯 Acquired leader lock: {}", self.config.instance_id);
                self.is_leader = true;
                self.last_leader_check = SystemTime::now();
                Ok(true)
            }
            Err(_) => {
                self.is_leader = false;
                self.last_leader_check = SystemTime::now();
                Ok(false)
            }
        }
    }

    /// Renew leader lock with optimized Lua script
    pub async fn renew_lock(&mut self) -> Result<(), BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end";

        let result: i32 = redis::Script::new(script)
            .key(&self.lock_key)
            .arg(&self.config.instance_id)
            .arg(self.config.lock_timeout)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Lock renewal failed: {}", e)))?;

        if result != 1 {
            self.is_leader = false;
            return Err(BeatError::RedisError("Lost leadership".to_string()));
        }

        Ok(())
    }

    /// Release the Redis lock
    pub async fn release_lock(&mut self) -> Result<(), BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

        let _: i32 = redis::Script::new(script)
            .key(&self.lock_key)
            .arg(&self.config.instance_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Lock release failed: {}", e)))?;

        self.is_leader = false;
        log::info!("🔓 Released leader lock: {}", self.config.instance_id);
        Ok(())
    }

    /// Add a task to Redis (for dynamic task scheduling)
    pub async fn add_task(
        &mut self,
        name: String,
        entry: RedBeatSchedulerEntry,
    ) -> Result<(), BeatError> {
        if !self.is_leader {
            return Err(BeatError::RedisError(
                "Only leader can add tasks".to_string(),
            ));
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let task_key = entry.key(&self.config.key_prefix);
        let task_data = serde_json::to_string(&entry)
            .map_err(|e| BeatError::RedisError(format!("Task serialization failed: {}", e)))?;

        // Add to sorted set and store task data
        let _: () = conn
            .zadd(&self.config.schedule_key, &task_key, entry.score())
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to add task to schedule: {}", e)))?;

        let _: () = conn
            .set(&task_key, &task_data)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to store task data: {}", e)))?;

        log::info!("📋 Added task to Redis: {}", name);
        Ok(())
    }

    /// Update a task in Redis
    pub async fn update_task(
        &mut self,
        name: String,
        entry: RedBeatSchedulerEntry,
    ) -> Result<(), BeatError> {
        if !self.is_leader {
            return Err(BeatError::RedisError(
                "Only leader can update tasks".to_string(),
            ));
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let task_key = entry.key(&self.config.key_prefix);
        let task_data = serde_json::to_string(&entry)
            .map_err(|e| BeatError::RedisError(format!("Task serialization failed: {}", e)))?;

        // Update sorted set score and task data
        let _: () = conn
            .zadd(&self.config.schedule_key, &task_key, entry.score())
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to update task schedule: {}", e)))?;

        let _: () = conn
            .set(&task_key, &task_data)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to update task data: {}", e)))?;

        log::info!("🔄 Updated task in Redis: {}", name);
        Ok(())
    }

    /// Remove a task from Redis
    pub async fn remove_task(&mut self, name: String) -> Result<(), BeatError> {
        if !self.is_leader {
            return Err(BeatError::RedisError(
                "Only leader can remove tasks".to_string(),
            ));
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let task_key = format!("{}:{}", self.config.key_prefix, name);

        // Remove from sorted set and delete task data
        let _: () = conn
            .zrem(&self.config.schedule_key, &task_key)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to remove task from schedule: {}", e)))?;

        let _: () = conn
            .del(&task_key)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to delete task data: {}", e)))?;

        log::info!("🗑️ Removed task from Redis: {}", name);
        Ok(())
    }

    /// Update task execution status in Redis using Hash
    pub async fn update_task_status(
        &mut self,
        name: String,
        last_run_at: SystemTime,
    ) -> Result<(), BeatError> {
        if !self.is_leader {
            return Ok(()); // Only leader updates status
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let task_key = format!("{}:{}", self.config.key_prefix, name);
        let last_run_timestamp = last_run_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Calculate next run time
        let next_run_time = if let Ok(schedule_str) = conn.hget::<_, _, String>(&task_key, "schedule").await {
            self.calculate_next_run_time_from_schedule(&schedule_str, last_run_at)
        } else {
            last_run_timestamp + 30 // Default 30s fallback
        };

        // Update task status using Hash operations
        let _: () = conn
            .hset_multiple(&task_key, &[
                ("last_run_at", last_run_timestamp.to_string()),
                ("next_run_time", next_run_time.to_string()),
            ])
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to update task status: {}", e)))?;

        // Increment run count atomically
        let new_count: i64 = conn
            .hincr(&task_key, "total_run_count", 1)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to increment run count: {}", e)))?;

        // Update sorted set score with new next run time
        let _: () = conn
            .zadd(&self.config.schedule_key, &task_key, next_run_time as f64)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to update task score: {}", e)))?;

        log::info!("📊 Updated task status in Redis: {} (run #{}, last: {}, next: {})", 
                   name, new_count, last_run_timestamp, next_run_time);

        Ok(())
    }

    /// Calculate next run time from schedule string
    fn calculate_next_run_time_from_schedule(&self, schedule: &str, last_run: SystemTime) -> u64 {
        let last_run_timestamp = last_run.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        
        // Simple interval parsing (e.g., "30s", "5m", "1h")
        if let Some(interval) = self.parse_interval(schedule) {
            return last_run_timestamp + interval;
        }
        
        // TODO: Add cron expression parsing
        // For now, default to 30 seconds
        last_run_timestamp + 30
    }

    /// Parse interval string (e.g., "30s", "5m", "1h")
    fn parse_interval(&self, schedule: &str) -> Option<u64> {
        if schedule.ends_with('s') {
            schedule[..schedule.len()-1].parse::<u64>().ok()
        } else if schedule.ends_with('m') {
            schedule[..schedule.len()-1].parse::<u64>().map(|m| m * 60).ok()
        } else if schedule.ends_with('h') {
            schedule[..schedule.len()-1].parse::<u64>().map(|h| h * 3600).ok()
        } else {
            None
        }
    }

    /// Sync local tasks to Redis using Hash (Leader only)
    pub async fn sync_tasks_to_redis(
        &mut self,
        scheduled_tasks: &BinaryHeap<ScheduledTask>,
    ) -> Result<(), BeatError> {
        if !self.is_leader {
            return Ok(());
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let mut updated_count = 0;

        // Sync local tasks to Redis using Hash
        for task in scheduled_tasks.iter() {
            let task_key = format!("{}:{}", self.config.key_prefix, task.name);
            
            // Check if task exists
            let exists: bool = conn
                .exists(&task_key)
                .await
                .map_err(|e| BeatError::RedisError(format!("Failed to check task existence: {}", e)))?;

            let local_last_run = task.last_run_at.map(|t| {
                t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
            });
            let local_next_run = task.next_call_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            let schedule_str = self.extract_schedule_string(task);

            if exists {
                // Check if local state is newer
                let redis_last_run: Option<u64> = conn
                    .hget(&task_key, "last_run_at")
                    .await
                    .ok()
                    .and_then(|s: String| s.parse().ok());

                let should_update = match (redis_last_run, local_last_run) {
                    (Some(redis_time), Some(local_time)) => local_time > redis_time,
                    (None, Some(_)) => true,
                    _ => false,
                };

                if should_update {
                    // Update existing task with Hash
                    let mut fields = vec![
                        ("schedule", schedule_str),
                        ("enabled", "true".to_string()),
                        ("next_run_time", local_next_run.to_string()),
                        ("total_run_count", task.total_run_count.to_string()),
                    ];

                    if let Some(last_run) = local_last_run {
                        fields.push(("last_run_at", last_run.to_string()));
                    }

                    let _: () = conn
                        .hset_multiple(&task_key, &fields)
                        .await
                        .map_err(|e| BeatError::RedisError(format!("Failed to update task: {}", e)))?;

                    updated_count += 1;
                }
            } else {
                // Create new task with Hash
                let mut fields = vec![
                    ("name", task.name.clone()),
                    ("task", task.name.clone()),
                    ("schedule", schedule_str),
                    ("enabled", "true".to_string()),
                    ("next_run_time", local_next_run.to_string()),
                    ("total_run_count", task.total_run_count.to_string()),
                ];

                if let Some(last_run) = local_last_run {
                    fields.push(("last_run_at", last_run.to_string()));
                }

                let _: () = conn
                    .hset_multiple(&task_key, &fields)
                    .await
                    .map_err(|e| BeatError::RedisError(format!("Failed to create task: {}", e)))?;

                updated_count += 1;
            }

            // Add to sorted set for scheduling
            let _: () = conn
                .zadd(&self.config.schedule_key, &task_key, local_next_run as f64)
                .await
                .map_err(|e| BeatError::RedisError(format!("Failed to add task to schedule: {}", e)))?;
        }

        if updated_count > 0 {
            log::info!("🔄 Synced {} task updates to Redis (Hash)", updated_count);
        }
        Ok(())
    }

    /// Extract schedule string representation
    fn extract_schedule_string(&self, task: &ScheduledTask) -> String {
        // Try to extract schedule information
        // For now, calculate interval from next_call_at and last_run_at
        if let Some(last_run) = task.last_run_at {
            let interval = task.next_call_at.duration_since(last_run).unwrap_or_default();
            format!("{}s", interval.as_secs())
        } else {
            "30s".to_string() // Default fallback
        }
    }
}

impl SchedulerBackend for RedBeatSchedulerBackend {
    fn should_sync(&self) -> bool {
        let now = SystemTime::now();
        
        // Check if we need to perform leadership operations
        let should_check_leadership = now
            .duration_since(self.last_leader_check)
            .unwrap_or(Duration::ZERO)
            >= self.config.follower_check_interval;
            
        let should_renew_lock = self.is_leader && now
            .duration_since(self.last_lock_renewal)
            .unwrap_or(Duration::ZERO)
            >= self.config.lock_renewal_interval;
            
        should_check_leadership || should_renew_lock
    }

    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        if self.is_leader {
            // LEADER: Mark tasks for Redis sync
            log::debug!("👑 LEADER sync: {} local tasks (Redis sync will be handled separately)", scheduled_tasks.len());
            
            // Store task information for later async sync
            self.pending_tasks.clear();
            for task in scheduled_tasks.iter() {
                let entry = RedBeatSchedulerEntry {
                    name: task.name.clone(),
                    task: task.name.clone(),
                    args: vec![],
                    kwargs: std::collections::HashMap::new(),
                    options: std::collections::HashMap::new(),
                    schedule: self.extract_schedule_string(task),
                    enabled: true,
                    last_run_at: task.last_run_at.map(|t| {
                        t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
                    }),
                    total_run_count: task.total_run_count as u64,
                    next_run_time: Some(
                        task.next_call_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
                    ),
                };
                self.pending_tasks.insert(task.name.clone(), entry);
            }
            
        } else {
            // FOLLOWER: Clear local tasks to prevent execution
            if !scheduled_tasks.is_empty() {
                log::debug!("👥 FOLLOWER sync: clearing {} local tasks", scheduled_tasks.len());
                scheduled_tasks.clear();
            }
        }
        
        Ok(())
    }
}

impl RedBeatSchedulerBackend {
    /// Sync pending tasks to Redis using Hash (called after regular sync)
    pub async fn sync_pending_tasks_to_redis(&mut self) -> Result<(), BeatError> {
        if !self.is_leader || self.pending_tasks.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Redis connection failed: {}", e)))?;

        let mut updated_count = 0;

        for (task_name, entry) in &self.pending_tasks {
            let task_key = format!("{}:{}", self.config.key_prefix, task_name);

            // Create Hash fields from entry
            let mut fields = vec![
                ("name", entry.name.clone()),
                ("task", entry.task.clone()),
                ("schedule", entry.schedule.clone()),
                ("enabled", entry.enabled.to_string()),
                ("total_run_count", entry.total_run_count.to_string()),
            ];

            if let Some(last_run) = entry.last_run_at {
                fields.push(("last_run_at", last_run.to_string()));
            }

            if let Some(next_run) = entry.next_run_time {
                fields.push(("next_run_time", next_run.to_string()));
            }

            // Save to Redis as Hash
            let _: () = conn
                .hset_multiple(&task_key, &fields)
                .await
                .map_err(|e| BeatError::RedisError(format!("Failed to save task: {}", e)))?;

            // Add to sorted set for scheduling
            let score = entry.next_run_time.unwrap_or(0) as f64;
            let _: () = conn
                .zadd(&self.config.schedule_key, &task_key, score)
                .await
                .map_err(|e| BeatError::RedisError(format!("Failed to add task to schedule: {}", e)))?;

            updated_count += 1;
        }

        if updated_count > 0 {
            log::info!("🔄 Synced {} pending tasks to Redis (Hash)", updated_count);
        }

        // Clear pending tasks after sync
        self.pending_tasks.clear();
        Ok(())
    }
}
