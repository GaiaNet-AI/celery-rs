/// RedBeat scheduler - Redis-based distributed scheduler
///
/// Implements Leader/Follower pattern with automatic failover
use super::backend::SchedulerBackend;
use super::scheduled_task::ScheduledTask;
use super::Schedule;
use crate::error::BeatError;
use base64::Engine;
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// const DEFAULT_MAX_INTERVAL: u64 = 300; // 5 minutes
const LOCK_TIMEOUT: u64 = 60; // 60 seconds lock timeout, balancing stability and recovery speed
const FOLLOWER_CHECK_INTERVAL: Duration = Duration::from_secs(30); // Follower check interval

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
    pub next_run_time: Option<u64>, // Add next execution time
}

impl RedBeatSchedulerEntry {
    pub fn key(&self, key_prefix: &str) -> String {
        format!("{}:{}", key_prefix, self.name)
    }

    pub fn score(&self) -> f64 {
        // Use next_run_time as score to avoid immediate execution
        match self.next_run_time {
            Some(next_time) => next_time as f64,
            None => {
                // If no next_run_time, calculate next execution time
                self.calculate_next_run_time_from_now() as f64
            }
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
            // Default to 30 seconds
            Duration::from_secs(30)
        }
    }

    pub fn is_due(&self) -> (bool, f64) {
        if !self.enabled {
            return (false, 5.0);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Use next_run_time as primary judgment basis
        let next_run_time = match self.next_run_time {
            Some(time) => time,
            None => {
                // If next_run_time is empty, calculate based on last_run_at
                match self.last_run_at {
                    None => self.calculate_next_run_time_from_now(),
                    Some(last_run) => {
                        let interval = self.parse_schedule_interval();
                        last_run + interval.as_secs()
                    }
                }
            }
        };

        if now >= next_run_time {
            (true, 0.0) // Time to execute
        } else {
            (false, (next_run_time - now) as f64) // Wait remaining time
        }
    }

    /// Calculate next run time from current time (for first execution)
    pub fn calculate_next_run_time_from_now(&self) -> u64 {
        let now = SystemTime::now();

        // For cron expressions, use CronSchedule parsing
        if self.schedule.contains("*") && self.schedule.contains(" ") {
            if let Ok(cron_schedule) = super::schedule::CronSchedule::from_string(&self.schedule) {
                if let Some(next_time) = cron_schedule.next_call_at(Some(now)) {
                    return next_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                }
            }
        }

        // Non-cron expression, use interval calculation
        let interval = self.parse_schedule_interval();
        now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() + interval.as_secs()
    }

    /// Calculate next run time using cron schedule string
    pub fn calculate_next_run_time_from_cron(&self, last_run_at: Option<u64>) -> Option<u64> {
        let base_time = match last_run_at {
            Some(last_run) => UNIX_EPOCH + Duration::from_secs(last_run),
            None => SystemTime::now(),
        };

        // For cron expressions, use CronSchedule parsing
        if self.schedule.contains("*") && self.schedule.contains(" ") {
            if let Ok(cron_schedule) = super::schedule::CronSchedule::from_string(&self.schedule) {
                if let Some(next_time) = cron_schedule.next_call_at(Some(base_time)) {
                    return Some(
                        next_time
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    );
                }
            }
        }

        // Fallback to interval calculation
        let interval = self.parse_schedule_interval();
        Some(
            base_time
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + interval.as_secs(),
        )
    }
}

/// RedBeat scheduler with Leader/Follower pattern
#[derive(Clone)]
pub struct RedBeatScheduler {
    redis_client: Client,
    key_prefix: String,
    schedule_key: String,
    lock_key: String,
    lock_timeout: u64,
    lock_renewal_interval: Duration, // New: lock renewal interval configuration
    instance_id: String,
    is_leader: bool,
    last_leader_check: SystemTime,
    // max_interval: Duration,
    follower_check_interval: Duration,
    tasks_synced_to_redis: bool,
}

impl RedBeatScheduler {
    pub fn new(redis_url: String) -> Result<Self, BeatError> {
        let redis_client = Client::open(redis_url)
            .map_err(|e| BeatError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        let key_prefix = "redbeat".to_string();
        let schedule_key = format!("{}:schedule", key_prefix);
        let lock_key = format!("{}:lock", key_prefix);

        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let instance_id = format!("{}:{}", hostname, std::process::id());

        log::info!("🚀 Initializing RedBeat instance: {}", instance_id);

        Ok(Self {
            redis_client,
            key_prefix,
            schedule_key,
            lock_key,
            lock_timeout: LOCK_TIMEOUT,
            lock_renewal_interval: Duration::from_secs(LOCK_TIMEOUT / 3),
            instance_id,
            is_leader: false,
            last_leader_check: UNIX_EPOCH,
            // max_interval: Duration::from_secs(DEFAULT_MAX_INTERVAL),
            follower_check_interval: FOLLOWER_CHECK_INTERVAL,
            tasks_synced_to_redis: false,
        })
    }

    /// Initialize the scheduler - try to acquire lock on startup
    pub async fn initialize(&mut self) -> Result<(), BeatError> {
        log::info!("🔄 Starting RedBeat initialization...");

        // 尝试获取实例锁
        let acquired = self.try_acquire_lock().await?;

        if acquired {
            log::info!("✅ RedBeat initialization complete - LEADER mode");
        } else {
            log::info!("✅ RedBeat initialization complete - FOLLOWER mode");
        }

        Ok(())
    }

    /// Set the lock renewal interval (how often to renew the leader lock)
    pub fn with_lock_renewal_interval(mut self, interval: Duration) -> Self {
        self.lock_renewal_interval = interval;
        self
    }

    /// Set the follower check interval (how often followers try to acquire leadership)
    pub fn with_follower_check_interval(mut self, interval: Duration) -> Self {
        self.follower_check_interval = interval;
        self
    }

    /// Set the lock timeout (how long the leader lock is valid)
    pub fn with_lock_timeout(mut self, timeout_secs: u64) -> Self {
        self.lock_timeout = timeout_secs;
        self
    }

    /// Try to acquire leader lock
    async fn try_acquire_lock(&mut self) -> Result<bool, BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        // 检查锁是否存在
        let lock_exists: bool = redis::cmd("EXISTS")
            .arg(&self.lock_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to check lock existence: {}", e)))?;

        if lock_exists {
            log::info!(
                "🔒 Lock already exists, becoming FOLLOWER: {}",
                self.instance_id
            );
            return Ok(false);
        }

        // 尝试获取锁
        let result: redis::RedisResult<String> = conn
            .set_options(
                &self.lock_key,
                &self.instance_id,
                redis::SetOptions::default()
                    .conditional_set(redis::ExistenceCheck::NX)
                    .with_expiration(redis::SetExpiry::EX(self.lock_timeout)),
            )
            .await;

        let acquired = result.is_ok();
        if acquired {
            self.is_leader = true;
            log::info!(
                "🎯 Successfully acquired lock, became LEADER: {}",
                self.instance_id
            );

            // 启动锁续期程序
            if let Err(e) = self.start_lock_renewal() {
                log::error!("Failed to start lock renewal: {}", e);
            }
        } else {
            log::info!(
                "❌ Failed to acquire lock, becoming FOLLOWER: {}",
                self.instance_id
            );
        }

        Ok(acquired)
    }

    /// Start background lock renewal task (independent of tick logic)
    pub fn start_lock_renewal(&self) -> Result<(), BeatError> {
        if !self.is_leader {
            return Ok(());
        }

        let redis_client = self.redis_client.clone();
        let lock_key = self.lock_key.clone();
        let instance_id = self.instance_id.clone();
        let lock_timeout = self.lock_timeout;

        // Renew lock at configured interval
        let renewal_interval = self.lock_renewal_interval;

        log::info!(
            "🔒 Starting background lock renewal every {:?}",
            renewal_interval
        );

        // Use std::thread instead of tokio::spawn to avoid runtime issues
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();

            loop {
                std::thread::sleep(renewal_interval);
                log::info!("🔒 Background renewal tick");

                let result = rt.block_on(async {
                    match redis_client.get_multiplexed_async_connection().await {
                        Ok(mut conn) => {
                            let script = r#"
                                if redis.call("GET", KEYS[1]) == ARGV[1] then
                                    return redis.call("EXPIRE", KEYS[1], ARGV[2])
                                else
                                    return 0
                                end
                            "#;

                            redis::Script::new(script)
                                .key(&lock_key)
                                .arg(&instance_id)
                                .arg(lock_timeout)
                                .invoke_async(&mut conn)
                                .await
                        }
                        Err(e) => {
                            log::error!("🔒 Background lock renewal connection error: {}", e);
                            Err(e)
                        }
                    }
                });

                match result {
                    Ok(1i32) => {
                        log::info!("🔒 Lock renewed successfully by background task");
                    }
                    Ok(0i32) => {
                        // 诊断锁失败的具体原因
                        let diagnosis_result = rt.block_on(async {
                            match redis_client.get_multiplexed_async_connection().await {
                                Ok(mut conn) => {
                                    let current_lock: Option<String> = redis::cmd("GET")
                                        .arg(&lock_key)
                                        .query_async(&mut conn)
                                        .await
                                        .unwrap_or(None);

                                    match current_lock {
                                        None => "Lock expired or deleted".to_string(),
                                        Some(owner) if owner != instance_id => {
                                            format!("Lock taken by another instance: {}", owner)
                                        }
                                        Some(_) => "Unexpected renewal failure".to_string(),
                                    }
                                }
                                Err(_) => "Redis connection failed during diagnosis".to_string(),
                            }
                        });

                        log::warn!(
                            "🔒 Background lock renewal failed - lost leadership: {}",
                            diagnosis_result
                        );
                        break; // Exit renewal loop
                    }
                    Ok(other) => {
                        log::warn!("🔒 Background lock renewal unexpected result: {}", other);
                    }
                    Err(e) => {
                        log::error!("🔒 Background lock renewal error: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    async fn get_schedule(&self) -> Result<HashMap<String, RedBeatSchedulerEntry>, BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as f64;

        log::debug!(
            "Querying due tasks with now={}, schedule_key={}",
            now,
            self.schedule_key
        );

        let due_keys: Vec<String> = conn
            .zrangebyscore(&self.schedule_key, 0.0, now)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get due tasks: {}", e)))?;

        log::debug!("Found {} due keys: {:?}", due_keys.len(), due_keys);

        let mut schedule = HashMap::new();
        for key in due_keys {
            match self.load_entry_from_key(&key).await {
                Ok(entry) => {
                    // 避免重复执行：检查任务是否真的到期
                    let (is_due, _) = entry.is_due();
                    if is_due {
                        schedule.insert(entry.name.clone(), entry);
                    }
                }
                Err(e) => {
                    log::warn!("Failed to load entry {}: {}", key, e);
                    let _: redis::RedisResult<i32> = conn.zrem(&self.schedule_key, &key).await;
                }
            }
        }

        Ok(schedule)
    }

    async fn load_entry_from_key(&self, key: &str) -> Result<RedBeatSchedulerEntry, BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let (definition, meta): (Option<String>, Option<String>) = redis::pipe()
            .hget(key, "definition")
            .hget(key, "meta")
            .query_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to load entry: {}", e)))?;

        let definition =
            definition.ok_or_else(|| BeatError::RedisError("Entry not found".to_string()))?;
        let definition: serde_json::Value = serde_json::from_str(&definition)
            .map_err(|e| BeatError::RedisError(format!("Failed to parse definition: {}", e)))?;

        let meta =
            meta.unwrap_or_else(|| r#"{"last_run_at": null, "total_run_count": 0}"#.to_string());
        let meta: serde_json::Value = serde_json::from_str(&meta)
            .map_err(|e| BeatError::RedisError(format!("Failed to parse meta: {}", e)))?;

        Ok(RedBeatSchedulerEntry {
            name: definition["name"].as_str().unwrap_or("").to_string(),
            task: definition["task"].as_str().unwrap_or("").to_string(),
            args: definition["args"].as_array().cloned().unwrap_or_default(),
            kwargs: definition["kwargs"]
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
            options: definition["options"]
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
            schedule: definition["schedule"].as_str().unwrap_or("").to_string(),
            enabled: definition["enabled"].as_bool().unwrap_or(true),
            last_run_at: meta["last_run_at"].as_u64(),
            total_run_count: meta["total_run_count"].as_u64().unwrap_or(0),
            next_run_time: meta["next_run_time"].as_u64(),
        })
    }

    /// Update task metadata after execution
    async fn update_task_meta(&self, entry: &RedBeatSchedulerEntry) -> Result<(), BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let key = entry.key(&self.key_prefix);

        let meta = serde_json::json!({
            "last_run_at": entry.last_run_at,
            "total_run_count": entry.total_run_count,
            "next_run_time": entry.next_run_time,
        });

        let score = entry.score();
        log::debug!(
            "Updating task {} with last_run_at={:?}, next_score={}",
            entry.name,
            entry.last_run_at,
            score
        );

        let _: () = redis::pipe()
            .hset(&key, "meta", meta.to_string())
            .zadd(&self.schedule_key, &key, score)
            .query_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to update task meta: {}", e)))?;

        Ok(())
    }

    /// Main tick method - implements Leader/Follower logic
    pub async fn tick(&mut self) -> Result<Duration, BeatError> {
        let now = SystemTime::now();

        // Check if we need to try acquiring leadership
        let should_check_leadership = now
            .duration_since(self.last_leader_check)
            .unwrap_or(Duration::ZERO)
            >= self.follower_check_interval;

        if should_check_leadership {
            self.last_leader_check = now;

            if !self.is_leader {
                // Follower: Try to acquire lock
                log::debug!("👥 FOLLOWER attempting to acquire lock...");
                let acquired = self.try_acquire_lock().await?;
                if !acquired {
                    log::debug!("👥 FOLLOWER failed to acquire lock, remaining as follower");
                }
            }
        }

        if self.is_leader {
            // Leader tick logic
            self.leader_tick().await
        } else {
            // Follower tick logic
            self.follower_tick().await
        }
    }

    /// Leader tick: query tasks, execute tasks, return next task time
    async fn leader_tick(&mut self) -> Result<Duration, BeatError> {
        log::debug!("👑 LEADER tick: checking tasks...");

        // Get all scheduled tasks from Redis
        let schedule = self.get_schedule().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut next_task_time = None;

        for (name, mut entry) in schedule {
            let (is_due, _) = entry.is_due();

            if is_due {
                log::info!("👑 LEADER executing task: {} ({})", name, entry.task);

                // Execute the task
                if let Err(e) = self.execute_task(&entry).await {
                    log::error!("Failed to execute task {}: {}", name, e);
                    continue;
                }

                // Update task metadata
                entry.last_run_at = Some(now);
                entry.total_run_count += 1;
                // Use cron schedule string to calculate next run time
                entry.next_run_time = entry.calculate_next_run_time_from_cron(Some(now));

                log::info!(
                    "👑 Task {} executed, next_run_time: {:?} ({})",
                    name,
                    entry.next_run_time,
                    entry
                        .next_run_time
                        .map(|t| {
                            chrono::DateTime::from_timestamp(t as i64, 0)
                                .unwrap_or_else(|| chrono::Utc::now())
                                .format("%Y-%m-%d %H:%M:%S UTC")
                                .to_string()
                        })
                        .unwrap_or_else(|| "None".to_string())
                );

                // Save updated metadata
                if let Err(e) = self.update_task_meta(&entry).await {
                    log::error!("Failed to update task metadata for {}: {}", name, e);
                }
            }

            // Track the earliest next task time
            if let Some(task_next_time) = entry.next_run_time {
                match next_task_time {
                    None => next_task_time = Some(task_next_time),
                    Some(current_min) => {
                        if task_next_time < current_min {
                            next_task_time = Some(task_next_time);
                        }
                    }
                }
            }
        }

        // Calculate sleep duration until next task
        match next_task_time {
            Some(next_time) => {
                if next_time > now {
                    let sleep_duration = Duration::from_secs(next_time - now);
                    log::debug!("👑 LEADER next task in {:?}", sleep_duration);
                    Ok(sleep_duration)
                } else {
                    // Task is overdue, check again soon
                    Ok(Duration::from_secs(1))
                }
            }
            None => {
                // No tasks scheduled, check again in follower interval
                Ok(self.follower_check_interval)
            }
        }
    }

    /// Execute a task by sending it to the broker
    async fn execute_task(&self, entry: &RedBeatSchedulerEntry) -> Result<(), BeatError> {
        // Create task message using exact Celery protocol format
        let task_id = uuid::Uuid::new_v4().to_string();
        let delivery_tag = uuid::Uuid::new_v4().to_string();

        // Create task body in the same format as celery-rs
        let task_body = serde_json::json!([
            entry.args,
            entry.kwargs,
            {
                "callbacks": null,
                "errbacks": null,
                "chain": null,
                "chord": null
            }
        ]);

        let message = serde_json::json!({
            // "body": base64::encode(task_body.to_string()),
            "body": base64::engine::general_purpose::STANDARD.encode(task_body.to_string()),
            "content-encoding": "utf-8",
            "content-type": "application/json",
            "headers": {
                "argsrepr": null,
                "eta": null,
                "expires": null,
                "group": null,
                "id": task_id,
                "kwargsrepr": null,
                "lang": null,
                "meth": null,
                "origin": format!("redbeat@{}", self.instance_id),
                "parent_id": null,
                "retries": null,
                "root_id": null,
                "shadow": null,
                "task": entry.task,
                "timelimit": [null, null]
            },
            "properties": {
                "body_encoding": "base64",
                "correlation_id": task_id,
                "delivery_info": {
                    "exchange": "",
                    "routing_key": "celery"
                },
                "delivery_tag": delivery_tag,
                "reply_to": null
            }
        });

        // Send to Redis queue
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let queue_key = "celery"; // Default queue
        let serialized = message.to_string();

        let _: () = conn
            .lpush(queue_key, &serialized)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to send task to queue: {}", e)))?;

        log::info!(
            "📤 Sent task {}[{}] to {} queue",
            entry.task,
            task_id,
            queue_key
        );
        Ok(())
    }

    /// Follower tick: check next task time from Redis for accurate scheduling
    async fn follower_tick(&self) -> Result<Duration, BeatError> {
        log::debug!("👥 FOLLOWER waiting...");

        // Get next task time from Redis to ensure accurate scheduling if we become leader
        match self.get_schedule().await {
            Ok(schedule) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let mut next_task_time = None;

                // Find the earliest next task time
                for (_, entry) in schedule {
                    if let Some(task_next_time) = entry.next_run_time {
                        match next_task_time {
                            None => next_task_time = Some(task_next_time),
                            Some(current_min) => {
                                if task_next_time < current_min {
                                    next_task_time = Some(task_next_time);
                                }
                            }
                        }
                    }
                }

                // Return the minimum of follower check interval and next task time
                match next_task_time {
                    Some(next_time) if next_time > now => {
                        let task_sleep = Duration::from_secs(next_time - now);
                        let follower_sleep = self.follower_check_interval;
                        Ok(std::cmp::min(task_sleep, follower_sleep))
                    }
                    _ => Ok(self.follower_check_interval),
                }
            }
            Err(_) => {
                // If we can't get schedule, fall back to follower check interval
                Ok(self.follower_check_interval)
            }
        }
    }

    /// Save task entry to Redis
    pub async fn save_task_entry(&self, entry: &RedBeatSchedulerEntry) -> Result<(), BeatError> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        let key = entry.key(&self.key_prefix);

        // Check if task already exists
        let exists: bool = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to check task existence: {}", e)))?;

        let definition = serde_json::json!({
            "name": entry.name,
            "task": entry.task,
            "args": entry.args,
            "kwargs": entry.kwargs,
            "options": entry.options,
            "schedule": entry.schedule,
            "enabled": entry.enabled,
        });

        if exists {
            // Task exists: only update definition and schedule, preserve meta
            let score = entry.score();
            let _: () = redis::pipe()
                .hset(&key, "definition", definition.to_string())
                .zadd(&self.schedule_key, &key, score)
                .query_async(&mut conn)
                .await
                .map_err(|e| BeatError::RedisError(format!("Failed to update entry: {}", e)))?;
        } else {
            // New task: correctly initialize meta information
            let next_run_time = entry
                .calculate_next_run_time_from_cron(None)
                .unwrap_or_else(|| entry.calculate_next_run_time_from_now());

            let meta = serde_json::json!({
                "last_run_at": entry.last_run_at,
                "total_run_count": entry.total_run_count,
                "next_run_time": next_run_time,
            });

            let score = next_run_time as f64;

            let _: () = redis::pipe()
                .hset(&key, "definition", definition.to_string())
                .hset(&key, "meta", meta.to_string())
                .zadd(&self.schedule_key, &key, score)
                .query_async(&mut conn)
                .await
                .map_err(|e| BeatError::RedisError(format!("Failed to save entry: {}", e)))?;

            log::info!(
                "Saved new task entry: {} with next_run_time: {}, score: {}",
                key,
                next_run_time,
                score
            );
        }

        Ok(())
    }

    /// Check if this instance is the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Get instance ID
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Release the leader lock (cleanup on shutdown)
    pub async fn release_lock(&mut self) -> Result<(), BeatError> {
        if !self.is_leader {
            return Ok(());
        }

        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get Redis connection: {}", e)))?;

        // Use Lua script to safely release only our own lock
        let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;

        let result: i32 = redis::Script::new(script)
            .key(&self.lock_key)
            .arg(&self.instance_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to release lock: {}", e)))?;

        if result == 1 {
            self.is_leader = false;
            log::info!("🔓 Released leader lock: {}", self.instance_id);
        } else {
            log::warn!("🔒 Failed to release lock - not owned by this instance");
        }

        Ok(())
    }

    /// Sync local scheduled tasks to Redis (called once on startup)
    fn sync_local_tasks_to_redis(
        &self,
        scheduled_tasks: &BinaryHeap<ScheduledTask>,
    ) -> Result<(), BeatError> {
        let mut task_entries = Vec::new();

        for task in scheduled_tasks.iter() {
            // Skip control tasks
            if task.name == "redbeat_control" || task.name == "redbeat_heartbeat" {
                continue;
            }

            // Extract task arguments
            let (args, kwargs) = self.extract_task_args(task);

            // Create RedBeat entry using original schedule object for timing
            let mut entry = RedBeatSchedulerEntry {
                name: task.name.clone(),
                task: task.name.clone(),
                args,
                kwargs,
                options: std::collections::HashMap::new(),
                schedule: self.extract_schedule_from_task(task), // Store as cron string for Redis compatibility
                enabled: true,
                last_run_at: None,
                total_run_count: 0,
                next_run_time: None,
            };

            // Use original schedule object to calculate accurate next execution time
            if let Some(next_time) = task.schedule.next_call_at(Some(SystemTime::now())) {
                entry.next_run_time = Some(
                    next_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );
            } else {
                entry.next_run_time = Some(entry.calculate_next_run_time_from_now());
            }
            task_entries.push(entry);
        }

        // Asynchronously save to Redis
        let redis_client = self.redis_client.clone();
        std::thread::scope(|s| {
            let handle = s.spawn(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    for entry in task_entries {
                        let temp_scheduler = RedBeatScheduler {
                            redis_client: redis_client.clone(),
                            key_prefix: "redbeat".to_string(),
                            schedule_key: "redbeat:schedule".to_string(),
                            lock_key: "redbeat:lock".to_string(),
                            lock_timeout: 60,
                            lock_renewal_interval: Duration::from_secs(20),
                            instance_id: "sync".to_string(),
                            is_leader: false,
                            last_leader_check: UNIX_EPOCH,
                            // max_interval: Duration::from_secs(300),
                            follower_check_interval: Duration::from_secs(30),
                            tasks_synced_to_redis: true,
                        };

                        if let Err(e) = temp_scheduler.save_task_entry(&entry).await {
                            log::error!("Failed to sync task {} to Redis: {}", entry.name, e);
                        } else {
                            log::info!(
                                "📋 Synced task '{}' with schedule '{}', next_run: {}",
                                entry.name,
                                entry.schedule,
                                chrono::DateTime::from_timestamp(
                                    entry.next_run_time.unwrap_or(0) as i64,
                                    0
                                )
                                .map(|dt| dt.format("%H:%M:%S UTC").to_string())
                                .unwrap_or_else(|| "invalid".to_string())
                            );
                        }
                    }
                })
            });
            handle.join().unwrap()
        });

        Ok(())
    }

    /// Extract schedule information from task object
    fn extract_schedule_from_task(&self, task: &ScheduledTask) -> String {
        // Use stored cron expression if available
        if let Some(ref cron_expr) = task.cron_expression {
            log::info!(
                "Task {} using stored cron expression: {}",
                task.name,
                cron_expr
            );
            return cron_expr.clone();
        }

        // Fallback to interval analysis
        let now = SystemTime::now();
        if let Some(first_call) = task.schedule.next_call_at(Some(now)) {
            let after_first = first_call + Duration::from_millis(1);
            if let Some(second_call) = task.schedule.next_call_at(Some(after_first)) {
                if let Ok(interval) = second_call.duration_since(first_call) {
                    let interval_secs = interval.as_secs();

                    log::info!(
                        "Task {} schedule analysis: interval={}s",
                        task.name,
                        interval_secs
                    );

                    if interval_secs > 0 {
                        return match interval_secs {
                            60 => "*/1 * * * *".to_string(),
                            n if n >= 120 && n <= 3540 && n % 60 == 0 => {
                                format!("*/{} * * * *", n / 60)
                            }
                            3600 => "0 * * * *".to_string(),
                            n if n >= 7200 && n <= 82800 && n % 3600 == 0 => {
                                format!("0 */{} * * *", n / 3600)
                            }
                            86400 => "0 0 * * *".to_string(),
                            n if n >= 172800 && n <= 604800 && n % 86400 == 0 => {
                                format!("0 0 */{} * *", n / 86400)
                            }
                            604800 => "0 0 * * 0".to_string(),
                            n if n < 60 => format!("{}s", n),
                            _ => format!("{}s", interval_secs),
                        };
                    }
                }
            }
        }

        // Default fallback
        log::info!("Task {} using default schedule: */1 * * * *", task.name);
        "*/1 * * * *".to_string()
    }

    /// Extract task arguments from task object
    fn extract_task_args(
        &self,
        task: &ScheduledTask,
    ) -> (
        Vec<serde_json::Value>,
        std::collections::HashMap<String, serde_json::Value>,
    ) {
        match task.message_factory.try_create_message() {
            Ok(message) => {
                if let Ok(body_str) = String::from_utf8(message.raw_body) {
                    if let Ok(body_json) = serde_json::from_str::<serde_json::Value>(&body_str) {
                        if let Some(array) = body_json.as_array() {
                            if array.len() >= 2 {
                                let args = array[0].as_array().cloned().unwrap_or_default();
                                let kwargs = array[1]
                                    .as_object()
                                    .map(|obj| {
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                                    })
                                    .unwrap_or_default();
                                return (args, kwargs);
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }

        (vec![], std::collections::HashMap::new())
    }
}

impl SchedulerBackend for RedBeatScheduler {
    fn should_sync(&self) -> bool {
        // Always sync for RedBeat to handle distributed logic, but make sync() lightweight after initialization
        true
    }

    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        // First sync initialization
        if !self.tasks_synced_to_redis {
            log::info!("🔄 Initializing RedBeat...");

            // Save local tasks for later sync
            let local_tasks: Vec<ScheduledTask> = scheduled_tasks.drain().collect();

            // Mark as synced to avoid repeated initialization
            self.tasks_synced_to_redis = true;

            // If there are local tasks, sync them to Redis
            if !local_tasks.is_empty() {
                log::info!("📋 Syncing {} local tasks to Redis", local_tasks.len());

                // Rebuild heap for sync
                let mut temp_heap = BinaryHeap::new();
                for task in local_tasks {
                    temp_heap.push(task);
                }

                // Sync tasks to Redis
                if let Err(e) = self.sync_local_tasks_to_redis(&temp_heap) {
                    log::error!("Failed to sync local tasks to Redis: {}", e);
                } else {
                    log::info!("✅ Successfully synced local tasks to Redis");
                }
            }

            // Set as follower initially, let tick method handle leader election
            self.is_leader = false;
            log::info!("✅ RedBeat initialization complete, leader election will happen in tick()");
        }

        Ok(())
    }
}

// // Control task implementations
// struct RedBeatControlMessageFactory;
// impl crate::protocol::TryCreateMessage for RedBeatControlMessageFactory {
//     fn try_create_message(&self) -> Result<crate::protocol::Message, crate::error::ProtocolError> {
//         // Create a dummy message that will never be executed
//         use crate::protocol::{DeliveryInfo, Message, MessageHeaders, MessageProperties};

//         Ok(Message {
//             headers: MessageHeaders {
//                 id: "redbeat-control".to_string(),
//                 task: "redbeat_control".to_string(),
//                 lang: None,
//                 root_id: None,
//                 parent_id: None,
//                 group: None,
//                 meth: None,
//                 shadow: None,
//                 eta: None,
//                 expires: None,
//                 retries: None,
//                 timelimit: (None, None),
//                 argsrepr: None,
//                 kwargsrepr: None,
//                 origin: Some("redbeat".to_string()),
//             },
//             properties: MessageProperties {
//                 correlation_id: "redbeat-control".to_string(),
//                 content_type: "application/json".to_string(),
//                 content_encoding: "utf-8".to_string(),
//                 reply_to: None,
//                 delivery_info: Some(DeliveryInfo {
//                     exchange: "".to_string(),
//                     routing_key: "".to_string(), // Empty routing key - should not be routed anywhere
//                 }),
//             },
//             raw_body: b"[]".to_vec(), // Empty JSON array
//         })
//     }
// }

// struct RedBeatControlSchedule {
//     next_time: SystemTime,
// }

// impl super::Schedule for RedBeatControlSchedule {
//     fn next_call_at(&self, _last_run_at: Option<SystemTime>) -> Option<SystemTime> {
//         // Return the next sync time
//         Some(self.next_time)
//     }
// }
