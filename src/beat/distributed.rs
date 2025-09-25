/// Distributed scheduler implementation based on RedBeat design
/// 
/// This module provides multi-instance distributed scheduling capabilities
/// using Redis for coordination and locking.

use super::{scheduled_task::ScheduledTask, Schedule, SchedulerBackend};
use crate::error::BeatError;
use redis::{AsyncCommands, Client, RedisResult};
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SCHEDULER_LOCK_KEY: &str = "redbeat:scheduler_lock";
const SCHEDULE_KEY: &str = "redbeat:schedule";
const LOCK_TTL: u64 = 10; // 10 seconds for faster failover
const HEARTBEAT_INTERVAL: u64 = 10; // seconds

/// Distributed scheduler entry stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedScheduleEntry {
    pub name: String,
    pub task: String,
    pub schedule: String,
    pub enabled: bool,
    pub last_run_at: Option<u64>,
    pub total_run_count: u64,
    pub queue: String,
}

/// Distributed scheduler backend using Redis for coordination
#[derive(Clone)]
pub struct DistributedSchedulerBackend {
    redis_client: Client,
    instance_id: String,
    hostname: String,
    last_sync: SystemTime,
    sync_interval: Duration,
    is_leader: bool,
}

impl DistributedSchedulerBackend {
    /// Create a new distributed scheduler backend
    pub fn new(redis_url: String) -> Result<Self, BeatError> {
        let redis_client = Client::open(redis_url)
            .map_err(|e| BeatError::RedisError(format!("Failed to create Redis client: {}", e)))?;

        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        
        let instance_id = format!("{}:{}", hostname, Uuid::new_v4());

        Ok(Self {
            redis_client,
            instance_id,
            hostname,
            last_sync: UNIX_EPOCH,
            sync_interval: Duration::from_secs(5),
            is_leader: false,
        })
    }

    /// Try to acquire the scheduler leader lock
    async fn try_acquire_leader_lock(&mut self) -> Result<bool, BeatError> {
        log::debug!("🔐 Attempting to acquire leader lock...");
        
        let mut conn = self.get_connection().await?;
        
        log::debug!("🔗 Redis connection established");
        
        let result: RedisResult<String> = conn
            .set_options(
                SCHEDULER_LOCK_KEY,
                &self.instance_id,
                redis::SetOptions::default()
                    .conditional_set(redis::ExistenceCheck::NX)
                    .with_expiration(redis::SetExpiry::EX(LOCK_TTL)),
            )
            .await;

        let acquired = result.is_ok();
        self.is_leader = acquired;
        
        if acquired {
            log::info!("👑 Acquired scheduler leader lock: {}", self.instance_id);
        } else {
            log::info!("👥 Failed to acquire leader lock (another instance is leader)");
            // Check who has the lock
            let current_holder: Result<String, _> = conn.get(SCHEDULER_LOCK_KEY).await;
            if let Ok(holder) = current_holder {
                log::debug!("🔒 Current lock holder: {}", holder);
            }
        }
        
        Ok(acquired)
    }

    /// Renew the leader lock if we own it
    async fn renew_leader_lock(&self) -> Result<bool, BeatError> {
        if !self.is_leader {
            return Ok(false);
        }

        let mut conn = self.get_connection().await?;
        
        // Use Lua script to atomically check and renew the lock
        let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("EXPIRE", KEYS[1], ARGV[2])
            else
                return 0
            end
        "#;

        let result: i32 = redis::Script::new(script)
            .key(SCHEDULER_LOCK_KEY)
            .arg(&self.instance_id)
            .arg(LOCK_TTL)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to renew leader lock: {}", e)))?;

        Ok(result == 1)
    }

    /// Get Redis connection
    async fn get_connection(&self) -> Result<redis::aio::MultiplexedConnection, BeatError> {
        self.redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                log::error!("Failed to get Redis connection: {}", e);
                BeatError::RedisError(format!("Failed to get Redis connection: {}", e))
            })
    }

    /// Sync schedule from Redis
    async fn sync_schedule_from_redis(&self) -> Result<HashMap<String, DistributedScheduleEntry>, BeatError> {
        let mut conn = self.get_connection().await?;
        
        let entries: HashMap<String, String> = conn
            .hgetall(SCHEDULE_KEY)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to get schedule from Redis: {}", e)))?;

        let mut schedule = HashMap::new();
        for (name, data) in entries {
            match serde_json::from_str::<DistributedScheduleEntry>(&data) {
                Ok(entry) => {
                    schedule.insert(name, entry);
                }
                Err(e) => {
                    log::warn!("Failed to deserialize schedule entry {}: {}", name, e);
                }
            }
        }

        Ok(schedule)
    }

    /// Update schedule entry in Redis
    async fn update_schedule_entry(&self, entry: &DistributedScheduleEntry) -> Result<(), BeatError> {
        let mut conn = self.get_connection().await?;
        
        let data = serde_json::to_string(entry)
            .map_err(|e| BeatError::RedisError(format!("Failed to serialize schedule entry: {}", e)))?;

        let _: () = conn.hset(SCHEDULE_KEY, &entry.name, data)
            .await
            .map_err(|e| BeatError::RedisError(format!("Failed to update schedule entry: {}", e)))?;

        Ok(())
    }

    /// Check if this instance is the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Update leader state (called by heartbeat task)
    pub fn set_leader_state(&mut self, is_leader: bool) {
        if is_leader != self.is_leader {
            self.is_leader = is_leader;
            if is_leader {
                log::info!("👑 Became leader: {}", self.instance_id);
            } else {
                log::info!("👥 Lost leadership: {}", self.instance_id);
            }
        }
    }

    /// Start heartbeat task to maintain leader lock and handle election
    pub async fn start_heartbeat(&self) -> tokio::task::JoinHandle<()> {
        let redis_client = self.redis_client.clone();
        let instance_id = self.instance_id.clone();
        let mut current_leader_state = self.is_leader;
        
        tokio::spawn(async move {
            loop {
                let mut conn = match redis_client.get_multiplexed_async_connection().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        log::error!("Heartbeat: Failed to connect to Redis: {}", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Check if we currently hold the lock
                let current_holder: Result<String, _> = conn.get(SCHEDULER_LOCK_KEY).await;
                
                let is_leader = if let Ok(holder) = current_holder {
                    if holder == instance_id {
                        // We are the leader, renew the lock
                        let _: RedisResult<String> = conn
                            .set_ex(SCHEDULER_LOCK_KEY, &instance_id, LOCK_TTL)
                            .await;
                        log::debug!("👑 Renewed leader lock: {}", instance_id);
                        true
                    } else {
                        log::debug!("👥 Another instance is leader: {}", holder);
                        false
                    }
                } else {
                    // No lock exists, try to acquire it
                    let result: RedisResult<String> = conn
                        .set_options(
                            SCHEDULER_LOCK_KEY,
                            &instance_id,
                            redis::SetOptions::default()
                                .conditional_set(redis::ExistenceCheck::NX)
                                .with_expiration(redis::SetExpiry::EX(LOCK_TTL)),
                        )
                        .await;
                    
                    if result.is_ok() {
                        log::info!("👑 Acquired leader lock in heartbeat: {}", instance_id);
                        true
                    } else {
                        false
                    }
                };
                
                // Log state changes
                if is_leader != current_leader_state {
                    current_leader_state = is_leader;
                    if is_leader {
                        log::info!("👑 Heartbeat: Became leader: {}", instance_id);
                    } else {
                        log::info!("👥 Heartbeat: Lost leadership: {}", instance_id);
                    }
                }

                // Sleep before next heartbeat (every 3 seconds)
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        })
    }
}

impl SchedulerBackend for DistributedSchedulerBackend {
    fn should_sync(&self) -> bool {
        self.last_sync.elapsed().unwrap_or(Duration::from_secs(0)) >= self.sync_interval
    }

    fn sync(&mut self, _scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        // Don't use async calls in sync - rely on heartbeat to update state
        self.last_sync = SystemTime::now();
        Ok(())
    }
}

impl DistributedSchedulerBackend {
    /// Check if this instance is currently the leader by examining Redis
    fn check_leader_status(&self) -> bool {
        // Try to acquire lock - if successful, we're the leader
        let rt = tokio::runtime::Handle::try_current();
        if let Ok(rt) = rt {
            rt.block_on(async {
                let mut conn = self.redis_client.get_multiplexed_async_connection().await.ok()?;
                
                // First check who holds the lock
                let current_holder: Result<String, _> = conn.get(SCHEDULER_LOCK_KEY).await;
                if let Ok(holder) = current_holder {
                    return Some(holder == self.instance_id);
                }
                
                // If no lock exists, try to acquire it
                let result: RedisResult<String> = conn
                    .set_options(
                        SCHEDULER_LOCK_KEY,
                        &self.instance_id,
                        redis::SetOptions::default()
                            .conditional_set(redis::ExistenceCheck::NX)
                            .with_expiration(redis::SetExpiry::EX(LOCK_TTL)),
                    )
                    .await;
                
                Some(result.is_ok())
            }).unwrap_or(false)
        } else {
            false
        }
    }
}

/// Distributed scheduler coordinator
pub struct DistributedSchedulerCoordinator {
    backend: DistributedSchedulerBackend,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DistributedSchedulerCoordinator {
    /// Create a new distributed scheduler coordinator
    pub fn new(redis_url: String) -> Result<Self, BeatError> {
        let backend = DistributedSchedulerBackend::new(redis_url)?;
        
        Ok(Self {
            backend,
            heartbeat_handle: None,
        })
    }

    /// Start the coordinator
    pub async fn start(&mut self) -> Result<(), BeatError> {
        log::info!("🚀 Starting distributed scheduler coordinator...");
        
        // Try to acquire leader lock
        let acquired = self.backend.try_acquire_leader_lock().await?;
        if acquired {
            log::info!("👑 Became leader: {}", self.backend.instance_id);
        } else {
            log::info!("👥 Started as follower: {}", self.backend.instance_id);
        }
        
        // Start heartbeat task
        let heartbeat_handle = self.backend.start_heartbeat().await;
        self.heartbeat_handle = Some(heartbeat_handle);
        
        log::info!("💓 Heartbeat started for: {}", self.backend.instance_id);
        Ok(())
    }

    /// Stop the coordinator
    pub async fn stop(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        
        log::info!("Distributed scheduler coordinator stopped: {}", self.backend.instance_id);
    }

    /// Get the backend for use with Beat
    pub fn get_backend(&mut self) -> DistributedSchedulerBackend {
        // Update backend with current leader state
        self.backend.is_leader = self.backend.is_leader;
        self.backend.clone()
    }
    
    /// Update backend leader state
    pub fn update_backend_state(&mut self, is_leader: bool) {
        self.backend.set_leader_state(is_leader);
    }

    /// Check if this instance is the leader
    pub fn is_leader(&self) -> bool {
        self.backend.is_leader
    }

    /// Get instance ID
    pub fn instance_id(&self) -> &str {
        &self.backend.instance_id
    }
}

impl Drop for DistributedSchedulerCoordinator {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
    }
}
