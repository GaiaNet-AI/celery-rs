use super::{DistributedScheduler, TickDecision};
use crate::beat::schedule::ScheduleDescriptor;
use crate::beat::scheduled_task::ScheduledTask;
use crate::error::BeatError;
use hostname::get as hostname_get;
use log::{info, warn};
use redis::{AsyncCommands, Client, Script};
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const DEFAULT_KEY_PREFIX: &str = "celery_beat";
const LOCK_RENEW_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";
const LOCK_RELEASE_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

fn generate_instance_id(prefix: &str) -> String {
    let host = hostname_get()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "unknown-host".to_string());
    format!("{}:{}:{}", prefix, host, Uuid::new_v4())
}

fn system_time_to_epoch(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

fn epoch_to_system_time(epoch: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(epoch)
}

#[derive(Clone)]
pub struct RedisBackendConfig {
    redis_url: String,
    key_prefix: String,
    lock_timeout: Duration,
    lock_renewal_interval: Duration,
    follower_check_interval: Duration,
    sync_interval: Duration,
    follower_idle_sleep: Duration,
    instance_id: Option<String>,
}

impl RedisBackendConfig {
    pub fn new(redis_url: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            lock_timeout: Duration::from_secs(30),
            lock_renewal_interval: Duration::from_secs(10),
            follower_check_interval: Duration::from_secs(5),
            sync_interval: Duration::from_secs(5),
            follower_idle_sleep: Duration::from_millis(750),
            instance_id: None,
        }
    }

    pub fn key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    pub fn lock_renewal_interval(mut self, interval: Duration) -> Self {
        self.lock_renewal_interval = interval;
        self
    }

    pub fn follower_check_interval(mut self, interval: Duration) -> Self {
        self.follower_check_interval = interval;
        self
    }

    pub fn sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    pub fn follower_idle_sleep(mut self, interval: Duration) -> Self {
        self.follower_idle_sleep = interval;
        self
    }

    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    pub fn resolve(self) -> ResolvedRedisBackendConfig {
        let RedisBackendConfig {
            redis_url,
            key_prefix,
            lock_timeout,
            lock_renewal_interval,
            follower_check_interval,
            sync_interval,
            follower_idle_sleep,
            instance_id,
        } = self;

        let instance_id = instance_id.unwrap_or_else(|| generate_instance_id(&key_prefix));

        ResolvedRedisBackendConfig {
            redis_url,
            key_prefix: key_prefix.clone(),
            lock_key: format!("{}:lock", key_prefix),
            schedule_key: format!("{}:schedule", key_prefix),
            instance_id,
            lock_timeout,
            lock_renewal_interval,
            follower_check_interval,
            sync_interval,
            follower_idle_sleep,
        }
    }
}

#[derive(Clone)]
pub struct ResolvedRedisBackendConfig {
    pub redis_url: String,
    pub key_prefix: String,
    pub lock_key: String,
    pub schedule_key: String,
    pub instance_id: String,
    pub lock_timeout: Duration,
    pub lock_renewal_interval: Duration,
    pub follower_check_interval: Duration,
    pub sync_interval: Duration,
    pub follower_idle_sleep: Duration,
}

impl ResolvedRedisBackendConfig {
    fn task_key(&self, name: &str) -> String {
        format!("{}:task:{}", self.key_prefix, name)
    }

    fn lock_ttl_millis(&self) -> usize {
        self.lock_timeout.as_millis() as usize
    }
}

pub struct RedisSchedulerBackend {
    config: ResolvedRedisBackendConfig,
    client: Client,
    state: BackendState,
}

struct BackendState {
    is_leader: bool,
    last_lock_refresh: Option<Instant>,
    last_leader_attempt: Option<Instant>,
    last_sync: Option<Instant>,
    local_snapshot: HashMap<String, TaskState>,
    pending_full_refresh: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct TaskState {
    descriptor: ScheduleDescriptor,
    next_run_at: SystemTime,
    last_run_at: Option<SystemTime>,
    total_run_count: u32,
}

impl RedisSchedulerBackend {
    pub fn new(config: RedisBackendConfig) -> Result<Self, BeatError> {
        let resolved = config.resolve();
        let client = Client::open(resolved.redis_url.as_str())
            .map_err(|err| BeatError::RedisError(err.to_string()))?;

        Ok(Self {
            config: resolved,
            client,
            state: BackendState {
                is_leader: false,
                last_lock_refresh: None,
                last_leader_attempt: None,
                last_sync: None,
                local_snapshot: HashMap::new(),
                pending_full_refresh: false,
            },
        })
    }

    async fn get_connection(&self) -> Result<redis::aio::MultiplexedConnection, BeatError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| BeatError::RedisError(err.to_string()))
    }

    async fn try_acquire_lock(&mut self) -> Result<bool, BeatError> {
        let mut conn = self.get_connection().await?;
        let result: Option<String> = redis::cmd("SET")
            .arg(&self.config.lock_key)
            .arg(&self.config.instance_id)
            .arg("NX")
            .arg("PX")
            .arg(self.config.lock_ttl_millis())
            .query_async(&mut conn)
            .await
            .map_err(|err| BeatError::RedisError(err.to_string()))?;

        if result.is_some() {
            info!("Redis scheduler backend acquired leadership");
            self.state.last_lock_refresh = Some(Instant::now());
            self.state.is_leader = true;
            self.state.pending_full_refresh = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn renew_lock(&mut self) -> Result<(), BeatError> {
        let mut conn = self.get_connection().await?;
        let script = Script::new(LOCK_RENEW_SCRIPT);
        let result: i32 = script
            .key(&self.config.lock_key)
            .arg(&self.config.instance_id)
            .arg(self.config.lock_ttl_millis())
            .invoke_async(&mut conn)
            .await
            .map_err(|err| BeatError::RedisError(err.to_string()))?;

        if result == 1 {
            self.state.last_lock_refresh = Some(Instant::now());
            Ok(())
        } else {
            Err(BeatError::RedisError("lost leadership".into()))
        }
    }

    async fn release_lock(&mut self) -> Result<(), BeatError> {
        let mut conn = self.get_connection().await?;
        let script = Script::new(LOCK_RELEASE_SCRIPT);
        let _: i32 = script
            .key(&self.config.lock_key)
            .arg(&self.config.instance_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|err| BeatError::RedisError(err.to_string()))?;
        Ok(())
    }

    fn collect_task_state(
        &self,
        scheduled_tasks: &BinaryHeap<ScheduledTask>,
    ) -> (HashMap<String, TaskState>, Vec<String>) {
        let mut map = HashMap::new();
        let mut unsupported = Vec::new();

        for task in scheduled_tasks.iter() {
            let descriptor = match task.schedule.describe() {
                Some(desc) => desc,
                None => {
                    unsupported.push(task.name.clone());
                    continue;
                }
            };

            map.insert(
                task.name.clone(),
                TaskState {
                    descriptor,
                    next_run_at: task.next_call_at,
                    last_run_at: task.last_run_at,
                    total_run_count: task.total_run_count,
                },
            );
        }

        (map, unsupported)
    }

    async fn apply_remote_state(
        &mut self,
        scheduled_tasks: &mut BinaryHeap<ScheduledTask>,
    ) -> Result<(), BeatError> {
        if scheduled_tasks.is_empty() {
            self.state.local_snapshot.clear();
            return Ok(());
        }

        let mut tasks = Vec::with_capacity(scheduled_tasks.len());
        while let Some(task) = scheduled_tasks.pop() {
            tasks.push(task);
        }

        let mut conn = self.get_connection().await?;
        for task in tasks.iter_mut() {
            let key = self.config.task_key(&task.name);
            let data: HashMap<String, String> = conn
                .hgetall(&key)
                .await
                .map_err(|err| BeatError::RedisError(err.to_string()))?;

            if data.is_empty() {
                continue;
            }

            if let Some(value) = data.get("last_run_at") {
                if let Ok(epoch) = value.parse::<u64>() {
                    task.last_run_at = Some(epoch_to_system_time(epoch));
                }
            }
            if let Some(value) = data.get("next_run_at") {
                if let Ok(epoch) = value.parse::<u64>() {
                    task.next_call_at = epoch_to_system_time(epoch);
                }
            }
            if let Some(value) = data.get("total_run_count") {
                if let Ok(count) = value.parse::<u32>() {
                    task.total_run_count = count;
                }
            }
        }

        for task in tasks.into_iter() {
            scheduled_tasks.push(task);
        }

        Ok(())
    }

    async fn write_updates(
        &mut self,
        upserts: &HashMap<String, TaskState>,
        deletes: &[String],
    ) -> Result<(), BeatError> {
        if upserts.is_empty() && deletes.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();

        for (name, state) in upserts {
            let key = self.config.task_key(name);
            let descriptor = serde_json::to_string(&state.descriptor)
                .map_err(|err| BeatError::RedisError(err.to_string()))?;

            pipe.cmd("HSET")
                .arg(&key)
                .arg("descriptor")
                .arg(descriptor)
                .arg("task")
                .arg(name)
                .arg("total_run_count")
                .arg(state.total_run_count)
                .arg("next_run_at")
                .arg(system_time_to_epoch(state.next_run_at));

            if let Some(last_run) = state.last_run_at {
                pipe.cmd("HSET")
                    .arg(&key)
                    .arg("last_run_at")
                    .arg(system_time_to_epoch(last_run));
            }

            pipe.cmd("ZADD")
                .arg(&self.config.schedule_key)
                .arg(system_time_to_epoch(state.next_run_at))
                .arg(&key);
        }

        for name in deletes {
            let key = self.config.task_key(name);
            pipe.cmd("DEL").arg(&key);
            pipe.cmd("ZREM").arg(&self.config.schedule_key).arg(&key);
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|err| BeatError::RedisError(err.to_string()))?;

        Ok(())
    }
}

impl super::SchedulerBackend for RedisSchedulerBackend {
    fn should_sync(&self) -> bool {
        false
    }

    fn sync(&mut self, _scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        Ok(())
    }

    fn as_distributed(&mut self) -> Option<&mut dyn DistributedScheduler> {
        Some(self)
    }
}

impl DistributedScheduler for RedisSchedulerBackend {
    fn before_tick<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<TickDecision, BeatError>> + 'a>> {
        Box::pin(async move {
            let now = Instant::now();
            if self.state.is_leader {
                if self
                    .state
                    .last_lock_refresh
                    .map(|instant| now.duration_since(instant) >= self.config.lock_renewal_interval)
                    .unwrap_or(true)
                {
                    if let Err(err) = self.renew_lock().await {
                        warn!("Redis scheduler backend failed to renew lock: {}", err);
                        self.state.is_leader = false;
                        return Ok(TickDecision::skip(self.config.follower_idle_sleep));
                    }
                }
                Ok(TickDecision::execute())
            } else {
                if self
                    .state
                    .last_leader_attempt
                    .map(|instant| {
                        now.duration_since(instant) >= self.config.follower_check_interval
                    })
                    .unwrap_or(true)
                {
                    self.state.last_leader_attempt = Some(now);
                    if self.try_acquire_lock().await? {
                        return Ok(TickDecision::execute());
                    }
                }
                Ok(TickDecision::skip(self.config.follower_idle_sleep))
            }
        })
    }

    fn after_tick<'a>(
        &'a mut self,
        scheduled_tasks: &'a mut BinaryHeap<ScheduledTask>,
    ) -> Pin<Box<dyn Future<Output = Result<(), BeatError>> + 'a>> {
        Box::pin(async move {
            if !self.state.is_leader {
                return Ok(());
            }

            if self.state.pending_full_refresh {
                self.apply_remote_state(scheduled_tasks).await?;
                self.state.pending_full_refresh = false;
            }

            if self
                .state
                .last_sync
                .map(|instant| instant.elapsed() < self.config.sync_interval)
                .unwrap_or(false)
            {
                return Ok(());
            }

            let (current_state, unsupported) = self.collect_task_state(scheduled_tasks);
            for name in unsupported {
                warn!(
                    "Redis scheduler backend skipping task '{}' (unsupported schedule)",
                    name
                );
            }

            let mut upserts = HashMap::new();
            for (name, state) in current_state.iter() {
                match self.state.local_snapshot.get(name) {
                    Some(existing) if existing == state => {}
                    _ => {
                        upserts.insert(name.clone(), state.clone());
                    }
                }
            }

            let mut deletes = Vec::new();
            for name in self.state.local_snapshot.keys() {
                if !current_state.contains_key(name) {
                    deletes.push(name.clone());
                }
            }

            self.write_updates(&upserts, &deletes).await?;
            self.state.local_snapshot = current_state;
            self.state.last_sync = Some(Instant::now());
            Ok(())
        })
    }

    fn shutdown<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<(), BeatError>> + 'a>> {
        Box::pin(async move {
            if self.state.is_leader {
                if let Err(err) = self.release_lock().await {
                    warn!("Redis scheduler backend failed to release lock: {}", err);
                }
                self.state.is_leader = false;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn resolve_applies_defaults() {
        let config = RedisBackendConfig::new("redis://localhost:6379");
        let resolved = config.resolve();

        assert_eq!(resolved.key_prefix, DEFAULT_KEY_PREFIX);
        assert_eq!(resolved.lock_key, format!("{}:lock", DEFAULT_KEY_PREFIX));
        assert_eq!(
            resolved.schedule_key,
            format!("{}:schedule", DEFAULT_KEY_PREFIX)
        );
        assert_eq!(resolved.lock_timeout, Duration::from_secs(30));
        assert_eq!(resolved.lock_renewal_interval, Duration::from_secs(10));
        assert_eq!(resolved.follower_check_interval, Duration::from_secs(5));
        assert!(resolved.instance_id.starts_with(DEFAULT_KEY_PREFIX));
    }

    #[tokio::test]
    async fn lock_lifecycle_smoke() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        let prefix = format!("test_lock_{}", Uuid::new_v4());
        let config = RedisBackendConfig::new(&url).key_prefix(&prefix);
        let mut backend = match RedisSchedulerBackend::new(config) {
            Ok(backend) => backend,
            Err(err) => {
                eprintln!("Skipping Redis lock test: {err}");
                return;
            }
        };

        match backend.try_acquire_lock().await {
            Ok(true) => {
                backend.renew_lock().await.expect("renew");
                backend.release_lock().await.expect("release");
            }
            Ok(false) => {
                eprintln!("Skipping Redis lock test: lock already held");
            }
            Err(err) => {
                eprintln!("Skipping Redis lock test: {err}");
            }
        }
    }
}
