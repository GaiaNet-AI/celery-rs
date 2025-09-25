use anyhow::Result;
use celery::beat::{CronSchedule, RedBeatScheduler};
use celery::task::TaskResult;
use env_logger::Env;
use std::time::Duration;

// Define tasks - these will be automatically synced to RedBeat
#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    println!("🧮 Executing add({}, {}) = {}", x, y, x + y);
    Ok(x + y)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    // Configurable check interval (read from environment variable, default 30s)
    let follower_check_interval = std::env::var("REDBEAT_FOLLOWER_CHECK_INTERVAL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(10));

    // Configurable lock timeout (read from environment variable, default 60s)
    let lock_timeout = std::env::var("REDBEAT_LOCK_TIMEOUT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);

    println!("🚀 Starting RedBeat Distributed Scheduler");
    println!("🔗 Redis URL: {}", redis_url);
    println!(
        "🆔 Instance: {}",
        hostname::get().unwrap_or_default().to_string_lossy()
    );
    println!("⏱️  Follower check interval: {:?}", follower_check_interval);
    println!("🔒 Lock timeout: {}s", lock_timeout);
    println!("🔄 Lock renewal interval: 10s"); // 显示自定义续期间隔
    println!("💡 Multiple instances can run safely - only one will execute tasks");
    println!("🔄 Press Ctrl+C to stop\n");

    // Create RedBeat scheduler - configure check interval, lock timeout, and renewal interval
    let redbeat_scheduler = RedBeatScheduler::new(redis_url.clone())?
        .with_follower_check_interval(follower_check_interval)
        .with_lock_timeout(lock_timeout)
        .with_lock_renewal_interval(Duration::from_secs(10)); // 自定义续期间隔：10秒

    // Use traditional celery::beat! macro with tasks - RedBeat will automatically sync them to Redis
    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatScheduler { redbeat_scheduler },
        tasks = [
            // Tasks will be automatically converted to RedBeat entries
            "add" => {
                add,
                schedule = CronSchedule::from_string("*/1 * * * *")?, // 每2分钟执行一次
                args = (10, 20)
            },
        ],
        task_routes = [
            "*" => "celery",
        ],
    )
    .await?;

    println!("✅ Beat service starting with RedBeat distributed scheduler...");
    println!("📋 Local tasks will be automatically synced to Redis on startup");

    // Start Beat service - includes automatic signal handling and lock cleanup
    // Local tasks will be automatically synced to RedBeat on first sync
    beat.start().await?;

    Ok(())
}
