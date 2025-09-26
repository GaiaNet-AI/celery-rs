use celery::beat::RedBeatScheduler;
use celery::prelude::*;
use std::time::Duration;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    println!("add({}, {}) = {}", x, y, x + y);
    Ok(x + y)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    println!("🚀 Starting RedBeat Distributed Scheduler");
    println!("🔗 Redis URL: {}", redis_url);
    println!("📅 Schedule: Every 2 minutes (*/2 * * * *)");
    println!("🔄 Press Ctrl+C to stop\n");

    // Create RedBeat scheduler
    let redbeat_scheduler = RedBeatScheduler::new(redis_url.clone())?
        .with_follower_check_interval(Duration::from_secs(10))
        .with_lock_timeout(60);

    // Use beat macro with unified cron syntax
    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatScheduler { redbeat_scheduler },
        tasks = [
            "add" => {
                add,
                args = (10, 20),
                schedule = "*/10 * * * *"
            },
        ],
        task_routes = [
            "*" => "celery",
        ],
    )
    .await?;

    println!("✅ Beat service starting with RedBeat distributed scheduler...");
    beat.start().await?;

    Ok(())
}
