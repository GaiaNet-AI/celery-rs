use celery::beat::RedBeatScheduler;
use celery::prelude::*;
use std::time::Duration;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    println!("add({}, {}) = {}", x, y, x + y);
    Ok(x + y)
}

/// Method 1: Define tasks and schedules directly in the beat macro's tasks
async fn start_with_tasks_macro() -> anyhow::Result<()> {
    println!("🚀 Method 1: Using beat macro tasks definition for scheduling");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let redbeat_scheduler = RedBeatScheduler::new(redis_url.clone())?
        .with_follower_check_interval(Duration::from_secs(10))
        .with_lock_timeout(60);

    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatScheduler { redbeat_scheduler },
        tasks = [
            "add" => {
                add,
                args = (1, 2),
                schedule = "*/2 * * * *"
            },
        ],
        task_routes = ["*" => "celery"],
    )
    .await?;

    println!("📋 Task scheduled in macro: add(1, 2) every 2 minutes");
    println!("✅ Beat service starting...");
    beat.start().await?;
    Ok(())
}

/// Method 2: Use schedule_named_task_cron to dynamically add tasks
async fn start_with_dynamic_scheduling() -> anyhow::Result<()> {
    println!("🚀 Method 2: Using schedule_named_task_cron to dynamically add tasks");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let redbeat_scheduler = RedBeatScheduler::new(redis_url.clone())?
        .with_follower_check_interval(Duration::from_secs(10))
        .with_lock_timeout(60);

    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatScheduler { redbeat_scheduler },
        tasks = [], // Empty tasks, will add dynamically later
        task_routes = ["*" => "celery"],
    )
    .await?;

    // Dynamically add tasks
    let signature = add::new(1, 2).with_queue("celery");
    beat.schedule_named_task_cron("add".to_string(), signature, "*/2 * * * *");
    println!("📋 Task scheduled dynamically: add(1, 2) every 2 minutes");

    println!("✅ Beat service starting...");
    beat.start().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let method = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    println!("🔗 Redis URL: {}", redis_url);
    println!("📅 Schedule: Every 2 minutes (*/2 * * * *)");
    println!("🔄 Press Ctrl+C to stop\n");

    match method {
        "macro" => start_with_tasks_macro().await,
        "dynamic" => start_with_dynamic_scheduling().await,
        _ => {
            println!("Usage: cargo run --example celery_redbeat [macro|dynamic]");
            println!("  macro   - Method 1: Define scheduling in beat macro tasks");
            println!("  dynamic - Method 2: Use schedule_named_task_cron to add dynamically");
            println!("\nExamples:");
            println!("  cargo run --example celery_redbeat macro");
            println!("  cargo run --example celery_redbeat dynamic");
            Ok(())
        }
    }
}
