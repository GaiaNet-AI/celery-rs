use anyhow::Result;
use celery::beat::{CronSchedule, DeltaSchedule, RedBeatConfig, RedBeatSchedulerBackend};
use celery::prelude::*;
use std::time::Duration;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

async fn macro_example() -> Result<()> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    // Use new configuration API
    let config = RedBeatConfig::new()
        .redis_url(&redis_url)
        .lock_timeout(12)
        .follower_check_interval(Duration::from_secs(3));

    let redbeat_scheduler = RedBeatSchedulerBackend::new(config)?;

    // Create beat without macro to use CronSchedule/DeltaSchedule objects
    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatSchedulerBackend { redbeat_scheduler },
        tasks = [],  // Empty tasks, will add manually
        task_routes = ["*" => "celery"],
    )
    .await?;

    // Add task using DeltaSchedule object
    let signature = add::new(1, 2).with_queue("celery");
    beat.schedule_named_task(
        "add".to_string(),
        signature,
        DeltaSchedule::new(Duration::from_secs(120)), // Every 2 minutes
    );

    println!("📋 Task scheduled in macro: add_macro(1, 2) every 2 minutes using DeltaSchedule");
    println!("✅ Beat service starting...");
    beat.start().await?;
    Ok(())
}

async fn dynamic_example() -> Result<()> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    // Use new configuration API
    let config = RedBeatConfig::new()
        .redis_url(&redis_url)
        .lock_timeout(60)
        .follower_check_interval(Duration::from_secs(10));

    let redbeat_scheduler = RedBeatSchedulerBackend::new(config)?;

    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatSchedulerBackend { redbeat_scheduler },
        tasks = [], // Empty tasks, will add dynamically later
        task_routes = ["*" => "celery"],
    )
    .await?;

    // Dynamically add tasks using CronSchedule object
    let signature = add::new(1, 2).with_queue("celery");
    beat.schedule_named_task(
        "add".to_string(),
        signature,
        CronSchedule::from_string("*/5 * * * *")?,
    );

    // Alternative: Using DeltaSchedule for interval-based scheduling
    // let signature2 = add::new(3, 4).with_queue("celery");
    // beat.schedule_named_task(
    //     "add_delta".to_string(),
    //     signature2,
    //     DeltaSchedule::new(Duration::from_secs(30)),
    // );

    println!("📋 Task scheduled dynamically: add_cron(1, 2) every 1 minute using CronSchedule");
    println!("📋 Task scheduled dynamically: add_delta(3, 4) every 30 seconds using DeltaSchedule");
    println!("✅ Beat service starting...");

    beat.start().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    println!("🔗 Redis URL: {}", redis_url);
    println!("🔄 Press Ctrl+C to stop\n");

    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "macro" {
        println!("🚀 Method 1: Using macro syntax for task scheduling");
        macro_example().await
    } else {
        println!("🚀 Method 2: Using schedule_named_task to dynamically add tasks");
        dynamic_example().await
    }
}
