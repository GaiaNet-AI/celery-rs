use celery::beat::RedBeatScheduler;
use celery::prelude::*;
use std::time::Duration;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    println!("add({}, {}) = {}", x, y, x + y);
    Ok(x + y)
}

/// 方法1：在 beat 宏的 tasks 中直接定义任务和调度
async fn start_with_tasks_macro() -> anyhow::Result<()> {
    println!("🚀 方法1：使用 beat 宏的 tasks 定义调度");

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

/// 方法2：使用 schedule_named_task_cron 动态添加任务
async fn start_with_dynamic_scheduling() -> anyhow::Result<()> {
    println!("🚀 方法2：使用 schedule_named_task_cron 动态添加任务");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let redbeat_scheduler = RedBeatScheduler::new(redis_url.clone())?
        .with_follower_check_interval(Duration::from_secs(10))
        .with_lock_timeout(60);

    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedBeatScheduler { redbeat_scheduler },
        tasks = [], // 空的 tasks，稍后动态添加
        task_routes = ["*" => "celery"],
    )
    .await?;

    // 动态添加任务
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
            println!("  macro   - 方法1：在 beat 宏的 tasks 中定义调度");
            println!("  dynamic - 方法2：使用 schedule_named_task_cron 动态添加");
            println!("\nExamples:");
            println!("  cargo run --example celery_redbeat macro");
            println!("  cargo run --example celery_redbeat dynamic");
            Ok(())
        }
    }
}
