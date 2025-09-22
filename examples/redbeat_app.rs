#![allow(unused_variables)]

use anyhow::Result;
use celery::beat::{CronSchedule, DeltaSchedule, RedBeatSchedulerBackend};
use celery::task::TaskResult;
use env_logger::Env;
use std::time::Duration;

const QUEUE_NAME: &str = "celery";

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[celery::task]
fn monitor_task() -> TaskResult<String> {
    Ok("Monitor task executed".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    // Create RedBeat scheduler backend
    let redbeat_backend = RedBeatSchedulerBackend::new(redis_url)?;

    // Build a `Beat` with RedBeat scheduler backend (equivalent to Python's redbeat.RedBeatScheduler)
    let mut beat = celery::beat!(
        broker = RedisBroker { std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".into()) },
        scheduler_backend = RedBeatSchedulerBackend { redbeat_backend },
        tasks = [
            "add_numbers" => {
                add,
                schedule = DeltaSchedule::new(Duration::from_secs(10)),
                args = (5, 3),
            },
            "monitor" => {
                monitor_task,
                schedule = CronSchedule::from_string("*/1 * * * *")?,  // Execute every minute
                args = (),
            }
        ],
        task_routes = [
            "*" => QUEUE_NAME,
        ],
    ).await?;

    println!("Starting RedBeat scheduler (equivalent to Python's redbeat.RedBeatScheduler)");
    beat.start().await?;

    Ok(())
}
