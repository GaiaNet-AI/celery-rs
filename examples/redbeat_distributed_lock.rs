#![allow(unused_variables)]

use anyhow::Result;
use celery::beat::RedBeatSchedulerBackend;
use celery::task::TaskResult;
use env_logger::Env;

const QUEUE_NAME: &str = "celery";

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    // Create RedBeat scheduler backend with distributed locking
    let redbeat_backend = RedBeatSchedulerBackend::new(redis_url)?;

    // Build a `Beat` with RedBeat distributed locking
    // This prevents multiple beat instances from executing the same task
    let mut beat = celery::beat!(
        broker = RedisBroker { std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".into()) },
        scheduler_backend = RedBeatSchedulerBackend { redbeat_backend },
        tasks = [],
        task_routes = [
            "*" => QUEUE_NAME,
        ],
    ).await?;

    println!("Starting RedBeat scheduler with distributed locking");
    println!("Multiple instances can run safely - only one will execute tasks");
    beat.start().await?;

    Ok(())
}
