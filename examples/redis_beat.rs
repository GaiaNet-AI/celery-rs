use anyhow::Result;
use celery::beat::{CronSchedule, RedisBackendConfig, RedisSchedulerBackend};
use celery::prelude::*;
use std::time::Duration;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    let scheduler_backend = RedisSchedulerBackend::new(
        RedisBackendConfig::new(&redis_url).key_prefix("celery_rs_example"),
    )?;

    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        scheduler_backend = RedisSchedulerBackend { scheduler_backend },
        tasks = [],
        task_routes = ["*" => "celery"],
    )
    .await?;

    let signature = add::new(1, 2).with_queue("celery");
    beat.schedule_named_task(
        "add_every_minute".to_string(),
        signature,
        CronSchedule::from_string("*/1 * * * *")?,
    );

    let fast_signature = add::new(3, 4).with_queue("celery");
    beat.schedule_named_task(
        "add_interval".to_string(),
        fast_signature,
        celery::beat::DeltaSchedule::new(Duration::from_secs(30)),
    );

    beat.start().await?;
    Ok(())
}
