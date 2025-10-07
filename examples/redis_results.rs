use anyhow::Result;
use celery::prelude::*;
use env_logger::Env;
use structopt::StructOpt;
use tokio::time::Duration;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "redis_results",
    about = "Demo: Redis result backend for celery-rs",
    setting = structopt::clap::AppSettings::ColoredHelp,
)]
enum ModeOpt {
    #[structopt(about = "Start a worker that consumes from the Redis broker")]
    Consume,
    #[structopt(about = "Send an add task and wait for the result")]
    Produce,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let mode = ModeOpt::from_args();

    let broker_url =
        std::env::var("REDIS_ADDR").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let backend_url =
        std::env::var("REDIS_RESULT_ADDR").unwrap_or_else(|_| "redis://127.0.0.1:6379/1".into());

    let app = celery::app!(
        broker = RedisBroker { broker_url },
        tasks = [add],
        task_routes = ["*" => "celery"],
        result_backend = RedisBackend::new(&backend_url)
            .expect("valid Redis result backend")
            .with_result_ttl(Duration::from_secs(600)),
    )
    .await?;

    match mode {
        ModeOpt::Consume => {
            app.display_pretty().await;
            app.consume().await?;
        }
        ModeOpt::Produce => {
            let handle = app.send_task(add::new(2, 40)).await?;
            println!("Dispatched task {}", handle.task_id());

            let sum: i32 = handle.get(Some(Duration::from_secs(10))).await?;
            println!("2 + 40 = {sum}");
        }
    }

    app.close().await?;
    Ok(())
}
