//! These tests should be compiled but not run.

fn ensure_addr() {
    std::env::set_var("AMQP_ADDR", "amqp://guest:guest@127.0.0.1:5672//");
}

async fn expect_macro_success<F, T>(future: F)
where
    F: std::future::Future<Output = celery::export::Result<T>>,
{
    match future.await {
        Ok(_) => (),
        Err(celery::error::CeleryError::BrokerError(_)) => (),
        Err(err) => panic!("unexpected error: {:?}", err),
    }
}

#[tokio::test]
async fn test_basic_use() {
    ensure_addr();
    expect_macro_success(celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = []
    ))
    .await;
}

#[tokio::test]
async fn test_basic_use_with_variable() {
    ensure_addr();
    let connection_string = std::env::var("AMQP_ADDR").unwrap();
    let default_queue = "default";
    expect_macro_success(celery::app!(
        broker = AMQPBroker { connection_string },
        tasks = [],
        task_routes = [],
        default_queue = default_queue,
    ))
    .await;
}

#[tokio::test]
async fn test_basic_use_with_trailing_comma() {
    ensure_addr();
    expect_macro_success(celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
    ))
    .await;
}

#[tokio::test]
async fn test_with_options() {
    ensure_addr();
    expect_macro_success(celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
        task_time_limit = 2
    ))
    .await;
}

#[tokio::test]
async fn test_with_options_and_trailing_comma() {
    ensure_addr();
    expect_macro_success(celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
        task_time_limit = 2,
    ))
    .await;
}
