fn ensure_addr() {
    std::env::set_var("AMQP_ADDR", "amqp://guest:guest@127.0.0.1:5672//");
}

async fn expect_macro_success<F, T>(future: F)
where
    F: std::future::Future<Output = celery::export::BeatResult<T>>,
{
    match future.await {
        Ok(_) => (),
        Err(celery::error::BeatError::BrokerError(_)) => (),
        Err(err) => panic!("unexpected error: {:?}", err),
    }
}

#[tokio::test]
async fn test_basic_use() {
    ensure_addr();
    expect_macro_success(celery::beat!(
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
    expect_macro_success(celery::beat!(
        broker = AMQPBroker { connection_string },
        tasks = [],
        task_routes = []
    ))
    .await;
}

#[tokio::test]
async fn test_basic_use_with_trailing_comma() {
    ensure_addr();
    expect_macro_success(celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
    ))
    .await;
}

#[tokio::test]
async fn test_with_options() {
    ensure_addr();
    expect_macro_success(celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
        default_queue = "celery"
    ))
    .await;
}

#[tokio::test]
async fn test_with_options_and_trailing_comma() {
    ensure_addr();
    expect_macro_success(celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
        default_queue = "celery",
    ))
    .await;
}

#[tokio::test]
async fn test_tasks_and_task_routes_with_trailing_comma() {
    ensure_addr();
    expect_macro_success(celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [,],
        task_routes = [,],
    ))
    .await;
}
