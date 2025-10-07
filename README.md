<div align="center">
    <br>
    <img src="img/rusty-celery-logo-transparent.png"/>
    <br>
    <br>
    <p>
    A Rust implementation of <a href="https://github.com/celery/celery">Celery</a> for producing and consuming asynchronous tasks with a distributed message queue.
    </p>
    <hr/>
</div>

> **📢 Project Status**: This is a community-maintained fork of the original [rusty-celery](https://github.com/rusty-celery/rusty-celery) project. The original project became inactive, so we've taken over maintenance to ensure continued development and support for the Rust Celery ecosystem.

<p align="center">
    <a href="https://github.com/GaiaNet-AI/celery-rs/actions/workflows/ci.yml">
        <img alt="Build" src="https://github.com/GaiaNet-AI/celery-rs/actions/workflows/ci.yml/badge.svg?branch=main">
    </a>
    <a href="https://github.com/GaiaNet-AI/celery-rs/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/github/license/GaiaNet-AI/celery-rs.svg?color=blue&cachedrop">
    </a>
    <a href="https://crates.io/crates/celery-rs">
        <img alt="Crates" src="https://img.shields.io/crates/v/celery-rs.svg?color=blue">
    </a>
    <a href="https://docs.rs/celery-rs/">
        <img alt="Docs" src="https://img.shields.io/badge/docs.rs-API%20docs-blue">
    </a>
    <a href="https://github.com/GaiaNet-AI/celery-rs/issues?q=is%3Aissue+is%3Aopen+label%3A%22help%20wanted%22">
        <img alt="Help wanted" src="https://img.shields.io/github/issues/GaiaNet-AI/celery-rs/help%20wanted?label=Help%20Wanted">
    </a>
</p>
<br/>


We welcome contributions from everyone regardless of your experience level with Rust. For complete beginners, see [HACKING_QUICKSTART.md](https://github.com/GaiaNet-AI/celery-rs/blob/main/HACKING_QUICKSTART.md).

If you already know the basics of Rust but are new to Celery, check out the [Rusty Celery Book](https://rusty-celery.github.io/) or the original Python [Celery Project](http://www.celeryproject.org/).

## Getting Started

### Quick start

Define tasks by decorating functions with the [`task`](https://docs.rs/celery-rs/*/celery/attr.task.html) attribute.

```rust
use celery::prelude::*;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}
```

Create an app with the [`app`](https://docs.rs/celery-rs/*/celery/macro.app.html) macro
and register your tasks with it:

```rust
let my_app = celery::app!(
    broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add],
    task_routes = [
        "*" => "celery",
    ],
).await?;
```

Then send tasks to a queue with

```rust
my_app.send_task(add::new(1, 2)).await?;
```

And consume tasks as a worker from a queue with

```rust
my_app.consume().await?;
```

### Capturing results

Configure a result backend to persist task state and fetch results from the client side:

```rust
use std::time::Duration;
use celery::backend::RedisBackend;
use celery::prelude::*;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backend = RedisBackend::new("redis://127.0.0.1/0")?
        .with_result_ttl(Duration::from_secs(600));

    let app = celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [add],
        task_routes = [ "*" => "celery" ],
        result_backend = backend,
    ).await?;

    let async_result = app.send_task(add::new(1, 2)).await?;
    println!("state = {}", async_result.state().await?);

    let sum: i32 = async_result.get(Some(Duration::from_secs(10))).await?;
    println!("1 + 2 = {sum}");
    Ok(())
}
```

[`AsyncResult`](https://docs.rs/celery-rs/latest/celery/task/struct.AsyncResult.html) now
exposes idiomatic helpers: `state()` for the latest `TaskState`, `ready()` to check completion,
and `get(timeout)` to await the final value (raising a `BackendError::Timeout` on expiration).

### Example catalog

The [`examples/`](https://github.com/GaiaNet-AI/celery-rs/tree/main/examples) directory contains:

- a simple Celery app implemented in Rust using an AMQP broker ([`examples/celery_app.rs`](https://github.com/GaiaNet-AI/celery-rs/blob/main/examples/celery_app.rs)),
- the same Celery app implemented in Python ([`examples/celery_app.py`](https://github.com/GaiaNet-AI/celery-rs/blob/main/examples/celery_app.py)),
- a Redis result-backend demo showing AsyncResult usage ([`examples/redis_results.rs`](https://github.com/GaiaNet-AI/celery-rs/blob/main/examples/redis_results.rs)),
- a Beat app implemented in Rust ([`examples/beat_app.rs`](https://github.com/GaiaNet-AI/celery-rs/blob/main/examples/beat_app.rs)),
- and a Redis-backed Beat scheduler with leader election ([`examples/redis_beat.rs`](https://github.com/GaiaNet-AI/celery-rs/blob/main/examples/redis_beat.rs)).

## Running the Examples

Explore the demos interactively (preview below):

![](./img/demo.gif)

### Prerequisites

If you already have an AMQP broker running you can set the environment variable `AMQP_ADDR` to your broker's URL (e.g., `amqp://localhost:5672//`, where
the second slash at the end is the name of the [default vhost](https://www.rabbitmq.com/access-control.html#default-state)).
Otherwise simply run the helper script:

```bash
./scripts/brokers/amqp.sh
```

This will download and run the official [RabbitMQ](https://www.rabbitmq.com/) image (RabbitMQ is a popular AMQP broker).

### Run the Rust Celery app

You can consume tasks with:

```bash
cargo run --example celery_app consume
```

And you can produce tasks with:

```bash
cargo run --example celery_app produce [task_name]
```

Current supported tasks for this example are: `add`, `buggy_task`, `long_running_task` and `bound_task`

### Run the Python Celery app

Similarly, you can consume or produce tasks from Python by running


```bash
python examples/celery_app.py consume [task_name]
```

or

```bash
python examples/celery_app.py produce
```

You'll need to have Python 3 installed, along with the requirements listed in the `requirements.txt` file.  You'll also have to provide a task name. This example implements 4 tasks: `add`, `buggy_task`, `long_running_task` and `bound_task`

### Run the Rust Beat app

You can start the Rust beat with:

```bash
cargo run --example beat_app
```

And then you can consume tasks from Rust or Python as explained above.

### Redis-backed Beat failover

A Redis-powered distributed scheduler backend is available through `RedisSchedulerBackend`.
To try it out locally (requires a Redis server running):

```bash
REDIS_URL=redis://127.0.0.1:6379/0 cargo run --example redis_beat
```

Only the instance that holds the Redis lock will dispatch tasks, while followers wait and
take over automatically if the leader disconnects.

To test multi-instance failover:

1. Run a worker connected to Redis so scheduled tasks are consumed (see `examples/celery_app.rs`).
2. Start the first beat instance as shown above; it will log that it acquired leadership.
3. Start a second beat instance with the same command; it stays on standby.
4. Stop the first instance (e.g. Ctrl+C). Within a few seconds the standby will acquire the lock
   and resume scheduling without losing any tasks.

## Road map and current state

✅ = Supported and mostly stable, although there may be a few incomplete features.<br/>
⚠️ = Partially implemented and under active development.<br/>
🔴 = Not supported yet but on-deck to be implemented soon.

> **Note**: Issue tracking links below reference this repository.

| Area      | Component  | Status | Notes / Tracking |
|-----------|------------|:------:|------------------|
| Core      | Protocol   | ⚠️ | [Open issues](https://github.com/GaiaNet-AI/celery-rs/issues?q=is%3Aissue+label%3A%22Protocol%20Feature%22+is%3Aopen) |
| Core      | Producers  | ✅ | |
| Core      | Consumers  | ✅ | |
| Core      | Beat       | ✅ | |
| Brokers   | AMQP       | ✅ | [Open issues](https://github.com/GaiaNet-AI/celery-rs/issues?q=is%3Aissue+label%3A%22Broker%3A%20AMQP%22+is%3Aopen) |
| Brokers   | Redis      | ✅ | [Open issues](https://github.com/GaiaNet-AI/celery-rs/issues?q=is%3Aissue+label%3A%22Broker%3A%20Redis%22+is%3Aopen) |
| Backends  | RPC        | 🔴 | [Open issues](https://github.com/GaiaNet-AI/celery-rs/issues?q=is%3Aissue+label%3A%22Backend%3A%20RPC%22+is%3Aopen) |
| Backends  | Redis      | ✅ | Task results + Beat (0.6.2); [Open issues](https://github.com/GaiaNet-AI/celery-rs/issues?q=is%3Aissue+label%3A%22Backend%3A%20Redis%22+is%3Aopen) |

## Project History and Maintenance

### This is a Community Fork

This project (`celery-rs`) is a community-maintained fork of the original [`rusty-celery`](https://github.com/rusty-celery/rusty-celery) project. We've taken over maintenance due to the original project becoming inactive.

**Key Changes in This Fork:**
- ✅ **Active Maintenance**: Regular updates and bug fixes
- ✅ **Updated Dependencies**: All dependencies kept up-to-date
- ✅ **Improved Stability**: Fixed broker connection issues and test reliability
- ✅ **Modern Rust**: Compatible with latest Rust versions and async ecosystem

### Migration from rusty-celery

If you're migrating from the original `rusty-celery`, the API remains **100% compatible**. Simply update your `Cargo.toml`:

```toml
[dependencies]
# Change from:
# celery = "0.5"

# To:
celery-rs = "0.6"
# Or use git directly:
# celery-rs = { git = "https://github.com/GaiaNet-AI/celery-rs", branch = "main" }
```

### Contributing

We welcome contributions! This fork aims to:
- Maintain API compatibility with the original project
- Provide active maintenance and support
- Keep dependencies updated
- Fix bugs and add features requested by the community

### Acknowledgments

Special thanks to the original [`rusty-celery`](https://github.com/rusty-celery/rusty-celery) team for creating this excellent foundation. This fork builds upon their work while ensuring continued development and support for the Rust Celery ecosystem.
