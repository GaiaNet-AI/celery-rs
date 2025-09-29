# Celery-RS

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

## Quick Start

### Define Tasks

Define tasks by decorating functions with the [`task`](https://docs.rs/celery-rs/*/celery/attr.task.html) attribute:

```rust
use celery::prelude::*;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}
```

### Create App and Send Tasks

Create an app with the [`app`](https://docs.rs/celery-rs/*/celery/macro.app.html) macro and register your tasks:

```rust
let my_app = celery::app!(
    broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add],
    task_routes = [
        "*" => "celery",
    ],
).await?;
```

Send tasks to a queue:

```rust
my_app.send_task(add::new(1, 2)).await?;
```

### Consume Tasks as Worker

Consume tasks as a worker from a queue:

```rust
my_app.consume().await?;
```

## Components

### Workers

Workers consume and execute tasks from message queues. They can be configured with different brokers and task routing rules.

**Example**: [`examples/celery_worker.rs`](examples/celery_worker.rs)

```bash
# Start a worker
cargo run --example celery_worker consume

# Send tasks to the worker
cargo run --example celery_worker produce add
```

### Beat Scheduler

Beat is a scheduler that sends tasks to workers at regular intervals. It supports both local and distributed scheduling.

#### Local Beat

Simple single-instance scheduler:

```rust
let mut beat = celery::beat!(
    broker = AMQPBroker { broker_url },
    tasks = [
        ("add-every-30s", add::new(1, 2), DeltaSchedule::new(Duration::from_secs(30))),
    ],
    task_routes = [
        "*" => "celery",
    ],
).await?;

beat.start().await?;
```

#### RedBeat - Distributed Scheduler

For production environments with multiple Beat instances, use RedBeat for distributed scheduling with automatic leader election:

**Example**: [`examples/celery_redbeat.rs`](examples/celery_redbeat.rs)

##### Manual Task Configuration

```rust
use celery::beat::{RedBeatScheduler, RedBeatSchedulerEntry};

// Create RedBeat scheduler with Redis coordination
let redbeat_scheduler = RedBeatScheduler::new(redis_url)?
    .with_follower_check_interval(Duration::from_secs(30))
    .with_lock_timeout(60);

// Configure tasks manually
let tasks = vec![
    RedBeatSchedulerEntry {
        name: "add_task".to_string(),
        task: "add".to_string(),
        args: vec![serde_json::json!(1), serde_json::json!(2)],
        kwargs: HashMap::new(),
        options: HashMap::new(),
        schedule: "30s".to_string(),
        enabled: true,
        last_run_at: None,
        total_run_count: 0,
    },
];

// Save tasks and start scheduler
for task in &tasks {
    redbeat_scheduler.save_task_entry(task).await?;
}

let mut beat = celery::beat!(
    broker = RedisBroker { redis_url },
    scheduler_backend = RedBeatScheduler { redbeat_scheduler },
    tasks = [],
    task_routes = ["*" => "celery"],
).await?;

beat.start().await?;
```

##### Traditional Tasks Mode

**Example**: [`examples/celery_redbeat_traditional.rs`](examples/celery_redbeat_traditional.rs)

You can also use the traditional `tasks` syntax - RedBeat will automatically sync them to Redis:

```rust
use celery::beat::{DeltaSchedule, RedBeatScheduler};

// Create RedBeat scheduler
let redbeat_scheduler = RedBeatScheduler::new(redis_url)?;

// Use traditional tasks syntax - automatically synced to Redis
let mut beat = celery::beat!(
    broker = RedisBroker { redis_url },
    scheduler_backend = RedBeatScheduler { redbeat_scheduler },
    tasks = [
        "add_every_15s" => {
            add,
            schedule = DeltaSchedule::new(Duration::from_secs(15)),
            args = (10, 20)
        },
        "multiply_every_30s" => {
            multiply,
            schedule = DeltaSchedule::new(Duration::from_secs(30)),
            args = (3, 4)
        },
    ],
    task_routes = ["*" => "celery"],
).await?;

beat.start().await?;
```

**RedBeat Features:**
- ✅ **Distributed Scheduling**: Multiple Beat instances with automatic leader election
- ✅ **Redis Coordination**: Uses Redis for locks and task persistence
- ✅ **Automatic Failover**: Seamless failover when leader instance fails
- ✅ **Signal Handling**: Automatic lock cleanup on Ctrl+C
- ✅ **Configurable**: Customizable check intervals and timeouts

```bash
# Start multiple RedBeat instances (only one will be active)
REDIS_URL=redis://localhost:6379/0 cargo run --example celery_redbeat
```

## Brokers

### AMQP (RabbitMQ)

```rust
let app = celery::app!(
    broker = AMQPBroker { "amqp://localhost:5672//" },
    tasks = [add],
    task_routes = ["*" => "celery"],
).await?;
```

### Redis

```rust
let app = celery::app!(
    broker = RedisBroker { "redis://localhost:6379/0" },
    tasks = [add],
    task_routes = ["*" => "celery"],
).await?;
```

## Prerequisites

### AMQP Broker (RabbitMQ)

If you already have an AMQP broker running, set the environment variable `AMQP_ADDR` to your broker's URL (e.g., `amqp://localhost:5672//`).

Otherwise, run the helper script:

```bash
./scripts/brokers/amqp.sh
```

### Redis Broker

For Redis broker or RedBeat scheduler, ensure Redis is running:

```bash
./scripts/brokers/redis.sh
```

Or set the environment variable:

```bash
export REDIS_URL=redis://localhost:6379/0
```

## Examples

The [`examples/`](examples/) directory contains comprehensive examples:

### Worker Examples
- [`examples/celery_worker.rs`](examples/celery_worker.rs) - Complete worker setup with task production and consumption
- [`examples/celery_app.py`](examples/celery_app.py) - Python Celery app for interoperability testing

### Scheduler Examples
- [`examples/beat_app.rs`](examples/beat_app.rs) - Basic Beat scheduler
- [`examples/celery_redbeat.rs`](examples/celery_redbeat.rs) - Production-ready RedBeat distributed scheduler (manual configuration)

### Running Examples

![](./img/demo.gif)

#### Worker Operations

```bash
# Start a worker to consume tasks
cargo run --example celery_worker consume

# Send tasks to workers
cargo run --example celery_worker produce add
cargo run --example celery_worker produce buggy_task
cargo run --example celery_worker produce long_running_task
```

#### Beat Scheduler

```bash
# Start basic Beat scheduler
cargo run --example beat_app

# Start distributed RedBeat scheduler (manual configuration)
cargo run --example celery_redbeat
```

#### Python Interoperability

```bash
# Install Python dependencies
pip install -r requirements.txt

# Start Python worker
python examples/celery_app.py consume

# Send tasks from Python
python examples/celery_app.py produce
```

## Configuration

### Environment Variables

```bash
# Broker URLs
export AMQP_ADDR=amqp://localhost:5672//
export REDIS_URL=redis://localhost:6379/0

# RedBeat Configuration
export REDBEAT_FOLLOWER_CHECK_INTERVAL=30  # seconds
export REDBEAT_LOCK_TIMEOUT=60             # seconds

# Logging
export RUST_LOG=info
```

### Task Routing

Configure task routing to different queues:

```rust
let app = celery::app!(
    broker = AMQPBroker { broker_url },
    tasks = [add, multiply],
    task_routes = [
        "add" => "math_queue",
        "multiply" => "math_queue",
        "*" => "default_queue",
    ],
).await?;
```

## Architecture

### Task Flow

1. **Task Definition**: Define tasks with `#[celery::task]` attribute
2. **Task Production**: Send tasks to message queue via broker
3. **Task Consumption**: Workers consume tasks from queues
4. **Task Execution**: Workers execute tasks and return results
5. **Scheduling**: Beat scheduler sends periodic tasks

### Distributed Components

- **Workers**: Consume and execute tasks (horizontally scalable)
- **Brokers**: Message queue systems (AMQP/Redis)
- **Beat**: Task scheduler (single instance or distributed with RedBeat)
- **RedBeat**: Distributed scheduler with Redis coordination

## Development Status

✅ = Supported and stable<br/>
⚠️ = Partially implemented<br/>
🔴 = Not supported yet

### Core Features

| Feature | Status | Notes |
|---------|--------|-------|
| Task Definition | ✅ | Compile-time task registration |
| Task Execution | ✅ | Async task execution |
| Task Routing | ✅ | Flexible routing rules |
| Error Handling | ✅ | Comprehensive error types |
| Serialization | ✅ | JSON serialization |

### Brokers

| Broker | Status | Features |
|--------|--------|----------|
| AMQP (RabbitMQ) | ✅ | Full support with connection pooling |
| Redis | ✅ | Full support with connection pooling |

### Schedulers

| Scheduler | Status | Features |
|-----------|--------|----------|
| Local Beat | ✅ | Single-instance scheduling |
| RedBeat | ✅ | Distributed scheduling with Redis |

### Backends

| Backend | Status | Notes |
|---------|--------|-------|
| RPC | 🔴 | Planned |
| Redis | 🔴 | Planned |

## Migration Guide

### From rusty-celery

The API is 100% compatible. Simply update your `Cargo.toml`:

```toml
[dependencies]
# Change from:
# celery = "0.5"

# To:
celery-rs = "0.6"
```

### From Python Celery

Key differences when migrating from Python Celery:

1. **Task Definition**: Tasks are defined at compile-time with attributes
2. **Type Safety**: Strong typing for task arguments and return values
3. **Async Runtime**: Built on tokio async runtime
4. **Configuration**: Environment variables and builder patterns

## Contributing

We welcome contributions! This fork aims to:
- Maintain API compatibility with the original project
- Provide active maintenance and support
- Keep dependencies updated
- Fix bugs and add features requested by the community

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Acknowledgments

Special thanks to the original [`rusty-celery`](https://github.com/rusty-celery/rusty-celery) team for creating this excellent foundation. This fork builds upon their work while ensuring continued development and support for the Rust Celery ecosystem.
