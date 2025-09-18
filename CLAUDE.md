# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **Rusty Celery**, a Rust implementation of the Celery distributed task queue system. The project provides both producer and consumer capabilities, supporting AMQP and Redis brokers. Key components include:

- **Core App (`src/app/mod.rs`)**: The main `Celery` struct and `CeleryBuilder` for creating apps
- **Brokers (`src/broker/`)**: AMQP and Redis broker implementations
- **Tasks (`src/task/`)**: Task definitions, signatures, async results, and execution
- **Protocol (`src/protocol/`)**: Message serialization and Celery protocol implementation
- **Beat (`src/beat/`)**: Periodic task scheduling (cron-like functionality)
- **Codegen**: Procedural macros for the `#[celery::task]` attribute

## Development Commands

### Build and Test
```bash
# Build the project
cargo build
# or
make build

# Run tests (excludes broker integration tests)
make test

# Run all tests including broker integration tests
make run-all-tests

# Run specific broker tests
make broker-tests
```

### Code Quality
```bash
# Format code
make format

# Lint (format check + clippy)
make lint

# Individual checks
make check-fmt
make check-clippy
```

### Documentation
```bash
# Build documentation
make build-docs
```

### Examples
```bash
# Run Rust celery app as consumer
cargo run --example celery_app consume

# Run Rust celery app as producer
cargo run --example celery_app produce [task_name]

# Available tasks: add, buggy_task, long_running_task, bound_task

# Run Beat scheduler
cargo run --example beat_app
```

## Architecture Notes

### Task Definition
Tasks are defined using the `#[celery::task]` procedural macro which generates a `Task` struct and implementation. The macro supports options like `name`, `max_retries`, `time_limit`, `bind`, etc.

### App Creation
Apps are created using the `celery::app!` macro which configures brokers, registers tasks, and sets up routing rules.

### Broker Architecture
- **AMQP Broker**: Uses `lapin` crate for RabbitMQ/AMQP connections
- **Redis Broker**: Uses `redis` crate with connection manager
- Brokers implement the `Broker` trait for sending/receiving messages

### Message Protocol
- Supports multiple content types: JSON (default), MessagePack, YAML, Pickle
- Compatible with Python Celery message format
- Handles task routing, delivery, and acknowledgments

### Concurrency Model
Built on Tokio async runtime with:
- Async task execution
- Stream-based message consumption
- Configurable prefetch counts for concurrency control

## Python Interoperability

The project maintains compatibility with Python Celery:
- Same message format and protocol
- Can consume tasks produced by Python Celery
- Can produce tasks for Python Celery workers
- Example Python app in `examples/celery_app.py`

## Environment Setup

Requires AMQP_ADDR environment variable for broker connection:
```bash
export AMQP_ADDR="amqp://localhost:5672//"
```

For Redis:
```bash
export REDIS_ADDR="redis://localhost:6379/"
```

Use provided broker setup script:
```bash
./scripts/brokers/amqp.sh
```