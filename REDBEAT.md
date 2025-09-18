# RedBeat Scheduler for celery-rs

This implementation provides a Redis-based distributed scheduler backend for celery-rs, equivalent to Python's `redbeat.RedBeatScheduler`.

## Features

- **Distributed Scheduling**: Multiple beat instances can run simultaneously without duplicate task execution
- **Redis-based Coordination**: Uses Redis for distributed locking and schedule storage
- **Fault Tolerance**: Automatic failover when the primary scheduler instance fails
- **Dynamic Scheduling**: Schedule information stored in Redis can be modified at runtime

## Usage

### Basic Setup

```rust
use celery::beat::{CronSchedule, RedBeatSchedulerBackend};

#[tokio::main]
async fn main() -> Result<()> {
    let redis_url = "redis://127.0.0.1:6379/0";
    
    // Create RedBeat scheduler backend
    let redbeat_backend = RedBeatSchedulerBackend::new(redis_url.to_string())?;

    // Build Beat with RedBeat backend
    let mut beat = celery::beat!(
        broker = RedisBroker { redis_url },
        backend = redbeat_backend,  // This is equivalent to Python's redbeat.RedBeatScheduler
        tasks = [
            "my_task" => {
                my_task_function,
                schedule = CronSchedule::from_string("*/30 * * * * *")?,
                args = (),
            }
        ],
        task_routes = ["*" => "celery"],
    ).await?;

    beat.start().await?;
    Ok(())
}
```

### Python Equivalent

This Rust implementation is equivalent to the following Python configuration:

```python
# Python Celery with RedBeat
from celery import Celery
from celery.schedules import crontab

app = Celery('myapp')
app.conf.update(
    broker_url='redis://127.0.0.1:6379/0',
    beat_scheduler='redbeat.RedBeatScheduler',  # This is what we implemented!
    redbeat_redis_url='redis://127.0.0.1:6379/0',
)

app.conf.beat_schedule = {
    'my-task': {
        'task': 'my_task_function',
        'schedule': crontab(second='*/30'),  # Every 30 seconds
    },
}
```

## How It Works

### Distributed Locking

The RedBeat scheduler uses Redis distributed locks to ensure only one scheduler instance is active at a time:

1. **Lock Acquisition**: Each scheduler tries to acquire a Redis lock (`redbeat:lock`)
2. **Schedule Sync**: Only the instance with the lock can modify the schedule
3. **Automatic Failover**: If the lock holder fails, other instances can take over

### Schedule Storage

Schedule information is stored in Redis hash (`redbeat:schedule`):

```json
{
  "task_name": {
    "name": "my_task",
    "task": "my_task_function", 
    "schedule": "*/30 * * * * *",
    "enabled": true,
    "last_run_at": 1634567890,
    "total_run_count": 42
  }
}
```

## Configuration

### Environment Variables

- `REDIS_URL`: Redis connection URL (default: `redis://127.0.0.1:6379/0`)

### Redis Keys

- `redbeat:lock`: Distributed lock key
- `redbeat:schedule`: Schedule storage key

## Benefits Over LocalSchedulerBackend

| Feature | LocalSchedulerBackend | RedBeatSchedulerBackend |
|---------|----------------------|-------------------------|
| Multiple Instances | ❌ Duplicate execution | ✅ Coordinated execution |
| Dynamic Scheduling | ❌ Code changes required | ✅ Runtime modifications |
| Fault Tolerance | ❌ Single point of failure | ✅ Automatic failover |
| Persistence | ❌ In-memory only | ✅ Redis persistence |

## Running the Example

```bash
# Start Redis
redis-server

# Run the RedBeat example
cd celery-rs
cargo run --example redbeat_app
```

## Integration with Existing Projects

To use RedBeat in your existing celery-rs project:

1. **Add RedBeat backend**:
```rust
use celery::beat::RedBeatSchedulerBackend;

let redbeat_backend = RedBeatSchedulerBackend::new(redis_url)?;
```

2. **Replace LocalSchedulerBackend**:
```rust
// Before (single instance)
let mut beat = celery::beat!(
    broker = RedisBroker { redis_url },
    // Uses LocalSchedulerBackend by default
    tasks = [...],
).await?;

// After (distributed)
let mut beat = celery::beat!(
    broker = RedisBroker { redis_url },
    backend = redbeat_backend,  // Now supports multiple instances!
    tasks = [...],
).await?;
```

This implementation brings Python Celery's RedBeat functionality to Rust! 🎉
