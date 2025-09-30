use super::{scheduled_task::ScheduledTask, Schedule};
use crate::{broker::Broker, error::BeatError, protocol::TryCreateMessage};
use log::{debug, info};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use std::future::Future;
use std::pin::Pin;

const DEFAULT_SLEEP_INTERVAL: Duration = Duration::from_millis(500);

/// Task execution callback type
pub type TaskExecutionCallback = Box<dyn Fn(String, SystemTime) -> Pin<Box<dyn Future<Output = Result<(), BeatError>> + Send>> + Send + Sync>;

/// A [`Scheduler`] manages scheduled tasks execution
pub struct Scheduler {
    heap: BinaryHeap<ScheduledTask>,
    default_sleep_interval: Duration,
    pub broker: Box<dyn Broker>,
    pub task_execution_callback: Option<TaskExecutionCallback>,
}

impl Scheduler {
    /// Create a new scheduler with the given `broker`.
    pub fn new(broker: Box<dyn Broker>) -> Scheduler {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: DEFAULT_SLEEP_INTERVAL,
            broker,
            task_execution_callback: None,
        }
    }

    /// Set task execution callback for Redis status updates
    pub fn set_task_execution_callback(&mut self, callback: TaskExecutionCallback) {
        self.task_execution_callback = Some(callback);
    }

    /// Initialize scheduler
    pub async fn setup_schedule(&mut self) -> Result<(), BeatError> {
        Ok(())
    }

    /// Schedule the execution of a task.
    pub fn schedule_task<S>(
        &mut self,
        name: String,
        message_factory: Box<dyn TryCreateMessage>,
        queue: String,
        schedule: S,
    ) where
        S: Schedule + 'static,
    {
        match schedule.next_call_at(None) {
            Some(next_call_at) => self.heap.push(ScheduledTask::new(
                name,
                message_factory,
                queue,
                schedule,
                next_call_at,
            )),
            None => debug!(
                "The schedule of task {} never scheduled the task to run, so it has been dropped.",
                name
            ),
        }
    }

    /// Get all scheduled tasks.
    pub fn get_scheduled_tasks(&mut self) -> &mut BinaryHeap<ScheduledTask> {
        &mut self.heap
    }

    /// Tick once - main scheduling logic
    pub async fn tick(&mut self) -> Result<SystemTime, BeatError> {
        let now = SystemTime::now();
        
        // 统一的本地调度逻辑，不区分后端类型
        self.tick_local(now).await
    }

    /// Local heap-based scheduling
    async fn tick_local(&mut self, now: SystemTime) -> Result<SystemTime, BeatError> {
        let next_task_time = if let Some(scheduled_task) = self.heap.peek() {
            scheduled_task.next_call_at
        } else {
            now + self.default_sleep_interval
        };

        if next_task_time <= now {
            let mut scheduled_task = self.heap.pop().unwrap();
            self.send_scheduled_task(&mut scheduled_task).await?;

            if let Some(rescheduled_task) = scheduled_task.reschedule_task() {
                self.heap.push(rescheduled_task);
            }
        }

        Ok(next_task_time)
    }

    /// Send a task to the broker
    async fn send_scheduled_task(
        &self,
        scheduled_task: &mut ScheduledTask,
    ) -> Result<(), BeatError> {
        let message = scheduled_task.message_factory.try_create_message()?;

        info!(
            "🚀 Sending task {}[{}] to {} queue",
            scheduled_task.name,
            message.task_id(),
            &scheduled_task.queue
        );

        self.broker.send(&message, &scheduled_task.queue).await?;
        let execution_time = SystemTime::now();
        scheduled_task.last_run_at.replace(execution_time);
        scheduled_task.total_run_count += 1;

        // Notify scheduler backend about task execution for Redis sync
        if let Some(callback) = &self.task_execution_callback {
            if let Err(e) = callback(scheduled_task.name.clone(), execution_time).await {
                log::warn!("Failed to update task status in Redis: {}", e);
            }
        }

        Ok(())
    }

    /// Close scheduler and release locks
    pub async fn close(&mut self) -> Result<(), BeatError> {
        // RedBeat handles cleanup internally
        Ok(())
    }
}
