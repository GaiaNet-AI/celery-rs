use super::{scheduled_task::ScheduledTask, Schedule};
use crate::{broker::Broker, error::BeatError, protocol::TryCreateMessage};
use log::{debug, info};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};

const DEFAULT_SLEEP_INTERVAL: Duration = Duration::from_millis(500);

/// A [`Scheduler`] manages scheduled tasks execution
pub struct Scheduler {
    heap: BinaryHeap<ScheduledTask>,
    default_sleep_interval: Duration,
    pub broker: Box<dyn Broker>,
    redbeat_scheduler: Option<super::redbeat::RedBeatScheduler>,
}

impl Scheduler {
    /// Create a new scheduler with the given `broker`.
    pub fn new(broker: Box<dyn Broker>) -> Scheduler {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: DEFAULT_SLEEP_INTERVAL,
            broker,
            redbeat_scheduler: None,
        }
    }

    /// Create a new scheduler with RedBeat distributed scheduling.
    pub fn new_with_redbeat(
        broker: Box<dyn Broker>,
        redbeat_scheduler: super::redbeat::RedBeatScheduler,
    ) -> Scheduler {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: DEFAULT_SLEEP_INTERVAL,
            broker,
            redbeat_scheduler: Some(redbeat_scheduler),
        }
    }

    /// Get mutable reference to RedBeat scheduler for cleanup
    pub fn get_redbeat_scheduler_mut(&mut self) -> Option<&mut super::redbeat::RedBeatScheduler> {
        self.redbeat_scheduler.as_mut()
    }

    /// Initialize scheduler
    pub async fn setup_schedule(&mut self) -> Result<(), BeatError> {
        // RedBeat handles initialization internally
        Ok(())
    }

    /// Schedule the execution of a task with cron expression.
    pub fn schedule_task_with_cron<S>(
        &mut self,
        name: String,
        message_factory: Box<dyn TryCreateMessage>,
        queue: String,
        schedule: S,
        cron_expression: String,
    ) where
        S: Schedule + 'static,
    {
        match schedule.next_call_at(None) {
            Some(next_call_at) => self.heap.push(ScheduledTask::new_with_cron(
                name,
                message_factory,
                queue,
                schedule,
                next_call_at,
                cron_expression,
            )),
            None => debug!(
                "The schedule of task {} never scheduled the task to run, so it has been dropped.",
                name
            ),
        }
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

        // Use RedBeat if available
        if let Some(ref mut redbeat) = self.redbeat_scheduler {
            let sleep_duration = redbeat.tick().await?;
            return Ok(now + sleep_duration);
        }

        // Fallback to local heap scheduling
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
        scheduled_task.last_run_at.replace(SystemTime::now());
        scheduled_task.total_run_count += 1;

        Ok(())
    }

    /// Close scheduler and release locks
    pub async fn close(&mut self) -> Result<(), BeatError> {
        // RedBeat handles cleanup internally
        Ok(())
    }
}
