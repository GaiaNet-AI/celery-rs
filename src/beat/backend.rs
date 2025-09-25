/// This module contains the definition of application-provided scheduler backends.
use super::scheduled_task::ScheduledTask;
use crate::error::BeatError;
use std::collections::BinaryHeap;

/// A `SchedulerBackend` is in charge of keeping track of the internal state of the scheduler
/// according to some source of truth, such as a database.
pub trait SchedulerBackend {
    /// Check whether the internal state of the scheduler should be synchronized.
    fn should_sync(&self) -> bool;

    /// Synchronize the internal state of the scheduler.
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError>;
}

/// The default [`SchedulerBackend`](trait.SchedulerBackend.html).
pub struct LocalSchedulerBackend {}

impl LocalSchedulerBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for LocalSchedulerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerBackend for LocalSchedulerBackend {
    fn should_sync(&self) -> bool {
        false
    }

    fn sync(&mut self, _scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
        Ok(())
    }
}

// Re-export RedBeat scheduler
pub use super::redbeat::RedBeatScheduler;
