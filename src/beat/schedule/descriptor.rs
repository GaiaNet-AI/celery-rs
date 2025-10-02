use super::{CronSchedule, DeltaSchedule, Schedule};
use crate::error::ScheduleError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScheduleDescriptor {
    Cron(CronDescriptor),
    Delta(DeltaDescriptor),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CronDescriptor {
    pub minutes: Vec<u32>,
    pub hours: Vec<u32>,
    pub month_days: Vec<u32>,
    pub months: Vec<u32>,
    pub week_days: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeltaDescriptor {
    pub interval_millis: u64,
}

impl ScheduleDescriptor {
    pub fn from_schedule(schedule: &dyn Schedule) -> Option<Self> {
        schedule.describe()
    }

    pub fn delta(interval: Duration) -> Self {
        ScheduleDescriptor::Delta(DeltaDescriptor {
            interval_millis: interval.as_millis() as u64,
        })
    }

    pub fn cron(descriptor: CronDescriptor) -> Self {
        ScheduleDescriptor::Cron(descriptor)
    }

    pub fn to_schedule(&self) -> Result<Box<dyn Schedule>, ScheduleError> {
        match self {
            ScheduleDescriptor::Delta(desc) => Ok(Box::new(DeltaSchedule::new(
                Duration::from_millis(desc.interval_millis),
            ))),
            ScheduleDescriptor::Cron(desc) => Ok(Box::new(desc.to_schedule()?)),
        }
    }
}

impl CronDescriptor {
    pub fn to_schedule(&self) -> Result<CronSchedule<chrono::Utc>, ScheduleError> {
        CronSchedule::new(
            self.minutes.clone(),
            self.hours.clone(),
            self.month_days.clone(),
            self.months.clone(),
            self.week_days.clone(),
        )
    }
}

impl DeltaDescriptor {
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_descriptor_roundtrip() {
        let schedule = DeltaSchedule::new(Duration::from_millis(250));
        let descriptor = ScheduleDescriptor::from_schedule(&schedule).expect("descriptor");

        let restored = descriptor.to_schedule().expect("restored");
        let next_original = schedule.next_call_at(None).unwrap();
        let next_restored = restored.next_call_at(None).unwrap();

        assert!(
            next_restored
                .duration_since(next_original)
                .unwrap_or_else(|_| Duration::from_secs(0))
                < Duration::from_millis(2)
        );
    }

    #[test]
    fn cron_descriptor_roundtrip() {
        let cron = CronSchedule::from_string("*/5 * * * *").expect("cron");
        let descriptor = ScheduleDescriptor::from_schedule(&cron).expect("descriptor");

        let restored = descriptor.to_schedule().expect("restored");
        let next_original = cron.next_call_at(None).unwrap();
        let next_restored = restored.next_call_at(None).unwrap();

        assert!(
            next_restored
                .duration_since(next_original)
                .unwrap_or_else(|_| Duration::from_secs(0))
                < Duration::from_secs(60)
        );
    }
}
