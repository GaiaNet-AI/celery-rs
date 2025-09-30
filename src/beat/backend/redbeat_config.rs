use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration options for RedBeat scheduler backend.
///
/// These options follow the same pattern as TaskOptions in the original architecture,
/// supporting configuration inheritance and builder pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedBeatConfig {
    /// Redis connection URL
    pub redis_url: Option<String>,

    /// Timeout for Redis locks in seconds
    pub lock_timeout: Option<u64>,

    /// Interval for followers to check for leadership opportunities
    pub follower_check_interval: Option<Duration>,

    /// Interval for leaders to renew their locks
    pub lock_renewal_interval: Option<Duration>,

    /// Key prefix for Redis keys
    pub key_prefix: Option<String>,

    /// Schedule key for Redis sorted set
    pub schedule_key: Option<String>,

    /// Instance ID for this scheduler instance
    pub instance_id: Option<String>,
}

impl Default for RedBeatConfig {
    fn default() -> Self {
        Self {
            redis_url: None,
            lock_timeout: Some(60),
            follower_check_interval: Some(Duration::from_secs(30)),
            lock_renewal_interval: None, // Will be calculated from lock_timeout
            key_prefix: Some("redbeat".to_string()),
            schedule_key: Some("redbeat:schedule".to_string()),
            instance_id: None, // Will be generated
        }
    }
}

impl RedBeatConfig {
    /// Create a new RedBeat configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set Redis URL
    pub fn redis_url<S: Into<String>>(mut self, url: S) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    /// Set lock timeout in seconds
    pub fn lock_timeout(mut self, timeout_secs: u64) -> Self {
        self.lock_timeout = Some(timeout_secs);
        // Auto-calculate renewal interval as 1/3 of timeout
        self.lock_renewal_interval = Some(Duration::from_secs(timeout_secs / 3));
        self
    }

    /// Set follower check interval
    pub fn follower_check_interval(mut self, interval: Duration) -> Self {
        self.follower_check_interval = Some(interval);
        self
    }

    /// Set lock renewal interval
    pub fn lock_renewal_interval(mut self, interval: Duration) -> Self {
        self.lock_renewal_interval = Some(interval);
        self
    }

    /// Set key prefix for Redis keys
    pub fn key_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    /// Set schedule key for Redis sorted set
    pub fn schedule_key<S: Into<String>>(mut self, key: S) -> Self {
        self.schedule_key = Some(key.into());
        self
    }

    /// Set instance ID
    pub fn instance_id<S: Into<String>>(mut self, id: S) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    /// Update the fields in `self` with the fields in `other`.
    /// This follows the same pattern as TaskOptions::update() in the original architecture.
    pub fn update(&mut self, other: &RedBeatConfig) {
        self.redis_url = self.redis_url.clone().or_else(|| other.redis_url.clone());
        self.lock_timeout = self.lock_timeout.or(other.lock_timeout);
        self.follower_check_interval = self
            .follower_check_interval
            .or(other.follower_check_interval);
        self.lock_renewal_interval = self.lock_renewal_interval.or(other.lock_renewal_interval);
        self.key_prefix = self.key_prefix.clone().or_else(|| other.key_prefix.clone());
        self.schedule_key = self
            .schedule_key
            .clone()
            .or_else(|| other.schedule_key.clone());
        self.instance_id = self
            .instance_id
            .clone()
            .or_else(|| other.instance_id.clone());
    }

    /// Override the fields in `other` with the fields in `self`.
    pub fn override_other(&self, other: &mut RedBeatConfig) {
        other.update(self);
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<(), String> {
        if let Some(timeout) = self.lock_timeout {
            if timeout < 5 {
                return Err("Lock timeout must be at least 5 seconds".to_string());
            }
            if timeout > 3600 {
                return Err("Lock timeout should not exceed 1 hour".to_string());
            }
        }

        if let Some(interval) = self.follower_check_interval {
            if interval < Duration::from_secs(1) {
                return Err("Follower check interval must be at least 1 second".to_string());
            }
            if interval > Duration::from_secs(300) {
                return Err("Follower check interval should not exceed 5 minutes".to_string());
            }
        }

        if let Some(interval) = self.lock_renewal_interval {
            if interval < Duration::from_secs(1) {
                return Err("Lock renewal interval must be at least 1 second".to_string());
            }
        }

        // Validate renewal interval vs lock timeout relationship
        if let (Some(timeout), Some(renewal)) = (self.lock_timeout, self.lock_renewal_interval) {
            if renewal.as_secs() >= timeout {
                return Err("Lock renewal interval must be less than lock timeout".to_string());
            }
        }

        Ok(())
    }

    /// Resolve all configuration values, providing defaults where needed
    pub fn resolve(self) -> Result<ResolvedRedBeatConfig, String> {
        // Validate configuration first
        self.validate()?;

        let default_config = RedBeatConfig::default();
        let mut resolved = self;
        resolved.update(&default_config);

        let lock_timeout = resolved.lock_timeout.unwrap();

        Ok(ResolvedRedBeatConfig {
            redis_url: resolved.redis_url.ok_or("Redis URL is required")?,
            lock_timeout,
            follower_check_interval: resolved.follower_check_interval.unwrap(),
            lock_renewal_interval: resolved
                .lock_renewal_interval
                .unwrap_or_else(|| Duration::from_secs(lock_timeout / 3)),
            key_prefix: resolved.key_prefix.unwrap(),
            schedule_key: resolved.schedule_key.unwrap(),
            instance_id: resolved.instance_id.unwrap_or_else(generate_instance_id),
        })
    }
}

/// Resolved RedBeat configuration with all values set
#[derive(Clone, Debug)]
pub struct ResolvedRedBeatConfig {
    pub redis_url: String,
    pub lock_timeout: u64,
    pub follower_check_interval: Duration,
    pub lock_renewal_interval: Duration,
    pub key_prefix: String,
    pub schedule_key: String,
    pub instance_id: String,
}

/// Generate a unique instance ID
fn generate_instance_id() -> String {
    use std::process;
    format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        process::id()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder_pattern() {
        let config = RedBeatConfig::new()
            .redis_url("redis://localhost:6379/0")
            .lock_timeout(30)
            .follower_check_interval(Duration::from_secs(15));

        assert_eq!(
            config.redis_url,
            Some("redis://localhost:6379/0".to_string())
        );
        assert_eq!(config.lock_timeout, Some(30));
        assert_eq!(
            config.follower_check_interval,
            Some(Duration::from_secs(15))
        );
    }

    #[test]
    fn test_config_update() {
        let mut config1 = RedBeatConfig::new().lock_timeout(30);

        let config2 = RedBeatConfig::new()
            .redis_url("redis://localhost:6379/0")
            .lock_timeout(60);

        config1.update(&config2);

        // config1's values should take precedence
        assert_eq!(config1.lock_timeout, Some(30));
        // config2's values should fill in missing fields
        assert_eq!(
            config1.redis_url,
            Some("redis://localhost:6379/0".to_string())
        );
    }

    #[test]
    fn test_config_resolve() {
        let config = RedBeatConfig::new().redis_url("redis://localhost:6379/0");

        let resolved = config.resolve().unwrap();

        assert_eq!(resolved.redis_url, "redis://localhost:6379/0");
        assert_eq!(resolved.lock_timeout, 60); // default
        assert_eq!(resolved.lock_renewal_interval, Duration::from_secs(20)); // 60/3
    }
}
