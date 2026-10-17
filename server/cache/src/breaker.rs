//! 每个上游一个熔断器：连续失败达到阈值后打开，冷却期内不再向它发请求；冷却期过后放一个探测请求
//! （半开），成功则关闭，失败则重新打开。

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Closed {
        failures: u32,
    },
    Open {
        until: Instant,
    },
    /// 探测请求在途。
    HalfOpen,
}

#[derive(Debug)]
pub struct Breaker {
    threshold: u32,
    cooldown: Duration,
    state: Mutex<State>,
}

impl Breaker {
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown,
            state: Mutex::new(State::Closed { failures: 0 }),
        }
    }

    /// 现在能否向这个上游发请求。冷却期过后第一个调用者获得探测机会。
    pub fn allow(&self) -> bool {
        self.allow_at(Instant::now())
    }

    fn allow_at(&self, now: Instant) -> bool {
        let Ok(mut s) = self.state.lock() else {
            return true;
        };
        match *s {
            State::Closed { .. } => true,
            State::Open { until } if now >= until => {
                *s = State::HalfOpen;
                true
            }
            State::Open { .. } | State::HalfOpen => false,
        }
    }

    pub fn success(&self) {
        if let Ok(mut s) = self.state.lock() {
            *s = State::Closed { failures: 0 };
        }
    }

    pub fn failure(&self) {
        self.failure_at(Instant::now());
    }

    fn failure_at(&self, now: Instant) {
        let Ok(mut s) = self.state.lock() else {
            return;
        };
        *s = match *s {
            State::Closed { failures } if failures + 1 < self.threshold => State::Closed { failures: failures + 1 },
            _ => State::Open {
                until: now + self.cooldown,
            },
        };
    }

    /// 是否处于打开状态（冷却中或探测中）。
    pub fn is_open(&self) -> bool {
        self.state
            .lock()
            .map(|s| !matches!(*s, State::Closed { .. }))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_threshold_and_probes_after_cooldown() {
        let b = Breaker::new(3, Duration::from_secs(1));
        let t0 = Instant::now();
        b.failure_at(t0);
        b.failure_at(t0);
        assert!(b.allow_at(t0) && !b.is_open(), "below threshold");
        b.success();
        b.failure_at(t0);
        b.failure_at(t0);
        assert!(!b.is_open(), "success reset the count");
        b.failure_at(t0);
        assert!(b.is_open());
        assert!(!b.allow_at(t0 + Duration::from_millis(500)), "cooling down");
        // 冷却结束：只放一个探测请求
        let later = t0 + Duration::from_secs(1);
        assert!(b.allow_at(later));
        assert!(!b.allow_at(later), "one probe at a time");
        // 探测失败：重新打开
        b.failure_at(later);
        assert!(!b.allow_at(later + Duration::from_millis(10)));
        // 再次探测成功：关闭
        let again = later + Duration::from_secs(2);
        assert!(b.allow_at(again));
        b.success();
        assert!(b.allow() && !b.is_open());
        assert!(Breaker::new(0, Duration::ZERO).allow(), "threshold at least 1");
        let one = Breaker::new(0, Duration::from_secs(60));
        one.failure();
        assert!(!one.allow());
    }
}
