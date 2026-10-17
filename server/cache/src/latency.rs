//! 最近若干次上游请求的延迟，用于计算对冲阈值（p95）。

use std::sync::Mutex;
use std::time::Duration;

const WINDOW: usize = 512;
/// 样本不足时不对冲依据的默认阈值。
const DEFAULT_THRESHOLD: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct Latency {
    inner: Mutex<Window>,
}

#[derive(Debug)]
struct Window {
    samples: Vec<Duration>,
    next: usize,
    /// 缓存的 p95，读取时若已有 16 个以上的新样本则重算。
    p95: Option<Duration>,
    since_update: usize,
}

impl Default for Latency {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Window {
                samples: Vec::with_capacity(WINDOW),
                next: 0,
                p95: None,
                since_update: 0,
            }),
        }
    }
}

impl Latency {
    pub fn record(&self, d: Duration) {
        let Ok(mut w) = self.inner.lock() else {
            return;
        };
        if w.samples.len() < WINDOW {
            w.samples.push(d);
        } else {
            let i = w.next;
            w.samples[i] = d;
        }
        w.next = (w.next + 1) % WINDOW;
        w.since_update += 1;
    }

    fn p95(&self) -> Option<Duration> {
        let mut w = self.inner.lock().ok()?;
        let stale = w.p95.is_none() || w.since_update >= 16;
        if stale && w.samples.len() >= 20 {
            let mut sorted = w.samples.clone();
            sorted.sort_unstable();
            let idx = (sorted.len() * 95 / 100).min(sorted.len() - 1);
            w.p95 = Some(sorted[idx]);
            w.since_update = 0;
        }
        w.p95
    }

    /// 对冲阈值：最近延迟的 p95，限制在 [`min`, `max`] 内；样本太少时用默认值。
    pub fn hedge_after(&self, min: Duration, max: Duration) -> Duration {
        self.p95().unwrap_or(DEFAULT_THRESHOLD).clamp(min, max.max(min))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p95_tracks_recent_samples() {
        let l = Latency::default();
        let (min, max) = (Duration::from_millis(1), Duration::from_secs(1));
        assert_eq!(l.hedge_after(min, max), DEFAULT_THRESHOLD, "no samples yet");
        for i in 1..=100u64 {
            l.record(Duration::from_millis(i));
        }
        let p = l.hedge_after(min, max);
        assert!(
            (Duration::from_millis(90)..=Duration::from_millis(100)).contains(&p),
            "{p:?}"
        );
        // 夹在 [min, max] 内
        assert_eq!(
            l.hedge_after(Duration::from_millis(200), max),
            Duration::from_millis(200)
        );
        assert_eq!(l.hedge_after(min, Duration::from_millis(10)), Duration::from_millis(10));
        // 窗口滚动：旧样本被新样本替换
        for _ in 0..WINDOW {
            l.record(Duration::from_millis(5));
        }
        assert_eq!(l.hedge_after(min, max), Duration::from_millis(5));
    }
}
