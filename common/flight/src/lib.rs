//! Single-flight：同一个 key 的并发请求只执行一次，其余请求等待并共享结果。
//!
//! 与常见实现不同，领取（[`Group::claim`]）和完成是分开的：调用方可以先为一批 key 领取，把自己是
//! leader 的那些合成一次批量请求，再逐个完成（缓存回源时一次请求取多个对象）。
//!
//! leader 没有完成就被丢弃（请求被取消、任务 panic）时，等待者收到 `None`，应重新领取：其中一个会
//! 成为新的 leader，不会因为第一个请求被取消而全部失败。

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

/// 一组 single-flight 调用。
pub struct Group<K, V> {
    calls: Arc<Mutex<HashMap<K, watch::Receiver<Option<V>>>>>,
}

impl<K, V> Default for Group<K, V> {
    fn default() -> Self {
        Self {
            calls: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// 领取的结果。
pub enum Claim<K: Hash + Eq + Clone, V: Clone> {
    /// 由调用方执行，并通过 [`Leader::complete`] 交付结果。
    Leader(Leader<K, V>),
    /// 已有人在执行：等待它的结果。
    Follower(Follower<V>),
}

/// 负责执行的一方。
pub struct Leader<K: Hash + Eq + Clone, V: Clone> {
    calls: Arc<Mutex<HashMap<K, watch::Receiver<Option<V>>>>>,
    key: K,
    tx: watch::Sender<Option<V>>,
}

/// 等待的一方。
pub struct Follower<V: Clone> {
    rx: watch::Receiver<Option<V>>,
}

impl<K: Hash + Eq + Clone, V: Clone> Group<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// 领取 `key`：没有进行中的调用时成为 leader，否则成为等待者。
    pub fn claim(&self, key: K) -> Claim<K, V> {
        let mut calls = match self.calls.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        if let Some(rx) = calls.get(&key) {
            return Claim::Follower(Follower { rx: rx.clone() });
        }
        let (tx, rx) = watch::channel(None);
        calls.insert(key.clone(), rx);
        Claim::Leader(Leader {
            calls: self.calls.clone(),
            key,
            tx,
        })
    }

    /// 便捷用法：成为 leader 时执行 `f`，否则等待结果；leader 被取消时重新领取。
    pub async fn run<F, Fut>(&self, key: K, mut f: F) -> V
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = V>,
    {
        loop {
            match self.claim(key.clone()) {
                Claim::Leader(l) => {
                    let v = f().await;
                    l.complete(v.clone());
                    return v;
                }
                Claim::Follower(w) => {
                    if let Some(v) = w.wait().await {
                        return v;
                    }
                }
            }
        }
    }

    /// 进行中的调用数。
    pub fn in_flight(&self) -> usize {
        self.calls.lock().map(|c| c.len()).unwrap_or(0)
    }
}

impl<K: Hash + Eq + Clone, V: Clone> Leader<K, V> {
    pub fn key(&self) -> &K {
        &self.key
    }

    /// 交付结果，唤醒全部等待者。
    pub fn complete(self, value: V) {
        self.tx.send_replace(Some(value));
        // Drop 负责从表中移除
    }
}

impl<K: Hash + Eq + Clone, V: Clone> Drop for Leader<K, V> {
    fn drop(&mut self) {
        let mut calls = match self.calls.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        // 只移除自己的那一项（同一个 key 之后可能已有新的调用）
        if calls
            .get(&self.key)
            .is_some_and(|rx| rx.same_channel(&self.tx.subscribe()))
        {
            calls.remove(&self.key);
        }
    }
}

impl<V: Clone> Follower<V> {
    /// 等待 leader 的结果；leader 没有完成就被丢弃时返回 `None`。
    pub async fn wait(mut self) -> Option<V> {
        let delivered = match self.rx.wait_for(Option::is_some).await {
            Ok(v) => v.clone(),
            Err(_) => None,
        };
        // 发送端已丢弃：最后看一眼是否在丢弃前交付了结果
        delivered.or_else(|| self.rx.borrow().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn concurrent_calls_share_one_execution() {
        let g = Arc::new(Group::<u32, u32>::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let (g, runs) = (g.clone(), runs.clone());
            tasks.push(tokio::spawn(async move {
                g.run(7, || async {
                    runs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    42
                })
                .await
            }));
        }
        for t in tasks {
            assert_eq!(t.await.expect("join"), 42);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(g.in_flight(), 0, "entry removed after completion");
        // 完成之后的新调用重新执行
        assert_eq!(g.run(7, || async { 43 }).await, 43);
    }

    #[tokio::test]
    async fn claim_separates_leaders_and_followers() {
        let g = Group::<&str, String>::new();
        let Claim::Leader(l) = g.claim("a") else {
            panic!("first claim leads");
        };
        assert_eq!(*l.key(), "a");
        let Claim::Follower(f) = g.claim("a") else {
            panic!("second claim follows");
        };
        assert!(matches!(g.claim("b"), Claim::Leader(_)), "other keys are independent");
        assert_eq!(g.in_flight(), 1, "dropped leader of b removed itself");
        l.complete("v".into());
        assert_eq!(f.wait().await.as_deref(), Some("v"));
        assert_eq!(g.in_flight(), 0);
    }

    #[tokio::test]
    async fn abandoned_leader_hands_over() {
        let g = Arc::new(Group::<u8, u8>::new());
        let Claim::Leader(l) = g.claim(1) else {
            panic!("leader");
        };
        let Claim::Follower(f) = g.claim(1) else {
            panic!("follower");
        };
        drop(l);
        assert_eq!(f.wait().await, None, "leader gone without a result");
        // run 在 leader 被取消后重新领取并自己执行
        let Claim::Leader(l) = g.claim(2) else {
            panic!("leader");
        };
        let g2 = g.clone();
        let waiter = tokio::spawn(async move { g2.run(2, || async { 9 }).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(l);
        assert_eq!(waiter.await.expect("join"), 9);
    }

    #[tokio::test]
    async fn stale_leader_does_not_remove_newer_call() {
        let g = Group::<u8, u8>::new();
        let Claim::Leader(old) = g.claim(1) else {
            panic!("leader");
        };
        // 模拟表项已被替换（例如旧 leader 完成后有了新的调用）
        g.calls.lock().expect("lock").remove(&1);
        let Claim::Leader(new) = g.claim(1) else {
            panic!("new leader");
        };
        drop(old);
        assert_eq!(g.in_flight(), 1, "newer call kept");
        new.complete(5);
        assert_eq!(g.in_flight(), 0);
    }

    #[tokio::test]
    async fn result_delivered_just_before_drop_is_seen() {
        let g = Group::<u8, u8>::new();
        let Claim::Leader(l) = g.claim(1) else {
            panic!("leader");
        };
        let Claim::Follower(f) = g.claim(1) else {
            panic!("follower");
        };
        l.complete(3);
        assert_eq!(f.wait().await, Some(3));
    }
}
