//! 过载保护（服务端设计文档 §5.3）：
//!
//! - [`ConcurrencyLimitLayer`]：全局在途请求上限，超过时立即拒绝（load shed），不排队，
//!   客户端收到可重试的 RESOURCE_EXHAUSTED / RATE_LIMITED；健康检查不受限制；
//! - [`RateLimiter`]：按仓库的写入令牌桶，在写入类接口（上传、创建提交、合入）中检查，
//!   避免一个仓库耗尽整个服务的写入能力。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tonic::Status;
use tower::{Layer, Service};

use crate::status;

/// 按 key（仓库）的令牌桶：每秒补充 `rate` 个，最多积攒 `burst` 个。
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    buckets: Mutex<HashMap<String, (f64, Instant)>>,
}

impl RateLimiter {
    pub fn new(per_second: u32, burst: u32) -> Self {
        Self {
            rate: f64::from(per_second.max(1)),
            burst: f64::from(burst.max(1)),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// 取一个令牌；没有时返回需要等待的时间。
    pub fn check(&self, key: &str) -> Result<(), Duration> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: &str, now: Instant) -> Result<(), Duration> {
        let Ok(mut buckets) = self.buckets.lock() else {
            return Ok(());
        };
        let (tokens, last) = buckets.entry(key.to_owned()).or_insert((self.burst, now));
        let elapsed = now.saturating_duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * self.rate).min(self.burst);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - *tokens) / self.rate))
        }
    }

    /// 检查并转换成 gRPC 状态。
    pub fn admit(limiter: Option<&RateLimiter>, repo: &str) -> Result<(), Status> {
        match limiter.map(|l| l.check(repo)) {
            Some(Err(wait)) => Err(status::rate_limited(
                &format!("write rate limit for repository {repo:?} exceeded"),
                wait,
            )),
            _ => Ok(()),
        }
    }
}

/// 全局在途请求上限。
#[derive(Clone)]
pub struct ConcurrencyLimitLayer {
    permits: Arc<Semaphore>,
}

impl ConcurrencyLimitLayer {
    pub fn new(max: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max.max(1))),
        }
    }
}

impl<S> Layer<S> for ConcurrencyLimitLayer {
    type Service = ConcurrencyLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyLimit {
            inner,
            permits: self.permits.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ConcurrencyLimit<S> {
    inner: S,
    permits: Arc<Semaphore>,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for ConcurrencyLimit<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // 按 tower 的约定：调用已就绪的那个实例，留一个克隆给下一次
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        if req.uri().path().starts_with("/grpc.health.v1.") {
            return Box::pin(inner.call(req));
        }
        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                let fut = inner.call(req);
                Box::pin(async move {
                    let resp = fut.await;
                    drop(permit);
                    resp
                })
            }
            Err(_) => {
                let s = status::rate_limited(
                    "server overloaded: too many requests in flight",
                    Duration::from_millis(100),
                );
                Box::pin(std::future::ready(Ok(s.into_http())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use tokio::sync::Notify;

    #[test]
    fn token_bucket_refills_over_time_per_key() {
        let l = RateLimiter::new(10, 2);
        let t0 = Instant::now();
        assert!(l.check_at("a", t0).is_ok());
        assert!(l.check_at("a", t0).is_ok());
        let wait = l.check_at("a", t0).expect_err("burst used up");
        assert!(wait > Duration::ZERO && wait <= Duration::from_millis(100), "{wait:?}");
        // 其它仓库互不影响
        assert!(l.check_at("b", t0).is_ok());
        // 100 ms 补充 1 个
        assert!(l.check_at("a", t0 + Duration::from_millis(100)).is_ok());
        assert!(l.check_at("a", t0 + Duration::from_millis(100)).is_err());
        // 不超过 burst
        let later = t0 + Duration::from_secs(10);
        assert!(l.check_at("a", later).is_ok());
        assert!(l.check_at("a", later).is_ok());
        assert!(l.check_at("a", later).is_err());
        assert!(l.check("fresh").is_ok());
        // 参数下限为 1
        let tiny = RateLimiter::new(0, 0);
        assert!(tiny.check_at("x", t0).is_ok());
        assert!(tiny.check_at("x", t0).is_err());
    }

    #[test]
    fn admit_maps_to_rate_limited_status() {
        let l = RateLimiter::new(1, 1);
        assert!(RateLimiter::admit(Some(&l), "r").is_ok());
        let s = RateLimiter::admit(Some(&l), "r").expect_err("limited");
        assert_eq!(s.code(), tonic::Code::ResourceExhausted);
        assert!(RateLimiter::admit(None, "r").is_ok(), "no limiter configured");
    }

    /// 测试用服务：请求在 `gate` 放行前不返回。
    #[derive(Clone)]
    struct Slow(Arc<Notify>);

    impl Service<http::Request<()>> for Slow {
        type Response = http::Response<String>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            let gate = self.0.clone();
            Box::pin(async move {
                gate.notified().await;
                Ok(http::Response::new("ok".to_string()))
            })
        }
    }

    fn request(path: &str) -> http::Request<()> {
        http::Request::builder().uri(path).body(()).expect("request")
    }

    #[tokio::test]
    async fn sheds_load_beyond_the_limit_but_not_health_checks() {
        let gate = Arc::new(Notify::new());
        let mut svc = ConcurrencyLimitLayer::new(1).layer(Slow(gate.clone()));
        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.expect("ready");
        let first = tokio::spawn(svc.call(request("/scion.v1.LandService/Land")));
        tokio::task::yield_now().await;
        // 第二个请求立即被拒绝
        let shed = svc.call(request("/scion.v1.LandService/Land")).await.expect("response");
        let status = Status::from_header_map(shed.headers()).expect("grpc status");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        // 健康检查不占名额
        let health = tokio::spawn(svc.call(request("/grpc.health.v1.Health/Check")));
        gate.notify_waiters();
        tokio::task::yield_now().await;
        gate.notify_waiters();
        assert_eq!(first.await.expect("join").expect("ok").into_body(), "ok");
        assert_eq!(health.await.expect("join").expect("ok").into_body(), "ok");
        // 名额释放后可以继续
        let next = tokio::spawn(svc.call(request("/scion.v1.LandService/Land")));
        tokio::task::yield_now().await;
        gate.notify_waiters();
        assert_eq!(next.await.expect("join").expect("ok").into_body(), "ok");
    }
}
