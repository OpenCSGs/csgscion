//! 身份：当前身份、OIDC device login（`scn login`）。

use scion_proto::v1::auth_service_client::AuthServiceClient;
use scion_proto::v1::{DeviceLoginState, PollDeviceLoginRequest, StartDeviceLoginRequest, WhoAmIRequest};

use crate::client::Client;
use crate::error::{Error, Result};

/// 当前身份。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// 未认证时为 `anonymous`。
    pub subject: String,
    pub groups: Vec<String>,
    /// 服务端是否要求认证。
    pub auth_required: bool,
    /// 访问令牌的过期时间（Unix 毫秒）。
    pub expires_at_ms: Option<i64>,
}

/// device login 的第一步：让用户在浏览器中打开 `verification_uri` 并输入 `user_code`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceLogin {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
    pub interval_seconds: u32,
    pub expires_at_ms: i64,
}

/// 轮询结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceLoginPoll {
    Pending,
    SlowDown,
    Denied,
    Expired,
    /// 完成：`refresh_token` 是需要保存的长期凭据。
    Complete {
        subject: String,
        refresh_token: String,
    },
}

impl Client {
    pub async fn who_am_i(&self) -> Result<Identity> {
        let r = AuthServiceClient::new(self.channel.clone())
            .who_am_i(self.request(WhoAmIRequest {})?)
            .await?
            .into_inner();
        Ok(Identity {
            subject: r.subject,
            groups: r.groups,
            auth_required: r.auth_required,
            expires_at_ms: (r.expires_at_ms > 0).then_some(r.expires_at_ms),
        })
    }

    pub async fn start_device_login(&self) -> Result<DeviceLogin> {
        let r = AuthServiceClient::new(self.channel.clone())
            .start_device_login(self.request(StartDeviceLoginRequest {})?)
            .await?
            .into_inner();
        Ok(DeviceLogin {
            device_code: r.device_code,
            user_code: r.user_code,
            verification_uri: r.verification_uri,
            verification_uri_complete: Some(r.verification_uri_complete).filter(|u| !u.is_empty()),
            interval_seconds: r.interval_seconds.max(1),
            expires_at_ms: r.expires_at_ms,
        })
    }

    pub async fn poll_device_login(&self, device_code: &str) -> Result<DeviceLoginPoll> {
        let r = AuthServiceClient::new(self.channel.clone())
            .poll_device_login(self.request(PollDeviceLoginRequest {
                device_code: device_code.to_owned(),
            })?)
            .await?
            .into_inner();
        Ok(match DeviceLoginState::try_from(r.state) {
            Ok(DeviceLoginState::Pending) => DeviceLoginPoll::Pending,
            Ok(DeviceLoginState::SlowDown) => DeviceLoginPoll::SlowDown,
            Ok(DeviceLoginState::Denied) => DeviceLoginPoll::Denied,
            Ok(DeviceLoginState::Expired) => DeviceLoginPoll::Expired,
            Ok(DeviceLoginState::Complete) if !r.refresh_token.is_empty() => DeviceLoginPoll::Complete {
                subject: r.subject,
                refresh_token: r.refresh_token,
            },
            _ => return Err(Error::Protocol(format!("unexpected device login state {}", r.state))),
        })
    }
}
