//! The built-in subscription providers' OAuth integration: the stored `openai-codex`, `anthropic`,
//! `github-copilot`, `xai`, and `muse-code` credentials refresh at their token endpoints when they expire; every
//! other provider serves its access token until expiry.

use std::sync::Arc;

use pa_ai::oauth::{
    refresh_anthropic_token, refresh_github_copilot_token, refresh_openai_codex_token,
    refresh_xai_token, CodexHttp, ProviderHttp, ProviderHttpMethod, ProviderHttpRequest,
    ReqwestCodexHttp, ReqwestProviderHttp,
};

use crate::auth::types::{AuthCredential, AuthStorageData};

/// The Codex Subscription provider id (a wire identifier; branding
/// never renames a provider id).
pub const OPENAI_CODEX_PROVIDER_ID: &str = "openai-codex";
/// The Anthropic (Claude Pro/Max) subscription provider id.
pub const ANTHROPIC_PROVIDER_ID: &str = "anthropic";
pub const GITHUB_COPILOT_PROVIDER_ID: &str = "github-copilot";
/// The xAI (Grok) subscription provider id.
pub const XAI_PROVIDER_ID: &str = "xai";
pub(crate) const MUSE_CODE_PROVIDER_ID: &str = "muse-code";
const MUSE_KEY_URL: &str = "https://api.meta.ai/muse-code/key";
const MUSE_KEY_LIFETIME_MS: i64 = 24 * 60 * 60 * 1000;

fn clean_muse_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 8192
        && !token
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
}

fn muse_access_tokens(access: &str) -> Option<(String, String)> {
    if access.is_empty() || access.len() > 16_384 {
        return None;
    }
    let compound: serde_json::Value = serde_json::from_str(access).ok()?;
    let identity = compound.get("oauthAccessToken")?.as_str()?;
    let minted = compound.get("apiKey")?.as_str()?;
    (clean_muse_token(identity) && clean_muse_token(minted))
        .then(|| (identity.to_string(), minted.to_string()))
}

/// Only a typed Muse OAuth credential can supply an inference key. The
/// identity token and serialized compound value must never become a bearer.
pub(crate) fn muse_inference_key(credential: &AuthCredential) -> Option<String> {
    let AuthCredential::Oauth { access, .. } = credential else {
        return None;
    };
    muse_access_tokens(access).map(|(_, minted)| minted)
}

/// The AI library's oauth registry as the auth storage's default
/// integration.
pub struct ProviderOAuth {
    http: Arc<dyn CodexHttp>,
    provider_http: Arc<dyn ProviderHttp>,
}

impl Default for ProviderOAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderOAuth {
    #[must_use]
    pub fn new() -> Self {
        ProviderOAuth {
            http: Arc::new(ReqwestCodexHttp::new()),
            provider_http: Arc::new(ReqwestProviderHttp::new()),
        }
    }

    /// Fixed transports (tests and embedded hosts).
    pub fn with_transports(http: Arc<dyn CodexHttp>, provider_http: Arc<dyn ProviderHttp>) -> Self {
        ProviderOAuth {
            http,
            provider_http,
        }
    }

    /// A fixed Codex transport plus the production provider transport.
    pub fn with_http(http: Arc<dyn CodexHttp>) -> Self {
        ProviderOAuth {
            http,
            provider_http: Arc::new(ReqwestProviderHttp::new()),
        }
    }

    /// Refresh one expired credential off the async runtime (the
    /// `AuthStorage` seam is synchronous by contract).
    fn refresh_blocking(
        &self,
        provider_id: &str,
        credential: &AuthCredential,
    ) -> Option<AuthCredential> {
        let AuthCredential::Oauth {
            access,
            refresh,
            enterprise_url,
            ..
        } = credential
        else {
            return None;
        };
        let refresh_token = if provider_id == MUSE_CODE_PROVIDER_ID {
            match refresh {
                Some(token) => clean_muse_token(token).then(|| token.clone())?,
                None => muse_access_tokens(access).map(|(identity, _)| identity)?,
            }
        } else {
            refresh.clone()?
        };
        let http = Arc::clone(&self.http);
        let provider_http = Arc::clone(&self.provider_http);
        let enterprise_url = enterprise_url.clone();
        let provider_id = provider_id.to_string();
        std::thread::Builder::new()
            .name("provider-oauth-refresh".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .ok()?;
                let refreshed: Option<AuthCredential> = match provider_id.as_str() {
                    OPENAI_CODEX_PROVIDER_ID => {
                        let credentials = runtime
                            .block_on(refresh_openai_codex_token(http.as_ref(), &refresh_token))
                            .ok()?;
                        Some(AuthCredential::Oauth {
                            access: credentials.access,
                            refresh: Some(credentials.refresh),
                            expires: credentials.expires,
                            account_id: Some(credentials.account_id),
                            enterprise_url: None,
                            endpoint: None,
                            token_endpoint: None,
                            client_id: None,
                            resource: None,
                            issuer: None,
                            audience_mode: None,
                        })
                    }
                    ANTHROPIC_PROVIDER_ID => {
                        let credentials = runtime
                            .block_on(refresh_anthropic_token(
                                provider_http.as_ref(),
                                &refresh_token,
                            ))
                            .ok()?;
                        Some(AuthCredential::Oauth {
                            access: credentials.access,
                            refresh: Some(credentials.refresh),
                            expires: credentials.expires,
                            account_id: None,
                            enterprise_url: None,
                            endpoint: None,
                            token_endpoint: None,
                            client_id: None,
                            resource: None,
                            issuer: None,
                            audience_mode: None,
                        })
                    }
                    GITHUB_COPILOT_PROVIDER_ID => {
                        // The stored GitHub token exchanges for a fresh Copilot token; the
                        // enterprise domain rides the credential.
                        let credentials = runtime
                            .block_on(refresh_github_copilot_token(
                                provider_http.as_ref(),
                                &refresh_token,
                                enterprise_url.as_deref(),
                            ))
                            .ok()?;
                        Some(AuthCredential::Oauth {
                            access: credentials.access,
                            refresh: Some(credentials.refresh),
                            expires: credentials.expires,
                            account_id: None,
                            enterprise_url: credentials.enterprise_url,
                            endpoint: None,
                            token_endpoint: None,
                            client_id: None,
                            resource: None,
                            issuer: None,
                            audience_mode: None,
                        })
                    }
                    XAI_PROVIDER_ID => {
                        let credentials = runtime
                            .block_on(refresh_xai_token(provider_http.as_ref(), &refresh_token))
                            .ok()?;
                        Some(AuthCredential::Oauth {
                            access: credentials.access,
                            refresh: Some(credentials.refresh),
                            expires: credentials.expires,
                            account_id: None,
                            enterprise_url: None,
                            endpoint: None,
                            token_endpoint: None,
                            client_id: None,
                            resource: None,
                            issuer: None,
                            audience_mode: None,
                        })
                    }
                    MUSE_CODE_PROVIDER_ID => {
                        let response = runtime
                            .block_on(provider_http.request(
                                ProviderHttpRequest {
                                    method: ProviderHttpMethod::Post,
                                    url: MUSE_KEY_URL.to_string(),
                                    headers: vec![
                                        ("Accept".to_string(), "application/json".to_string()),
                                        (
                                            "Content-Type".to_string(),
                                            "application/json".to_string(),
                                        ),
                                        ("x-api-version".to_string(), "1.0.0".to_string()),
                                        (
                                            "Authorization".to_string(),
                                            format!("Bearer {refresh_token}"),
                                        ),
                                    ],
                                    body: Some("{}".to_string()),
                                    follow_redirects: false,
                                },
                                20_000,
                            ))
                            .ok()?;
                        if !response.ok() {
                            return None;
                        }
                        let body: serde_json::Value = serde_json::from_str(&response.body).ok()?;
                        if body.get("is_subs_active")?.as_bool() != Some(true)
                            || body
                                .get("require_payment")
                                .and_then(serde_json::Value::as_bool)
                                == Some(true)
                            || body
                                .get("action_url")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|url| !url.trim().is_empty())
                            || body
                                .get("require_payment_action_url")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|url| !url.trim().is_empty())
                        {
                            return None;
                        }
                        let minted = body.get("api_key")?.as_str()?;
                        if !clean_muse_token(minted) {
                            return None;
                        }
                        let account_id = body
                            .get("user_id")
                            .and_then(serde_json::Value::as_str)
                            .filter(|id| !id.trim().is_empty())
                            .map(|id| id.trim().to_string())
                            .or_else(|| {
                                body.get("user_email")
                                    .and_then(serde_json::Value::as_str)
                                    .filter(|email| !email.trim().is_empty())
                                    .map(|email| email.trim().to_lowercase())
                            })?;
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .ok()?
                            .as_millis() as i64;
                        Some(AuthCredential::Oauth {
                            access: serde_json::json!({
                                "oauthAccessToken": refresh_token,
                                "apiKey": minted,
                            })
                            .to_string(),
                            refresh: Some(refresh_token),
                            expires: now_ms.saturating_add(MUSE_KEY_LIFETIME_MS),
                            account_id: Some(account_id),
                            enterprise_url: None,
                            endpoint: None,
                            token_endpoint: None,
                            client_id: None,
                            resource: None,
                            issuer: None,
                            audience_mode: None,
                        })
                    }
                    // The match arms cover the supported subscription ids;
                    // `refresh` never dispatches another.
                    _ => None,
                };
                refreshed
            })
            .ok()?
            .join()
            .ok()?
    }
}

impl crate::auth::OAuthIntegration for ProviderOAuth {
    fn api_key_for(&self, provider: &str, credential: &AuthCredential) -> Option<String> {
        if provider == MUSE_CODE_PROVIDER_ID {
            return muse_inference_key(credential);
        }
        match credential {
            AuthCredential::Oauth { access, .. } => Some(access.clone()),
            AuthCredential::ApiKey { .. } | AuthCredential::McpStaticToken { .. } => None,
        }
    }

    fn refresh(&self, provider_id: &str, credentials: &AuthStorageData) -> Option<AuthCredential> {
        if !matches!(
            provider_id,
            OPENAI_CODEX_PROVIDER_ID
                | ANTHROPIC_PROVIDER_ID
                | GITHUB_COPILOT_PROVIDER_ID
                | XAI_PROVIDER_ID
                | MUSE_CODE_PROVIDER_ID
        ) {
            return None;
        }
        let credential = credentials.credential(provider_id)?;
        self.refresh_blocking(provider_id, &credential)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pa_ai::oauth::{CodexHttpResponse, ProviderHttpRequest, ProviderHttpResponse};

    use super::*;
    use crate::auth::manager::OAuthIntegration;

    /// A scripted Codex transport (url -> response); unknown urls fail
    /// the request.
    struct ScriptedHttp(HashMap<String, CodexHttpResponse>);

    impl CodexHttp for ScriptedHttp {
        fn post_form<'a>(
            &'a self,
            url: &'a str,
            _body: &'a str,
            _timeout_ms: u64,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<CodexHttpResponse, String>> + Send + 'a>,
        > {
            let response = self.0.get(url).cloned();
            Box::pin(async move { response.ok_or_else(|| format!("{url} was not scripted")) })
        }
    }

    /// A scripted provider transport (url -> response); unknown urls
    /// fail the request.
    struct ScriptedProviderHttp(HashMap<String, ProviderHttpResponse>);

    impl ProviderHttp for ScriptedProviderHttp {
        fn request(
            &self,
            request: ProviderHttpRequest,
            _timeout_ms: u64,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ProviderHttpResponse, String>> + Send + '_>,
        > {
            let response = self.0.get(&request.url).cloned();
            Box::pin(
                async move { response.ok_or_else(|| format!("{} was not scripted", request.url)) },
            )
        }
    }

    /// One fake access token carrying the account id (no signature
    /// verified, like the TS decode).
    fn account_jwt(account_id: &str) -> String {
        use base64::Engine as _;
        let segment = |value: &serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_string(value).unwrap().as_bytes())
        };
        format!(
            "{}.{}.not-a-signature",
            segment(&serde_json::json!({"alg": "RS256"})),
            segment(&serde_json::json!({
                "https://api.openai.com/auth": {"chatgpt_account_id": account_id}
            }))
        )
    }

    fn expired_codex_credential() -> AuthCredential {
        AuthCredential::Oauth {
            access: "stale-access".to_string(),
            refresh: Some("r-old".to_string()),
            expires: 1,
            account_id: Some("acct-1".to_string()),
            enterprise_url: None,
            endpoint: None,
            token_endpoint: None,
            client_id: None,
            resource: None,
            issuer: None,
            audience_mode: None,
        }
    }

    /// One expired subscription credential for a provider id.
    fn expired_credential(provider_id: &str) -> AuthCredential {
        match provider_id {
            GITHUB_COPILOT_PROVIDER_ID => AuthCredential::Oauth {
                access: "stale-copilot".to_string(),
                refresh: Some("gh-old".to_string()),
                expires: 1,
                account_id: None,
                enterprise_url: Some("company.ghe.com".to_string()),
                endpoint: None,
                token_endpoint: None,
                client_id: None,
                resource: None,
                issuer: None,
                audience_mode: None,
            },
            _ => AuthCredential::Oauth {
                access: "stale-access".to_string(),
                refresh: Some("r-old".to_string()),
                expires: 1,
                account_id: None,
                enterprise_url: None,
                endpoint: None,
                token_endpoint: None,
                client_id: None,
                resource: None,
                issuer: None,
                audience_mode: None,
            },
        }
    }

    /// The scripted endpoints for the four subscription refreshes.
    fn integrations() -> std::sync::Arc<ProviderOAuth> {
        let mut codex = HashMap::new();
        codex.insert(
            "https://auth.openai.com/oauth/token".to_string(),
            CodexHttpResponse {
                status: 200,
                body: serde_json::json!({
                    "access_token": account_jwt("acct-2"),
                    "refresh_token": "r-new",
                    "expires_in": 3600,
                })
                .to_string(),
            },
        );
        let mut providers = HashMap::new();
        providers.insert(
            "https://platform.claude.com/v1/oauth/token".to_string(),
            ProviderHttpResponse {
                status: 200,
                body: serde_json::json!({
                    "access_token": "anthropic-access",
                    "refresh_token": "anthropic-refresh",
                    "expires_in": 3600,
                })
                .to_string(),
            },
        );
        providers.insert(
            "https://api.company.ghe.com/copilot_internal/v2/token".to_string(),
            ProviderHttpResponse {
                status: 200,
                body: serde_json::json!({
                    "token": "copilot-access",
                    "expires_at": 4_000_000_000i64,
                })
                .to_string(),
            },
        );
        providers.insert(
            "https://auth.x.ai/oauth2/token".to_string(),
            ProviderHttpResponse {
                status: 200,
                body: serde_json::json!({
                    "access_token": "grok-access",
                    "refresh_token": "grok-refresh",
                    "expires_in": 3600,
                })
                .to_string(),
            },
        );
        std::sync::Arc::new(ProviderOAuth::with_transports(
            std::sync::Arc::new(ScriptedHttp(codex)),
            std::sync::Arc::new(ScriptedProviderHttp(providers)),
        ))
    }

    fn storage_with_credential(
        provider_id: &str,
        credential: &AuthCredential,
    ) -> crate::auth::AuthStorage {
        let mut data = crate::auth::types::AuthStorageData::default();
        data.insert(provider_id, credential);
        crate::auth::AuthStorage::in_memory_without_env(&data, integrations())
    }

    #[test]
    fn an_expired_codex_credential_refreshes_and_resolves() {
        let mut auth =
            storage_with_credential(OPENAI_CODEX_PROVIDER_ID, &expired_codex_credential());
        let api_key = auth
            .get_api_key(OPENAI_CODEX_PROVIDER_ID)
            .expect("the refreshed access token resolves");
        assert_eq!(api_key, account_jwt("acct-2"));
        let stored = auth.get_all().credential(OPENAI_CODEX_PROVIDER_ID).unwrap();
        let AuthCredential::Oauth {
            access,
            refresh,
            expires,
            account_id,
            ..
        } = stored
        else {
            panic!("the stored credential is OAuth");
        };
        assert_eq!(access, account_jwt("acct-2"));
        assert_eq!(refresh.as_deref(), Some("r-new"));
        assert!(expires > 1, "the fresh expiry landed");
        assert_eq!(account_id.as_deref(), Some("acct-2"));
    }

    #[test]
    fn an_expired_anthropic_credential_refreshes_and_resolves() {
        let mut auth = storage_with_credential(
            ANTHROPIC_PROVIDER_ID,
            &expired_credential(ANTHROPIC_PROVIDER_ID),
        );
        let api_key = auth
            .get_api_key(ANTHROPIC_PROVIDER_ID)
            .expect("the refreshed access token resolves");
        assert_eq!(api_key, "anthropic-access");
        let stored = auth.get_all().credential(ANTHROPIC_PROVIDER_ID).unwrap();
        let AuthCredential::Oauth {
            refresh, expires, ..
        } = stored
        else {
            panic!("the stored credential is OAuth");
        };
        assert_eq!(refresh.as_deref(), Some("anthropic-refresh"));
        assert!(expires > 1, "the fresh expiry landed");
    }

    #[test]
    fn an_expired_copilot_credential_refreshes_through_the_enterprise_domain() {
        let mut auth = storage_with_credential(
            GITHUB_COPILOT_PROVIDER_ID,
            &expired_credential(GITHUB_COPILOT_PROVIDER_ID),
        );
        let api_key = auth
            .get_api_key(GITHUB_COPILOT_PROVIDER_ID)
            .expect("the refreshed Copilot token resolves");
        assert_eq!(api_key, "copilot-access");
        let stored = auth
            .get_all()
            .credential(GITHUB_COPILOT_PROVIDER_ID)
            .unwrap();
        let AuthCredential::Oauth {
            refresh,
            expires,
            enterprise_url,
            ..
        } = stored
        else {
            panic!("the stored credential is OAuth");
        };
        // The GitHub token stays the refresh source; the enterprise
        // domain rides the credential.
        assert_eq!(refresh.as_deref(), Some("gh-old"));
        assert_eq!(enterprise_url.as_deref(), Some("company.ghe.com"));
        assert!(expires > 1, "the fresh expiry landed");
    }

    #[test]
    fn an_expired_xai_credential_refreshes_and_resolves() {
        let mut auth =
            storage_with_credential(XAI_PROVIDER_ID, &expired_credential(XAI_PROVIDER_ID));
        let api_key = auth
            .get_api_key(XAI_PROVIDER_ID)
            .expect("the refreshed access token resolves");
        assert_eq!(api_key, "grok-access");
        let stored = auth.get_all().credential(XAI_PROVIDER_ID).unwrap();
        let AuthCredential::Oauth {
            refresh, expires, ..
        } = stored
        else {
            panic!("the stored credential is OAuth");
        };
        assert_eq!(refresh.as_deref(), Some("grok-refresh"));
        assert!(expires > 1, "the fresh expiry landed");
    }

    #[test]
    fn a_failed_refresh_keeps_the_stored_credential() {
        // Nothing scripted: the refresh fails, the stored credential stays.
        let mut auth = crate::auth::AuthStorage::in_memory_without_env(
            &{
                let mut data = crate::auth::types::AuthStorageData::default();
                data.insert(OPENAI_CODEX_PROVIDER_ID, &expired_codex_credential());
                data
            },
            std::sync::Arc::new(ProviderOAuth::with_transports(
                std::sync::Arc::new(ScriptedHttp(HashMap::new())),
                std::sync::Arc::new(ScriptedProviderHttp(HashMap::new())),
            )),
        );
        assert_eq!(auth.get_api_key(OPENAI_CODEX_PROVIDER_ID), None);
        assert!(
            auth.get_all()
                .credential(OPENAI_CODEX_PROVIDER_ID)
                .is_some(),
            "the failed refresh keeps the stored credential"
        );
    }

    #[test]
    fn non_subscription_providers_do_not_refresh() {
        let mut auth = storage_with_credential("other-oauth", &expired_credential("other-oauth"));
        assert_eq!(auth.get_api_key("other-oauth"), None);
        assert!(auth.get_all().credential("other-oauth").is_some());
    }

    #[test]
    fn an_unexpired_credential_serves_without_a_refresh() {
        let unexpired = AuthCredential::Oauth {
            access: "live-access".to_string(),
            refresh: Some("r".to_string()),
            expires: i64::MAX,
            account_id: Some("acct-1".to_string()),
            enterprise_url: None,
            endpoint: None,
            token_endpoint: None,
            client_id: None,
            resource: None,
            issuer: None,
            audience_mode: None,
        };
        let mut auth = storage_with_credential(OPENAI_CODEX_PROVIDER_ID, &unexpired);
        assert_eq!(
            auth.get_api_key(OPENAI_CODEX_PROVIDER_ID),
            Some("live-access".to_string())
        );
    }

    #[test]
    fn muse_compound_oauth_never_returns_the_account_token() {
        let oauth: AuthCredential = serde_json::from_value(serde_json::json!({
            "type": "oauth",
            "access": r#"{"oauthAccessToken":"account-token","apiKey":"inference-key"}"#,
            "expires": i64::MAX,
        }))
        .unwrap();
        let integration = integrations();
        assert_eq!(
            integration.api_key_for(MUSE_CODE_PROVIDER_ID, &oauth),
            Some("inference-key".to_string())
        );
        let malformed: AuthCredential = serde_json::from_value(serde_json::json!({
            "type": "oauth", "access": "account-token", "expires": i64::MAX,
        }))
        .unwrap();
        assert_eq!(
            integration.api_key_for(MUSE_CODE_PROVIDER_ID, &malformed),
            None
        );
        let wrong_type = AuthCredential::ApiKey {
            key: "account-token".to_string(),
            prime_team: None,
        };
        assert_eq!(
            integration.api_key_for(MUSE_CODE_PROVIDER_ID, &wrong_type),
            None
        );
    }

    #[test]
    fn expired_muse_renews_only_at_its_key_endpoint_then_serves_minted_key() {
        struct CaptureMuseHttp {
            seen: std::sync::Mutex<Vec<ProviderHttpRequest>>,
        }
        impl ProviderHttp for CaptureMuseHttp {
            fn request(
                &self,
                request: ProviderHttpRequest,
                _timeout_ms: u64,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<ProviderHttpResponse, String>>
                        + Send
                        + '_,
                >,
            > {
                self.seen.lock().unwrap().push(request);
                Box::pin(async {
                    Ok(ProviderHttpResponse {
                        status: 200,
                        body: serde_json::json!({
                            "is_subs_active": true,
                            "require_payment": false,
                            "api_key": "fresh-inference-key",
                            "user_id": "account-1",
                        })
                        .to_string(),
                    })
                })
            }
        }
        let capture = Arc::new(CaptureMuseHttp {
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let integration = Arc::new(ProviderOAuth::with_transports(
            Arc::new(ScriptedHttp(HashMap::new())),
            capture.clone(),
        ));
        let expired: AuthCredential = serde_json::from_value(serde_json::json!({
            "type": "oauth",
            "access": r#"{"oauthAccessToken":"account-token","apiKey":"old-inference-key"}"#,
            "refresh": "account-token",
            "expires": 1,
        }))
        .unwrap();
        let mut data = AuthStorageData::default();
        data.insert(MUSE_CODE_PROVIDER_ID, &expired);
        data.0
            .get_mut(MUSE_CODE_PROVIDER_ID)
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .insert("email".to_string(), serde_json::json!("member@example.com"));
        let mut auth = crate::auth::AuthStorage::in_memory_without_env(&data, integration);
        assert_eq!(
            auth.get_api_key(MUSE_CODE_PROVIDER_ID),
            Some("fresh-inference-key".to_string())
        );
        let refreshed = auth.get_all().credential(MUSE_CODE_PROVIDER_ID).unwrap();
        assert_eq!(
            muse_inference_key(&refreshed).as_deref(),
            Some("fresh-inference-key")
        );
        assert_eq!(
            auth.get_all().get(MUSE_CODE_PROVIDER_ID).unwrap()["email"],
            "member@example.com"
        );
        let AuthCredential::Oauth { expires, .. } = refreshed else {
            panic!("Muse refresh must persist OAuth");
        };
        assert!(expires > 1);
        let requests = capture.seen.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, MUSE_KEY_URL);
        assert_eq!(requests[0].body.as_deref(), Some("{}"));
        assert!(!requests[0].follow_redirects);
        assert!(requests[0].headers.contains(&(
            "Authorization".to_string(),
            "Bearer account-token".to_string()
        )));
        assert!(!requests[0]
            .headers
            .iter()
            .any(|(_, value)| value.contains("old-inference-key")));
    }

    #[test]
    fn inactive_muse_subscription_does_not_disclose_or_persist_a_key() {
        let mut responses = HashMap::new();
        responses.insert(
            MUSE_KEY_URL.to_string(),
            ProviderHttpResponse {
                status: 200,
                body: serde_json::json!({
                    "is_subs_active": false,
                    "api_key": "must-not-be-served",
                    "user_id": "account-1",
                })
                .to_string(),
            },
        );
        let integration = Arc::new(ProviderOAuth::with_transports(
            Arc::new(ScriptedHttp(HashMap::new())),
            Arc::new(ScriptedProviderHttp(responses)),
        ));
        let expired: AuthCredential = serde_json::from_value(serde_json::json!({
            "type": "oauth",
            "access": r#"{"oauthAccessToken":"account-token","apiKey":"old-inference-key"}"#,
            "refresh": "account-token",
            "expires": 1,
        }))
        .unwrap();
        let mut data = AuthStorageData::default();
        data.insert(MUSE_CODE_PROVIDER_ID, &expired);
        let mut auth = crate::auth::AuthStorage::in_memory_without_env(&data, integration);
        assert_eq!(auth.get_api_key(MUSE_CODE_PROVIDER_ID), None);
        assert_eq!(
            auth.get_all().credential(MUSE_CODE_PROVIDER_ID),
            Some(expired)
        );
    }
}
