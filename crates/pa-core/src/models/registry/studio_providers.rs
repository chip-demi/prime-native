//! Studio-only Meta and Muse Code defaults. These never change the CLI catalog
//! unless the Studio launcher opts in, and never override a user provider entry.

use std::path::Path;

use pa_types::ai::Model;

use crate::auth::provider_oauth::{muse_inference_key, MUSE_CODE_PROVIDER_ID};
use crate::auth::types::AuthCredential;

use super::super::custom::parse_models_config;

const META_PROVIDER_ID: &str = "meta";

// The definitions mirror Studio's meta-provider.mjs and muse-oauth-provider.mjs.
// The Muse subscription route has zero metered cost; Meta's paid API does not.
const META_MODELS: &str = r#"[
  {"id":"muse-spark-1.3","name":"Muse Spark 1.3","api":"openai-responses","provider":"meta","baseUrl":"https://api.meta.ai/v1","reasoning":true,"thinkingLevelMap":{"off":null,"minimal":"minimal","low":"low","medium":"medium","high":"high","xhigh":"xhigh","max":"max"},"input":["text","image"],"cost":{"input":1.25,"output":4.25,"cacheRead":0.15,"cacheWrite":0},"contextWindow":1048576,"maxTokens":131072,"maxTokensExplicit":true},
  {"id":"muse-spark-1.3-contributor","name":"Muse Spark 1.3 Contributor (training allowed)","api":"openai-responses","provider":"meta","baseUrl":"https://api.meta.ai/v1","reasoning":true,"thinkingLevelMap":{"off":null,"minimal":"minimal","low":"low","medium":"medium","high":"high","xhigh":"xhigh"},"input":["text","image"],"cost":{"input":0.1,"output":0.2,"cacheRead":0.002,"cacheWrite":0},"contextWindow":1048576,"maxTokens":131072,"maxTokensExplicit":true}
]"#;

const MUSE_MODELS: &str = r#"[
  {"id":"muse-spark-1.3","name":"Muse Spark 1.3","api":"openai-responses","provider":"muse-code","baseUrl":"https://api.meta.ai/v1","reasoning":true,"thinkingLevelMap":{"off":null,"minimal":"minimal","low":"low","medium":"medium","high":"high","xhigh":"xhigh","max":"max"},"input":["text","image"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":1048576,"maxTokens":131072,"maxTokensExplicit":true,"headers":{"x-api-version":"1.0.0"}},
  {"id":"muse-spark-1.3-contributor","name":"Muse Spark 1.3 Contributor (training allowed)","api":"openai-responses","provider":"muse-code","baseUrl":"https://api.meta.ai/v1","reasoning":true,"thinkingLevelMap":{"off":null,"minimal":"minimal","low":"low","medium":"medium","high":"high","xhigh":"xhigh"},"input":["text","image"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":1048576,"maxTokens":131072,"maxTokensExplicit":true,"headers":{"x-api-version":"1.0.0"}}
]"#;

pub(super) struct StudioProviderDefaults {
    pub models: Vec<Model>,
    pub canonical_muse: bool,
}

pub(super) fn studio_provider_defaults(
    enabled: bool,
    models_json_path: Option<&Path>,
    built_in: &[Model],
    muse_credential: Option<&AuthCredential>,
) -> StudioProviderDefaults {
    let mut defaults = StudioProviderDefaults {
        models: Vec::new(),
        canonical_muse: false,
    };
    if !enabled {
        return defaults;
    }
    let config = match models_json_path {
        Some(path) if path.exists() => {
            let Ok(content) = std::fs::read_to_string(path) else {
                return defaults;
            };
            let Ok(config) = parse_models_config(&content) else {
                return defaults;
            };
            Some(config)
        }
        _ => None,
    };
    let has_user = |provider: &str| {
        config
            .as_ref()
            .is_some_and(|config| config.providers.contains_key(provider))
    };
    let has_builtin = |provider: &str| built_in.iter().any(|model| model.provider == provider);
    if !has_user(META_PROVIDER_ID) && !has_builtin(META_PROVIDER_ID) {
        defaults.models.extend(
            serde_json::from_str::<Vec<Model>>(META_MODELS).expect("canonical Meta models"),
        );
    }
    if !has_user(MUSE_CODE_PROVIDER_ID)
        && !has_builtin(MUSE_CODE_PROVIDER_ID)
        && muse_credential.and_then(muse_inference_key).is_some()
    {
        defaults.models.extend(
            serde_json::from_str::<Vec<Model>>(MUSE_MODELS).expect("canonical Muse Code models"),
        );
        defaults.canonical_muse = true;
    }
    defaults
}

#[cfg(test)]
mod tests {
    use super::*;

    fn muse_oauth() -> AuthCredential {
        serde_json::from_value(serde_json::json!({
            "type": "oauth",
            "access": r#"{"oauthAccessToken":"identity-token","apiKey":"minted-key"}"#,
            "refresh": "identity-token",
            "expires": i64::MAX,
        }))
        .unwrap()
    }

    #[test]
    fn studio_defaults_require_opt_in_and_real_muse_oauth() {
        let credential = muse_oauth();
        assert!(
            studio_provider_defaults(false, None, &[], Some(&credential))
                .models
                .is_empty()
        );

        let defaulted = studio_provider_defaults(true, None, &[], Some(&credential));
        assert_eq!(defaulted.models.len(), 4);
        assert!(defaulted.canonical_muse);
        let meta = defaulted
            .models
            .iter()
            .find(|model| model.provider == "meta" && model.id == "muse-spark-1.3")
            .unwrap();
        assert_eq!(meta.base_url, "https://api.meta.ai/v1");
        assert!(meta.reasoning);
        assert!(meta.input.contains(&pa_types::ai::ModelInput::Image));
        assert_eq!(
            meta.thinking_level_map
                .as_ref()
                .and_then(|map| map.get(&pa_types::ai::ModelThinkingLevel::Off)),
            Some(&None)
        );

        let key_only: AuthCredential = serde_json::from_value(serde_json::json!({
            "type": "api_key", "key": "paid-meta-key"
        }))
        .unwrap();
        let no_muse = studio_provider_defaults(true, None, &[], Some(&key_only));
        assert!(!no_muse.canonical_muse);
        assert_eq!(no_muse.models.len(), 2);
    }

    #[test]
    fn jsonc_provider_entry_suppresses_only_that_default_and_bad_file_suppresses_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("models.json");
        std::fs::write(
            &path,
            r#"{ // explicit provider, including a model-less entry
                "providers": {"meta": {},}
            }"#,
        )
        .unwrap();
        let credential = muse_oauth();
        let defaults = studio_provider_defaults(true, Some(&path), &[], Some(&credential));
        assert_eq!(defaults.models.len(), 2);
        assert!(defaults
            .models
            .iter()
            .all(|model| model.provider == "muse-code"));

        std::fs::write(
            &path,
            r#"{"providers":{"muse-code":{"baseUrl":"https://custom.example"}}}"#,
        )
        .unwrap();
        let defaults = studio_provider_defaults(true, Some(&path), &[], Some(&credential));
        assert_eq!(defaults.models.len(), 2);
        assert!(defaults.models.iter().all(|model| model.provider == "meta"));
        assert!(!defaults.canonical_muse);

        std::fs::write(&path, "{broken").unwrap();
        let defaults = studio_provider_defaults(true, Some(&path), &[], Some(&credential));
        assert!(defaults.models.is_empty());
        assert!(!defaults.canonical_muse);
    }
}
