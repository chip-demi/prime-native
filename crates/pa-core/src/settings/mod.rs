//! Settings subsystem: typed settings documents, global/project storage,
//! legacy migrations, and the manager.

pub(crate) mod interactive_settings;
pub(crate) mod load;
pub(crate) mod manager;
pub(crate) mod merge;
pub(crate) mod storage;
pub(crate) mod types;

#[cfg(test)]
mod persistence_tests;

pub use manager::{
    IdleEviction, SessionArchivePolicy, SettingsError, SettingsManager,
    DEFAULT_IDLE_EVICTION_MINUTES, DEFAULT_SESSION_ARCHIVE_MAX_AGE_DAYS,
    DEFAULT_SESSION_ARCHIVE_MAX_SESSIONS,
};
pub use storage::{
    FileSettingsStorage, InMemorySettingsStorage, SettingsScope, SettingsStorage, CONFIG_DIR_NAME,
};
pub use types::{
    AutoRefineSettings, AutonomousSettings, CompactionSettings, McpServerConfig, QueueModeSetting,
    Settings, ThinkingLevelSetting, TransportSetting, UpdateChannel,
};

#[cfg(test)]
mod legacy_public_api_tests {
    #[test]
    fn settings_mcp_server_config_preserves_legacy_json_shape() {
        use super::McpServerConfig;
        let value = serde_json::json!({"type":"http", "url":"https://example.test", "headers":{"custom":4}, "bearer_token_env_var":"TOKEN"});
        let config: McpServerConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(config).unwrap(), value);
    }
}
