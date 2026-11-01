//! Studio's opt-in, request-only compression of large Python namespace notices.
//! The transform receives a cloned context snapshot; persisted rows stay intact.

use std::{fmt::Write as _, sync::Arc};

use pa_agent::agent_loop::TransformContextFn;
use pa_agent::types::AgentMessage;

const MAX_NAMES: usize = 40;

fn enabled() -> bool {
    std::env::var("PRIME_STUDIO_CONTEXT_HYGIENE").is_ok_and(|value| value == "1")
}

pub(super) fn transform_context() -> TransformContextFn {
    Arc::new(|messages, _signal| {
        Box::pin(async move { Ok(transform_messages(messages, enabled())) })
    })
}

fn transform_messages(mut messages: Vec<AgentMessage>, enabled: bool) -> Vec<AgentMessage> {
    if enabled {
        for message in &mut messages {
            let AgentMessage::Custom(custom) = message else {
                continue;
            };
            let kind = custom
                .payload
                .get("customType")
                .and_then(serde_json::Value::as_str);
            if !matches!(kind, Some("ipython_state" | "ipython_state_restored")) {
                continue;
            }
            let Some(content) = custom.payload.get_mut("content") else {
                continue;
            };
            if let Some(text) = content.as_str() {
                let shortened = shorten_name_lists(text);
                if shortened != text {
                    *content = serde_json::Value::String(shortened);
                }
            }
        }
    }
    messages
}

/// Matches the two namespace sentences emitted by native notices. A candidate
/// is changed only when every comma-separated item is an ASCII identifier.
fn shorten_name_lists(text: &str) -> String {
    let mut output = text.to_string();
    for (prefix, state) in [
        ("These names are still defined: ", "still defined"),
        ("These names are available again: ", "available again"),
    ] {
        // This pass may leave the other phrase; the outer loop handles both.
        let mut remaining = output.as_str();
        let mut next = String::with_capacity(output.len());
        while let Some(start) = remaining.find(prefix) {
            next.push_str(&remaining[..start]);
            let candidate = &remaining[start + prefix.len()..];
            let Some(end) = candidate.find('.') else {
                next.push_str(&remaining[start..]);
                remaining = "";
                break;
            };
            let names = &candidate[..end];
            let valid = names.split(", ").all(|name| {
                !name.is_empty()
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            });
            let count = names.split(", ").count();
            if valid && count > MAX_NAMES {
                write!(
                    next,
                    "{count} names are {state}; run dir() in the kernel to list them."
                )
                .expect("writing to a String cannot fail");
            } else {
                next.push_str(prefix);
                next.push_str(names);
                next.push('.');
            }
            remaining = &candidate[end + 1..];
        }
        next.push_str(remaining);
        output = next;
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_lists_shorten_and_small_lists_stay_exact() {
        let names = (0..41)
            .map(|i| format!("name_{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let text = format!("These names are still defined: {names}. Small: x.");
        assert_eq!(
            shorten_name_lists(&text),
            "41 names are still defined; run dir() in the kernel to list them. Small: x."
        );
        let forty = (0..40)
            .map(|i| format!("name_{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let text = format!("These names are available again: {forty}.");
        assert_eq!(shorten_name_lists(&text), text);
    }

    #[test]
    fn request_transform_leaves_persisted_source_and_unrelated_rows_intact() {
        let names = (0..41)
            .map(|i| format!("name_{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let text = format!("[python-state-restored] These names are available again: {names}.");
        let row: AgentMessage = serde_json::from_value(serde_json::json!({
            "role": "custom", "customType": "ipython_state_restored",
            "content": text, "timestamp": 1
        }))
        .unwrap();
        let unrelated: AgentMessage = serde_json::from_value(serde_json::json!({
            "role": "custom", "customType": "other", "content": text, "timestamp": 2
        }))
        .unwrap();
        let original = vec![row, unrelated];
        assert_eq!(transform_messages(original.clone(), false), original);
        let request = transform_messages(original.clone(), true);
        assert!(serde_json::to_string(&request[0])
            .unwrap()
            .contains("41 names are available again"));
        assert_eq!(request[1], original[1]);
        assert!(serde_json::to_string(&original[0])
            .unwrap()
            .contains("name_40"));
    }
}
