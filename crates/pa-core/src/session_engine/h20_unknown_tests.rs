use super::auto_retry::run_turn_with_auto_retry;
use super::provider_adapter::{json_round_trip, real_stream_fn};
use super::provider_failover::{run_turn_with_provider_failover, ProviderFailoverPolicy};
use super::provider_retry::{
    complete_with_provider_retry, ProviderRetryPolicy, UNBOUNDED_BACKOFF_MS,
};
use super::side_question::{run_side_question, SideQuestionSink, SideQuestionStatus};
use pa_agent::abort::AbortSignal;
use pa_agent::agent::{Agent, AgentInitialState, AgentOptions};
use pa_agent::stream::{LlmContext, StreamRequestOptions};
use pa_agent::types::StopReason;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn policy() -> ProviderRetryPolicy {
    ProviderRetryPolicy {
        enabled: true,
        max_retries: 2,
        base_delay_ms: 1,
        max_retry_delay_ms: 1000,
        max_delay_ms: UNBOUNDED_BACKOFF_MS,
    }
}

async fn fake_server(
    mode: &str,
    provider: &str,
) -> (
    pa_types::ai::Model,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let seen_server = Arc::clone(&seen);
    let mode = mode.to_owned();
    let job = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            seen_server.fetch_add(1, Ordering::SeqCst);
            let mut request = vec![0u8; 16384];
            let _ = socket.read(&mut request).await.unwrap();
            match mode.as_str() {
                "transport" => { /* peer closes after request */ }
                "rate" => {
                    let _ = socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
                }
                "timeout" => {
                    let _ = socket.write_all(b"HTTP/1.1 408 Request Timeout\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
                }
                "conflict" => {
                    let _ = socket.write_all(b"HTTP/1.1 409 Conflict\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
                }
                "server" => {
                    let _ = socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
                }
                "sse" => {
                    let body = "data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n";
                    let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = socket.write_all(reply.as_bytes()).await;
                }
                _ => unreachable!(),
            }
        }
    });
    let model = serde_json::from_value(serde_json::json!({
        "id":"m", "name":"fixture", "api":"openai-completions",
        "provider":provider, "baseUrl":format!("http://{addr}"),
        "reasoning":false, "input":["text"],
        "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},
        "contextWindow":8192, "maxTokens":100
    }))
    .unwrap();
    (model, seen, job)
}

#[tokio::test]
async fn h20_unknown_outcome_never_retries_any_driver_against_fake_server() {
    for mode in ["transport", "server", "sse", "timeout", "conflict", "rate"] {
        for driver in ["one-shot", "interactive", "failover", "side"] {
            let (model, seen, server) = fake_server(mode, "h20-keys").await;
            let agent_model: pa_agent::types::Model = json_round_trip(&model).unwrap();
            let stream = real_stream_fn(Some("fixture".to_string()), model.clone());
            let make_attempt = || {
                let stream = Arc::clone(&stream);
                let model = agent_model.clone();
                async move {
                    let mut stream = stream(
                        model,
                        LlmContext::default(),
                        StreamRequestOptions::default(),
                    )
                    .await?;
                    stream.result().await
                }
            };
            match driver {
                "one-shot" => {
                    let message = complete_with_provider_retry(
                        &policy(),
                        None,
                        |_| async { true },
                        make_attempt,
                    )
                    .await
                    .unwrap();
                    assert_eq!(message.stop_reason, StopReason::Error);
                    assert_eq!(message.provider, "h20-keys");
                }
                "interactive" => {
                    let message = run_turn_with_auto_retry(
                        &policy(),
                        0,
                        None,
                        make_attempt,
                        |_| async { Ok(()) },
                        |_| async { true },
                        None,
                    )
                    .await
                    .unwrap();
                    assert_eq!(message.stop_reason, StopReason::Error);
                    assert_eq!(message.provider, "h20-keys");
                }
                "failover" => {
                    let failover = ProviderFailoverPolicy {
                        enabled: true,
                        max_retries: 1,
                        base_delay_ms: 1,
                        max_delay_ms: 10,
                    };
                    let switches = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&switches);
                    let message = run_turn_with_provider_failover(
                        &policy(),
                        &failover,
                        std::slice::from_ref(&model),
                        0,
                        None,
                        make_attempt,
                        |_| async { Ok(()) },
                        |_| async { true },
                        move |_| {
                            let count = Arc::clone(&count);
                            async move {
                                count.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            }
                        },
                        || async { Ok(None) },
                        None,
                    )
                    .await
                    .unwrap();
                    assert_eq!(message.stop_reason, StopReason::Error);
                    assert_eq!(switches.load(Ordering::SeqCst), 0);
                }
                "side" => {
                    let parent = Arc::new(Agent::new(AgentOptions {
                        initial_state: AgentInitialState {
                            model: Some(agent_model),
                            ..Default::default()
                        },
                        stream_fn: Some(stream),
                        ..Default::default()
                    }));
                    let sink: SideQuestionSink = Arc::new(|_| true);
                    let result = run_side_question(
                        &parent,
                        parent.stream_fn().unwrap().clone(),
                        "fixture",
                        &[],
                        &policy(),
                        &AbortSignal::never(),
                        &sink,
                    )
                    .await;
                    assert_eq!(result.status, SideQuestionStatus::Error);
                }
                _ => unreachable!(),
            }
            assert_eq!(
                seen.load(Ordering::SeqCst),
                1,
                "{driver} {mode}: repeated request"
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn other_provider_keeps_existing_retries() {
    for (mode, provider) in [("rate", "other-provider"), ("server", "other-provider")] {
        let (model, seen, server) = fake_server(mode, provider).await;
        let agent_model: pa_agent::types::Model = json_round_trip(&model).unwrap();
        let stream = real_stream_fn(Some("fixture".to_string()), model);
        let make_attempt = || {
            let stream = Arc::clone(&stream);
            let model = agent_model.clone();
            async move {
                let mut stream = stream(
                    model,
                    LlmContext::default(),
                    StreamRequestOptions::default(),
                )
                .await?;
                stream.result().await
            }
        };
        let message =
            complete_with_provider_retry(&policy(), None, |_| async { true }, make_attempt)
                .await
                .unwrap();
        assert_eq!(message.stop_reason, StopReason::Error);
        assert_eq!(
            seen.load(Ordering::SeqCst),
            3,
            "{provider} {mode} should retain retries"
        );
        server.abort();
    }
}
