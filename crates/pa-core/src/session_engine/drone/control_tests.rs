use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn models(directory: &Path, address: std::net::SocketAddr) {
    std::fs::write(directory.join("models.json"),json!({"providers":{"h20-keys":{
        "api":"openai-completions","baseUrl":format!("http://{address}"),"apiKey":"fixture",
        "models":[{"id":"h20-mmfast","name":"fixture","reasoning":false,"contextWindow":128_000,"maxTokens":16384}]
    }}}).to_string()).unwrap();
}

#[tokio::test]
async fn stopped_assessment_never_contacts_the_configured_loopback_provider() {
    let dir = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    models(dir.path(), listener.local_addr().unwrap());
    let control = AssessmentControl::default();
    control.cancel();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        assess_with_control(
            dir.path(),
            dir.path(),
            json!({"events":[],"manifest":[]}),
            |_| {},
            &control,
        ),
    )
    .await
    .unwrap();
    assert_eq!(outcome.unwrap_err().to_string(), "drone_control_stopped");
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn user_stop_closes_only_the_held_assessment_and_confirms_transport_eof() {
    let dir = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    models(dir.path(), listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 16384];
        assert!(socket.read(&mut bytes).await.unwrap() > 0);
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let body = format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{"content":"n"},"finish_reason":null}]})
        );
        socket
            .write_all(format!("{:x}\r\n{body}\r\n", body.len()).as_bytes())
            .await
            .unwrap();
        seen_tx.send(()).unwrap();
        while socket.read(&mut bytes).await.unwrap_or(0) != 0 {}
        "transport EOF"
    });
    let control = AssessmentControl::default();
    let task_control = control.clone();
    let agent = dir.path().to_owned();
    let assessment = tokio::spawn(async move {
        assess_with_control(
            &agent,
            &agent,
            json!({"events":[],"manifest":[]}),
            |_| {},
            &task_control,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), seen_rx)
        .await
        .unwrap()
        .unwrap();
    control.cancel();
    let result = tokio::time::timeout(Duration::from_secs(6), assessment)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().to_string(), "drone_control_stopped");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap(),
        "transport EOF"
    );
}
