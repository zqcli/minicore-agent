use super::*;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

#[derive(Default)]
struct Written {
    bytes: Vec<u8>,
    flushes: usize,
}

struct Writer(Arc<Mutex<Written>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = bytes.len().min(7);
        self.0.lock().unwrap().bytes.extend_from_slice(&bytes[..n]);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.lock().unwrap().flushes += 1;
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Counted(Arc<AtomicUsize>);
impl Serialize for Counted {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        "quoted\"\nUnicode 中".serialize(serializer)
    }
}

#[tokio::test]
async fn queued_responses_are_encoded_once_and_flushed_as_one_batch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(Mutex::new(Written::default()));
    let (sender, receiver) = mpsc::channel(8);
    for index in 0..5 {
        sender
            .send(RpcOutbound::Response(success(
                &RpcId::String(index.to_string()),
                Counted(calls.clone()),
            )))
            .await
            .unwrap();
    }
    drop(sender);
    write_outbound(Writer(written.clone()), receiver)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    let written = written.lock().unwrap();
    assert_eq!(written.flushes, 1);
    assert!(written.bytes.ends_with(b"\n"));
    let lines: Vec<_> = written
        .bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(lines.len(), 5);
    for (index, line) in lines.into_iter().enumerate() {
        assert_eq!(
            serde_json::from_slice::<Value>(line).unwrap(),
            serde_json::json!({
                "jsonrpc":"2.0", "id":index.to_string(), "result":"quoted\"\nUnicode 中"
            })
        );
    }
}

#[tokio::test]
async fn serialization_failure_is_a_complete_error_frame() {
    struct Failure;
    impl Serialize for Failure {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("private details"))
        }
    }
    let written = Arc::new(Mutex::new(Written::default()));
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(RpcOutbound::Response(success(
            &RpcId::String("failed".into()),
            Failure,
        )))
        .await
        .unwrap();
    drop(sender);
    write_outbound(Writer(written.clone()), receiver)
        .await
        .unwrap();
    let written = written.lock().unwrap();
    let value: Value = serde_json::from_slice(&written.bytes).unwrap();
    assert_eq!(value["error"]["data"]["kind"], "internal_error");
    assert_eq!(value["id"], "failed");
    assert!(!String::from_utf8_lossy(&written.bytes).contains("private details"));
}
