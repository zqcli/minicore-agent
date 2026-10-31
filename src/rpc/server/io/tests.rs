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

#[tokio::test]
async fn snapshot_encoding_keeps_its_charge_until_the_writer_flushes() {
    struct FlushGate {
        writer: Writer,
        started: Arc<tokio::sync::Notify>,
        release: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    }
    impl AsyncWrite for FlushGate {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.writer).poll_write(cx, bytes)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.started.notify_one();
            self.release.as_mut().poll(cx).map(Ok)
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let (sink, mut events) = crate::event::AgentEventSink::channel(1);
    let hub = crate::sessions::SnapshotHub::new(sink.clone());
    let session_id = crate::ids::SessionId::new().unwrap();
    let load = crate::test_support::load_id();
    let event = crate::AgentEvent::SessionSnapshot {
        snapshot: crate::SessionSnapshot {
            session_id,
            load,
            revision: 1,
            execution: crate::SessionExecution::None,
            pending_interaction: None,
            queue: crate::Queue::default(),
            compaction: None,
            block_reason: None,
            completions: crate::CompletionWindow {
                through_seq: 0,
                retained_from_seq: 1,
                entries: Vec::new(),
                truncated: false,
            },
        },
        meta: crate::event::EventMeta::session(session_id, load),
    };
    let stopped = tokio_util::sync::CancellationToken::new();
    assert!(
        sink.send_snapshot(event, hub.try_reserve().unwrap(), &stopped)
            .await
    );
    let (event, charge) = events.recv_owned().await.unwrap();
    assert!(hub.used_for_test() > 0);
    let response = RpcResponse::notification(
        AgentEventNotification::new(event),
        charge.expect("owned control charge"),
    )
    .unwrap();
    let bytes = response.encoded().len() + 1 + crate::rpc::WRITER_BUFFER_BYTES;
    assert_eq!(hub.used_for_test(), bytes);
    let (sender, receiver) = mpsc::channel(1);
    sender.send(RpcOutbound::Response(response)).await.unwrap();
    drop(sender);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = tokio_util::sync::CancellationToken::new();
    let writer = FlushGate {
        writer: Writer(Arc::new(Mutex::new(Written::default()))),
        started: Arc::clone(&started),
        release: Box::pin(release.clone().cancelled_owned()),
    };
    let task = tokio::spawn(write_outbound(writer, receiver));
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    assert_eq!(hub.used_for_test(), bytes);
    release.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(hub.used_for_test(), 0);
}
