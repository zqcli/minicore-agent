#![allow(dead_code)] // Included by unit and process test crates with different helper subsets.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct MockResponse {
    status: u16,
    content_type: &'static str,
    headers: Vec<(String, String)>,
    chunks: Vec<Vec<u8>>,
    declared_length: Option<usize>,
    delay_before_headers: Duration,
    delay_between_chunks: Duration,
    chunk_gate: Option<Arc<Semaphore>>,
}

#[derive(Clone)]
pub struct ChunkGate {
    permits: Arc<Semaphore>,
}

impl ChunkGate {
    pub fn release(&self) {
        self.permits.add_permits(1);
    }
}

impl MockResponse {
    pub fn sse(events: &[Value]) -> Self {
        Self::sse_bytes(sse_body(events).into_bytes())
    }

    pub fn sse_bytes(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            headers: Vec::new(),
            chunks: vec![body],
            declared_length: None,
            delay_before_headers: Duration::ZERO,
            delay_between_chunks: Duration::ZERO,
            chunk_gate: None,
        }
    }

    pub fn json(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            content_type: "application/json",
            headers: Vec::new(),
            chunks: vec![body.into()],
            declared_length: None,
            delay_before_headers: Duration::ZERO,
            delay_between_chunks: Duration::ZERO,
            chunk_gate: None,
        }
    }

    pub fn with_chunks(mut self, chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
        self.chunks = chunks.into_iter().collect();
        self
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn with_declared_length(mut self, length: usize) -> Self {
        self.declared_length = Some(length);
        self
    }

    pub fn with_header_delay(mut self, delay: Duration) -> Self {
        self.delay_before_headers = delay;
        self
    }

    pub fn with_chunk_delay(mut self, delay: Duration) -> Self {
        self.delay_between_chunks = delay;
        self
    }

    pub fn with_chunk_gate(mut self) -> (Self, ChunkGate) {
        let permits = Arc::new(Semaphore::new(0));
        self.chunk_gate = Some(Arc::clone(&permits));
        (self, ChunkGate { permits })
    }
}

#[derive(Clone, Debug)]
pub struct CapturedRequest {
    request_line: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl CapturedRequest {
    pub fn method(&self) -> &str {
        self.request_line
            .split_whitespace()
            .next()
            .unwrap_or_default()
    }

    pub fn path(&self) -> &str {
        self.request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.as_str()))
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn json_body(&self) -> Value {
        serde_json::from_slice(&self.body).expect("captured request body must be JSON")
    }
}

pub struct MockServer {
    base_url: String,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    captured_notify: Arc<Notify>,
    fully_written: Arc<ProgressCounter>,
    handler_finished: Arc<ProgressCounter>,
    served: Arc<AtomicUsize>,
    task: JoinHandle<io::Result<()>>,
}

pub struct ConcurrentMockServer {
    base_url: String,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    captured_notify: Arc<Notify>,
    fully_written: Arc<ProgressCounter>,
    handler_finished: Arc<ProgressCounter>,
    task: JoinHandle<io::Result<()>>,
}

struct ProgressCounter {
    count: AtomicUsize,
    notify: Notify,
}

impl ProgressCounter {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    fn increment(&self) {
        self.count.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl ConcurrentMockServer {
    pub async fn spawn(responses: impl IntoIterator<Item = MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("concurrent mock server must bind loopback");
        let address = listener.local_addr().expect("concurrent mock address");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let task_captured = Arc::clone(&captured);
        let captured_notify = Arc::new(Notify::new());
        let task_notify = Arc::clone(&captured_notify);
        let fully_written = Arc::new(ProgressCounter::new());
        let task_fully_written = Arc::clone(&fully_written);
        let handler_finished = Arc::new(ProgressCounter::new());
        let task_handler_finished = Arc::clone(&handler_finished);
        let mut responses: VecDeque<_> = responses.into_iter().collect();
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            while !responses.is_empty() {
                let (stream, _) = listener.accept().await?;
                let response = responses
                    .pop_front()
                    .expect("checked concurrent response queue must not be empty");
                let handler_captured = Arc::clone(&task_captured);
                let handler_notify = Arc::clone(&task_notify);
                let handler_fully_written = Arc::clone(&task_fully_written);
                let handler_finished = Arc::clone(&task_handler_finished);
                handlers.spawn(async move {
                    let result = handle_connection(
                        stream,
                        response,
                        handler_captured,
                        handler_notify,
                        handler_fully_written,
                    )
                    .await;
                    handler_finished.increment();
                    result
                });
            }
            while let Some(result) = handlers.join_next().await {
                result.map_err(|_| io::Error::other("concurrent mock handler panicked"))??;
            }
            Ok(())
        });
        Self {
            base_url: format!("http://{address}"),
            captured,
            captured_notify,
            fully_written,
            handler_finished,
            task,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_for_requests(&self, count: usize) {
        wait_for_requests(&self.captured, &self.captured_notify, count).await;
    }

    pub async fn wait_for_fully_written(&self, count: usize) {
        wait_for_progress(
            &self.fully_written,
            count,
            "mock full response write timed out",
        )
        .await;
    }

    pub async fn wait_for_handler_finished(&self, count: usize) {
        wait_for_progress(
            &self.handler_finished,
            count,
            "mock response handler timed out",
        )
        .await;
    }

    pub async fn finish(self) -> Vec<CapturedRequest> {
        finish_server(self.task).await;
        Arc::try_unwrap(self.captured)
            .expect("concurrent captured requests must have one owner")
            .into_inner()
            .expect("concurrent captured request mutex must not be poisoned")
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    response: MockResponse,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    captured_notify: Arc<Notify>,
    fully_written: Arc<ProgressCounter>,
) -> io::Result<()> {
    let request = read_request(&mut stream).await?;
    captured.lock().unwrap().push(request);
    captured_notify.notify_waiters();
    if write_response(&mut stream, response).await? == WriteOutcome::FullyWritten {
        fully_written.increment();
    }
    Ok(())
}

impl MockServer {
    pub async fn spawn(responses: impl IntoIterator<Item = MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock server must bind loopback");
        let address = listener.local_addr().expect("mock listener address");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let task_captured = Arc::clone(&captured);
        let captured_notify = Arc::new(Notify::new());
        let task_notify = Arc::clone(&captured_notify);
        let fully_written = Arc::new(ProgressCounter::new());
        let handler_finished = Arc::new(ProgressCounter::new());
        let task_handler_finished = Arc::clone(&handler_finished);
        let served = Arc::new(AtomicUsize::new(0));
        let task_served = Arc::clone(&served);
        let _ = fully_written;
        let mut responses: VecDeque<_> = responses.into_iter().collect();
        let task = tokio::spawn(async move {
            // A response only counts as served once a complete request has
            // been read. HTTP clients may open and abort extra connections
            // (connection pooling, cancellation), and those must not consume
            // a scripted response.
            while !responses.is_empty() {
                let (mut stream, _) = listener.accept().await?;
                // Stray/aborted connections (client cancellation, pooling)
                // must not stall the accept loop: drop anything that does not
                // deliver a complete request promptly.
                let request =
                    tokio::time::timeout(Duration::from_secs(5), read_request(&mut stream))
                        .await
                        .ok()
                        .and_then(Result::ok);
                let Some(request) = request else {
                    continue;
                };
                let response = responses.pop_front().expect("checked response queue");
                task_served.fetch_add(1, Ordering::SeqCst);
                let handler_captured = Arc::clone(&task_captured);
                let handler_notify = Arc::clone(&task_notify);
                let handler_finished = Arc::clone(&task_handler_finished);
                tokio::spawn(async move {
                    handler_captured.lock().unwrap().push(request);
                    handler_notify.notify_waiters();
                    let result = write_response(&mut stream, response).await;
                    handler_finished.increment();
                    result
                });
            }
            Ok(())
        });
        Self {
            base_url: format!("http://{address}"),
            captured,
            captured_notify,
            fully_written,
            handler_finished,
            served,
            task,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_for_requests(&self, count: usize) {
        wait_for_requests(&self.captured, &self.captured_notify, count).await;
    }

    pub async fn wait_for_fully_written(&self, count: usize) {
        wait_for_progress(
            &self.fully_written,
            count,
            "mock full response write timed out",
        )
        .await;
    }

    pub fn fully_written_count(&self) -> usize {
        self.fully_written.count.load(Ordering::Acquire)
    }

    pub async fn wait_for_handler_finished(&self, count: usize) {
        wait_for_progress(
            &self.handler_finished,
            count,
            "mock response handler timed out",
        )
        .await;
    }

    pub async fn finish(self) -> Vec<CapturedRequest> {
        // The accept loop returns once every scripted response was handed to
        // a handler task; wait for those handlers to actually finish writing
        // before unwrapping the shared capture.
        let expected = self.served.load(Ordering::SeqCst);
        let task = self.task;
        finish_server(task).await;
        if expected > 0 {
            wait_for_progress(
                &self.handler_finished,
                expected,
                "mock response handler timed out",
            )
            .await;
        }
        Arc::try_unwrap(self.captured)
            .expect("captured requests must have one owner")
            .into_inner()
            .expect("captured request mutex must not be poisoned")
    }
}

async fn finish_server(task: JoinHandle<io::Result<()>>) {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("mock server finish timed out")
        .expect("mock server task must not panic")
        .expect("mock server I/O must succeed");
}

async fn wait_for_requests(captured: &Mutex<Vec<CapturedRequest>>, notify: &Notify, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if captured.lock().unwrap().len() >= count {
                return;
            }
            notified.await;
        }
    })
    .await
    .expect("mock request capture timed out");
}

async fn wait_for_progress(
    progress: &ProgressCounter,
    count: usize,
    timeout_message: &'static str,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let notified = progress.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if progress.count.load(Ordering::Acquire) >= count {
                return;
            }
            notified.await;
        }
    })
    .await
    .expect(timeout_message);
}

async fn read_request(stream: &mut TcpStream) -> io::Result<CapturedRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        if bytes.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mock request headers are too large",
            ));
        }
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "mock request headers ended early",
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
    };
    let head = std::str::from_utf8(&bytes[..header_end]).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "mock request headers are UTF-8")
    })?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let headers = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid header"))?;
            Ok((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let content_length = headers
        .iter()
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.parse::<usize>().ok())
                .flatten()
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing content length"))?;
    if content_length > MAX_REQUEST_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mock request body is too large",
        ));
    }
    let mut body = bytes[header_end + 4..].to_vec();
    if body.len() > content_length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mock request exceeds content length",
        ));
    }
    body.resize(content_length, 0);
    stream
        .read_exact(&mut body[bytes.len().saturating_sub(header_end + 4)..])
        .await?;
    Ok(CapturedRequest {
        request_line,
        headers,
        body,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteOutcome {
    FullyWritten,
    PeerClosed,
}

async fn write_response(
    stream: &mut TcpStream,
    response: MockResponse,
) -> io::Result<WriteOutcome> {
    let MockResponse {
        status,
        content_type,
        headers,
        chunks,
        declared_length,
        delay_before_headers,
        delay_between_chunks,
        chunk_gate,
    } = response;
    if !delay_before_headers.is_zero() {
        tokio::time::sleep(delay_before_headers).await;
    }
    let body_length = chunks.iter().map(Vec::len).sum::<usize>();
    let declared_length = declared_length.unwrap_or(body_length);
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        status,
        reason(status),
        content_type,
        declared_length,
    );
    for (name, value) in headers {
        head.push_str(&name);
        head.push_str(": ");
        head.push_str(&value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    if !write_bytes(stream, head.as_bytes()).await? {
        return Ok(WriteOutcome::PeerClosed);
    }
    for (index, chunk) in chunks.into_iter().enumerate() {
        if let Some(gate) = &chunk_gate {
            gate.acquire()
                .await
                .map_err(|_| io::Error::other("mock response gate closed"))?
                .forget();
        }
        if index > 0 && !delay_between_chunks.is_zero() {
            tokio::time::sleep(delay_between_chunks).await;
        }
        if !write_bytes(stream, &chunk).await? {
            return Ok(WriteOutcome::PeerClosed);
        }
        if !flush_stream(stream).await? {
            return Ok(WriteOutcome::PeerClosed);
        }
    }
    match stream.shutdown().await {
        Ok(()) => Ok(WriteOutcome::FullyWritten),
        Err(error) if is_peer_closed(&error) => Ok(WriteOutcome::FullyWritten),
        Err(error) => Err(error),
    }
}

async fn write_bytes(stream: &mut TcpStream, bytes: &[u8]) -> io::Result<bool> {
    match stream.write_all(bytes).await {
        Ok(()) => Ok(true),
        Err(error) if is_peer_closed(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn flush_stream(stream: &mut TcpStream) -> io::Result<bool> {
    match stream.flush().await {
        Ok(()) => Ok(true),
        Err(error) if is_peer_closed(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

fn is_peer_closed(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
    )
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Response",
    }
}

pub fn sse_body(events: &[Value]) -> String {
    let mut body = String::new();
    for event in events {
        body.push_str("data: ");
        body.push_str(&event.to_string());
        body.push_str("\n\n");
    }
    body
}
