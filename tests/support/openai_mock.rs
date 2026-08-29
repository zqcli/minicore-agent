#![allow(dead_code)] // Included by unit and process test crates with different helper subsets.

use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct MockResponse {
    status: u16,
    content_type: &'static str,
    headers: Vec<(String, String)>,
    chunks: Vec<Vec<u8>>,
    declared_length: Option<usize>,
    delay_before_headers: Duration,
    delay_between_chunks: Duration,
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
    task: JoinHandle<io::Result<()>>,
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
        let mut responses: VecDeque<_> = responses.into_iter().collect();
        let task = tokio::spawn(async move {
            while let Some(response) = responses.pop_front() {
                let (mut stream, _) = listener.accept().await?;
                let request = read_request(&mut stream).await?;
                task_captured.lock().unwrap().push(request);
                task_notify.notify_waiters();
                write_response(&mut stream, response).await?;
            }
            Ok(())
        });
        Self {
            base_url: format!("http://{address}"),
            captured,
            captured_notify,
            task,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_for_requests(&self, count: usize) {
        loop {
            let notified = self.captured_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.captured.lock().unwrap().len() >= count {
                return;
            }
            notified.await;
        }
    }

    pub async fn finish(self) -> Vec<CapturedRequest> {
        self.task
            .await
            .expect("mock server task must not panic")
            .expect("mock server I/O must succeed");
        Arc::try_unwrap(self.captured)
            .expect("captured requests must have one owner")
            .into_inner()
            .expect("captured request mutex must not be poisoned")
    }
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

async fn write_response(stream: &mut TcpStream, response: MockResponse) -> io::Result<()> {
    if !response.delay_before_headers.is_zero() {
        tokio::time::sleep(response.delay_before_headers).await;
    }
    let body_length = response.chunks.iter().map(Vec::len).sum::<usize>();
    let declared_length = response.declared_length.unwrap_or(body_length);
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        reason(response.status),
        response.content_type,
        declared_length,
    );
    for (name, value) in response.headers {
        head.push_str(&name);
        head.push_str(": ");
        head.push_str(&value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    if stream.write_all(head.as_bytes()).await.is_err() {
        return Ok(());
    }
    for (index, chunk) in response.chunks.into_iter().enumerate() {
        if index > 0 && !response.delay_between_chunks.is_zero() {
            tokio::time::sleep(response.delay_between_chunks).await;
        }
        if stream.write_all(&chunk).await.is_err() {
            return Ok(());
        }
        let _ = stream.flush().await;
    }
    let _ = stream.shutdown().await;
    Ok(())
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
