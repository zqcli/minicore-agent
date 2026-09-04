#![allow(dead_code)] // Shared by process flow suites with different helper subsets.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::mpsc::{self as tokio_mpsc, UnboundedReceiver};

// Process-level guard only; protocol barriers and Events prove ordering.
const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);

/// An RPC subprocess driven through blocking stdio on dedicated threads.
/// Async callers never touch the raw pipes directly, which keeps readiness
/// handling deterministic instead of racing the OS pipe buffers.
pub struct RpcProcess {
    child: Option<Child>,
    stdin_tx: Sender<Vec<u8>>,
    stdout_rx: UnboundedReceiver<String>,
    stderr_lines: Arc<Mutex<Vec<String>>>,
    pending: VecDeque<Value>,
    events: Vec<Value>,
    observed: Vec<Value>,
}

impl RpcProcess {
    pub async fn spawn(config_path: &Path, key_env: &str, key: &str) -> Self {
        Self::spawn_with_extra_env(config_path, key_env, key, &[]).await
    }

    pub async fn spawn_with_extra_env(
        config_path: &Path,
        key_env: &str,
        key: &str,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_minicore-agent"));
        command
            .args(["--config", config_path.to_str().unwrap(), "--stdio"])
            .env(key_env, key);
        for &(name, value) in extra_env {
            command.env(name, value);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>();
        std::thread::spawn(move || {
            let mut stdin = stdin;
            for frame in stdin_rx {
                if stdin.write_all(&frame).is_err() || stdin.flush().is_err() {
                    break;
                }
            }
        });

        let (stdout_tx, stdout_rx) = tokio_mpsc::unbounded_channel::<String>();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if stdout_tx.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let (stderr_tx, stderr_rx) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if stderr_tx.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let stderr_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let log_lines = Arc::clone(&stderr_lines);
        std::thread::spawn(move || {
            for line in stderr_rx {
                log_lines.lock().unwrap().push(line);
            }
        });

        Self {
            child: Some(child),
            stdin_tx,
            stdout_rx,
            stderr_lines,
            pending: VecDeque::new(),
            events: Vec::new(),
            observed: Vec::new(),
        }
    }

    pub async fn send(&mut self, id: &str, method: &str, params: Value) {
        let mut frame = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .unwrap();
        frame.push(b'\n');
        let tx = self.stdin_tx.clone();
        tokio::task::spawn_blocking(move || {
            let _ = tx.send(frame);
        })
        .await
        .unwrap();
    }

    pub async fn response(&mut self, id: &str) -> Value {
        if let Some(index) = self.pending.iter().position(|frame| frame["id"] == id) {
            return self.pending.remove(index).unwrap();
        }
        loop {
            let frame = self.next_frame().await.expect("process stdout ended");
            if frame["method"] == "agent.event" {
                self.events.push(frame);
            } else if frame["id"] == id {
                return frame;
            } else {
                self.pending.push_back(frame);
            }
        }
    }

    pub async fn send_turn_and_register_wait(
        &mut self,
        prefix: &str,
        session_id: &Value,
        text: &str,
    ) -> (Value, String) {
        let send_id = format!("{prefix}-send");
        self.send(
            &send_id,
            "turn.send",
            json!({"session_id": session_id, "text": text}),
        )
        .await;
        let turn = self.response(&send_id).await["result"]["turn"].clone();
        let wait_id = format!("{prefix}-wait");
        self.send(
            &wait_id,
            "turn.wait",
            json!({
                "session_id": turn["session_id"],
                "loop_id": turn["loop_id"],
            }),
        )
        .await;
        let dispatch_ping_id = format!("{wait_id}-dispatch-ping");
        self.send(&dispatch_ping_id, "agent.ping", json!({})).await;
        assert_eq!(
            self.response(&dispatch_ping_id).await["result"]["version"],
            env!("CARGO_PKG_VERSION")
        );
        (turn, wait_id)
    }

    pub async fn event(&mut self, event_type: &str) -> Value {
        self.event_matching(event_type, |_| true).await
    }

    pub async fn event_matching(
        &mut self,
        event_type: &str,
        matches: impl Fn(&Value) -> bool,
    ) -> Value {
        if let Some(index) = self.events.iter().position(|frame| {
            frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type)
                && matches(frame)
        }) {
            return self.events.remove(index);
        }
        loop {
            let frame = self.next_frame().await.expect("process stdout ended");
            if frame["method"] == "agent.event" {
                if frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type)
                    && matches(&frame)
                {
                    return frame;
                }
                self.events.push(frame);
            } else {
                self.pending.push_back(frame);
            }
        }
    }

    pub fn observed(&self) -> &[Value] {
        &self.observed
    }

    /// Best-effort event probe: output deltas are not contractual, so tests
    /// assert through `turn.wait` and history and treat a delivered delta as
    /// a bonus signal rather than a requirement.
    pub async fn try_event(&mut self, event_type: &str) -> Option<Value> {
        if let Some(index) = self.events.iter().position(|frame| {
            frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type)
        }) {
            return Some(self.events.remove(index));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = self.next_frame().await?;
                if frame["method"] == "agent.event" {
                    if frame.pointer("/params/type").and_then(Value::as_str) == Some(event_type) {
                        return Some(frame);
                    }
                    self.events.push(frame);
                } else {
                    self.pending.push_back(frame);
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    async fn next_frame(&mut self) -> Option<Value> {
        let line = tokio::time::timeout(PROCESS_TIMEOUT, self.stdout_rx.recv())
            .await
            .unwrap_or_else(|_| {
                let recent = self
                    .stderr_lines
                    .lock()
                    .unwrap()
                    .iter()
                    .rev()
                    .take(40)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("");
                panic!("process stdout timed out\n--- recent stderr ---\n{recent}")
            })?;
        Some(self.store_line(line))
    }

    fn store_line(&mut self, line: String) -> Value {
        assert!(line.ends_with('\n'), "line must be newline terminated");
        let frame: Value = serde_json::from_str(&line).unwrap();
        self.observed.push(frame.clone());
        frame
    }

    pub async fn shutdown(mut self) -> (Vec<Value>, String) {
        self.send("shutdown", "agent.shutdown", json!({})).await;
        assert_eq!(
            self.response("shutdown").await["result"],
            json!({"ok": true})
        );
        assert!(self.next_frame().await.is_none());
        let child = self.child.take().expect("child already taken");
        let status = tokio::time::timeout(
            PROCESS_TIMEOUT,
            tokio::task::spawn_blocking(move || {
                let mut child = child;
                child.wait()
            }),
        )
        .await
        .expect("process did not exit")
        .expect("process wait task panicked")
        .expect("process wait failed");
        assert!(status.success());
        let stderr = self
            .stderr_lines
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<String>();
        (std::mem::take(&mut self.observed), stderr)
    }
}

impl Drop for RpcProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
