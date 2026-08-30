#![allow(dead_code)] // Shared by process flow suites with different helper subsets.

use std::collections::VecDeque;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

pub struct RpcProcess {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    stderr_task: JoinHandle<std::io::Result<Vec<u8>>>,
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
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).await?;
            Ok(bytes)
        });
        Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            stderr_task,
            child,
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
        self.input.write_all(&frame).await.unwrap();
        self.input.flush().await.unwrap();
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
                "instance_id": turn["instance_id"],
                "turn_id": turn["turn_id"],
            }),
        )
        .await;
        let dispatch_ping_id = format!("{wait_id}-dispatch-ping");
        self.send(&dispatch_ping_id, "agent.ping", json!({})).await;
        assert_eq!(
            self.response(&dispatch_ping_id).await["result"]["version"],
            "0.1.0"
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

    async fn next_frame(&mut self) -> Option<Value> {
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(10), self.output.read_line(&mut line))
            .await
            .expect("process stdout timed out")
            .unwrap();
        if read == 0 {
            return None;
        }
        assert!(line.ends_with('\n'));
        let frame: Value = serde_json::from_str(&line).unwrap();
        self.observed.push(frame.clone());
        Some(frame)
    }

    pub async fn shutdown(mut self) -> (Vec<Value>, String) {
        self.send("shutdown", "agent.shutdown", json!({})).await;
        assert_eq!(
            self.response("shutdown").await["result"],
            json!({"ok": true})
        );
        assert!(self.next_frame().await.is_none());
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("process did not exit")
            .unwrap();
        assert!(status.success());
        let stderr = tokio::time::timeout(Duration::from_secs(10), self.stderr_task)
            .await
            .expect("process stderr task did not exit")
            .expect("process stderr task panicked")
            .expect("process stderr read failed");
        (self.observed, String::from_utf8_lossy(&stderr).into_owned())
    }
}
