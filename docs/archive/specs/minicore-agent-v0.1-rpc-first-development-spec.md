# MiniCore Agent v0.1：RPC-First Coding Agent 开发实施规格

## 0. 文档目的

本文档用于指导代码 Agent 开发 MiniCore 的第一版 Agent 层。

第一版的唯一目标是：

> 使用 MiniCore Runtime 作为单 Session 执行内核，通过一个简单的本地 RPC 进程，跑通较完整的 Coding Agent Loop。

本版强调：

- 架构简洁；
- 代码直接；
- 所有权清楚；
- 能通过 RPC 真实创建 Session、调用模型、执行工具、继续模型循环并返回结果；
- 不提前实现多客户端、插件平台、断线恢复、动态扩展管理和复杂分布式能力；
- 不复制 MiniCore 已经负责的 Agent Loop、Conversation、Turn、Cancellation 和 durable semantics。

本文档不是长期终态设计。它定义一个可运行、可测试、便于后续演进的 v0.1。

---

# 1. 开发基线

## 1.1 MiniCore 基线

依赖：

```text
repository:
https://github.com/zqcli/minicore-runtime

branch:
refactor/v0.3-simplify

reviewed API commit:
7e85eaab18e273e43e03c50040b460f1b13f0ac9

crate:
minicore-runtime = 0.3.0
```

开始开发前确认分支最新 HEAD。

如果 MiniCore 后续提交只做内部重构而 Public API 不变，可以直接升级。

如果 Public API 已发生变化，先更新本文档中的 API 映射，不得把已经从 Core 移出的功能重新塞回 MiniCore。

## 1.2 当前 MiniCore 提供的能力

Agent 层直接使用：

```text
SessionRuntime
SessionHandle
TurnHandle
SessionState
SessionEventStream

SessionSpec
SessionManifest
KernelConfig
TurnOptions
UserInput

SessionBindings

Model
ModelDescriptor
ModelRequest
ModelEvent
ModelError

Tool
ToolSet
ToolPolicy
ToolContext
ToolInvocation
ToolOutput

ContextProvider
ContextRequest
ContextBundle

CompactionStrategy

SessionLog
ConversationEntry
TranscriptPage
```

MiniCore 保证：

```text
一个 SessionRuntime 对应一个 loaded Session
一个 Session 同时只有一个 active Turn
Model → Tool → Model 的 Agent Loop
Conversation durable-first
ToolCall / ToolResult 匹配
typed interaction
exact Turn cancellation
unfinished Turn restart repair
SessionRuntime shutdown barrier
```

Agent 层不得重新实现这些语义。

---

# 2. v0.1 的明确范围

## 2.1 必须实现

```text
一个 Agent 顶层对象
多个 loaded SessionRuntime
本地 Session 存储
本地 Workspace
一套最小 Coding Tools
一个真实 Model Provider
一个简单 Project Context
一个简单 Tool Policy
stdio JSON-RPC
Session/Turn/Event RPC
完整离线测试
一个真实 Provider smoke test
```

最小 Coding Tools：

```text
read
write
edit
apply_patch
bash
```

最小 Agent 流程：

```text
RPC create/open Session
→ send user message
→ MiniCore 调用 Model
→ Model 返回 ToolCall
→ Agent Tool 执行
→ ToolResult 返回 MiniCore
→ MiniCore 再次调用 Model
→ Model 返回 Final
→ RPC 收到流式事件与 durable Turn outcome
```

## 2.2 明确不实现

v0.1 不实现：

```text
GUI
TUI
CLI 交互界面
TCP/WebSocket/HTTP RPC
多个 RPC 客户端
事件 replay
客户端重连
Agent 状态跨重启恢复
pending approval 跨重启恢复
active Turn 跨重启恢复
Session Fork
Session Tree
Session Branch
Subagent 专用系统
Remote Agent
MCP
Skills
Memory
RAG
Plugin Manager
动态 dylib
WASI
Provider fallback
多 Provider 负载均衡
价格计算
账号管理
OAuth
数据库
多进程 Session writer lock
Git worktree 管理
容器 Sandbox
进程树完整隔离
后台任务恢复
复杂 Profile 版本管理
```

## 2.3 重启语义

Agent 重启后：

```text
所有 Session 默认 Unloaded
所有 TurnHandle 丢失
所有审批状态丢失
所有事件状态丢失
所有 Workspace runtime object 丢失
```

重新打开 Session：

```text
Store 打开 SessionLog
→ SessionRuntime::load
→ MiniCore replay durable Conversation
→ MiniCore repair unfinished Turn
→ Session 返回 Idle
```

Agent 层不恢复瞬时执行状态。

注意：

> 不能只读取最后一条消息。

必须让 `SessionRuntime::load` 从完整 SessionLog 恢复 MiniCore 需要的 durable Conversation。Agent 层只是“不恢复瞬时状态”，不是丢弃对话历史。

---

# 3. 总体架构

```text
┌────────────────────────────────────────────┐
│                RPC Client                  │
│     test / future GUI / TUI / CLI          │
└─────────────────────┬──────────────────────┘
                      │ JSON-RPC 2.0 / NDJSON
┌─────────────────────▼──────────────────────┐
│                 RpcServer                  │
│                                            │
│  stdin reader                              │
│  stdout writer                             │
│  request dispatch                          │
│  AgentEvent notification                   │
└─────────────────────┬──────────────────────┘
                      │ direct Rust call
┌─────────────────────▼──────────────────────┐
│                    Agent                   │
│                                            │
│  Sessions                                  │
│  Store                                     │
│  Profiles                                  │
│  Models                                    │
│  Workspaces                                │
│  Tools                                     │
│  Context                                   │
│  Policy                                    │
└─────────────────────┬──────────────────────┘
                      │ one loaded Session
┌─────────────────────▼──────────────────────┐
│             MiniCore SessionRuntime        │
│                                            │
│  SessionHandle                             │
│  TurnHandle                                │
│  SessionState                              │
│  SessionEventStream                        │
└────────────────────────────────────────────┘
```

核心原则：

```text
RpcServer 只做协议
Agent 只做产品编排
MiniCore 只做单 Session 执行
Adapter 持有真实权限
```

---

# 4. 仓库与 crate 组织

## 4.1 第一版只使用一个 crate

建议新仓库或新 package：

```text
minicore-agent
```

第一版不要拆成：

```text
minicore-agent-core
minicore-agent-rpc
minicore-agent-local
minicore-agent-provider-openai
minicore-agent-tools
```

一个 crate 同时提供：

```text
library:
    Agent API

binary:
    minicore-agent
```

以后确认边界稳定后再拆 crate。

## 4.2 目标目录

```text
minicore-agent/
├── Cargo.toml
├── README.md
├── example.agent.toml
│
├── src/
│   ├── lib.rs
│   ├── main.rs
│   │
│   ├── agent.rs
│   ├── config.rs
│   ├── error.rs
│   ├── event.rs
│   ├── sessions.rs
│   ├── store.rs
│   ├── profiles.rs
│   ├── models.rs
│   ├── workspace.rs
│   ├── context.rs
│   ├── policy.rs
│   │
│   ├── models/
│   │   └── openai.rs
│   │
│   ├── tools/
│   │   ├── mod.rs
│   │   ├── read.rs
│   │   ├── write.rs
│   │   ├── edit.rs
│   │   ├── apply_patch.rs
│   │   └── bash.rs
│   │
│   └── rpc/
│       ├── mod.rs
│       ├── protocol.rs
│       └── server.rs
│
├── tests/
│   ├── agent_loop.rs
│   ├── restart.rs
│   ├── rpc_stdio.rs
│   ├── tools.rs
│   └── store.rs
│
└── fixtures/
    └── workspace/
```

## 4.3 模块职责

| 模块 | 责任 |
|---|---|
| `agent.rs` | 顶层 API 与流程编排 |
| `sessions.rs` | loaded SessionRuntime 集合 |
| `store.rs` | Session metadata 与 LocalSessionLog |
| `profiles.rs` | 配置中的 Agent Profile |
| `models.rs` | Model profile 构造与查找 |
| `models/openai.rs` | 第一版真实 Model adapter |
| `workspace.rs` | 本地根目录和安全路径 |
| `tools/*` | 五个具体 Coding Tool |
| `context.rs` | AGENTS.md ContextProvider |
| `policy.rs` | 简单 ToolPolicy |
| `event.rs` | Agent 对外事件 DTO |
| `rpc/*` | NDJSON JSON-RPC |
| `config.rs` | TOML 配置 |
| `error.rs` | Agent 与 RPC 错误 |

---

# 5. 依赖原则

## 5.1 建议依赖

```toml
[dependencies]
minicore-runtime = { version = "0.3.0" }

tokio = { version = "...", features = [
  "rt-multi-thread",
  "macros",
  "sync",
  "time",
  "fs",
  "io-util",
  "process",
  "signal"
] }

tokio-util = { version = "...", features = ["rt"] }
futures-util = "..."
serde = { version = "...", features = ["derive"] }
serde_json = "..."
toml = "..."
thiserror = "..."
tracing = "..."
tracing-subscriber = { version = "...", features = ["env-filter"] }
reqwest = { version = "...", default-features = false, features = [
  "json",
  "stream",
  "rustls-tls"
] }
bytes = "..."
diffy = "..."
```

可选：

```toml
time = { version = "...", features = ["serde"] }
```

## 5.2 不引入

第一版不要引入：

```text
axum
tonic
jsonrpsee
sqlx
diesel
sea-orm
dashmap
inventory
libloading
wasmtime
tower plugin stack
dependency injection framework
service locator
actor framework
```

JSON-RPC 协议很小，直接实现即可。

## 5.3 代码原则

- 禁止 `unsafe`；
- 不使用全局 mutable singleton；
- 不使用 `Any` / `TypeMap`；
- 不写宏生成大批业务代码；
- 不为未来功能预建抽象；
- 同一逻辑只保留一套状态；
- 对安全和 durable contract 必须严格；
- 对不影响正确性的边界不做复杂防御。

---

# 6. Agent 顶层对象

## 6.1 类型

```rust
pub struct Agent {
    config: AgentConfig,
    store: Store,
    profiles: Profiles,
    models: Models,
    sessions: Sessions,

    events_tx: tokio::sync::mpsc::Sender<AgentEvent>,
    events_rx: Option<tokio::sync::mpsc::Receiver<AgentEvent>>,

    task_runtime: tokio::runtime::Handle,
}
```

`Agent`：

- 不实现 `Clone`；
- 一个进程通常只创建一个；
- RPC server 独占一个 mutable Agent；
- 多个 SessionRuntime 由 Agent 管理；
- 不使用额外 Agent actor；
- 不使用 `AgentService / AgentClient` 双层类型。

## 6.2 Public API

```rust
impl Agent {
    pub async fn open(
        config: AgentConfig,
    ) -> Result<Self, AgentError>;

    pub fn take_events(
        &mut self,
    ) -> Result<AgentEventStream, AgentError>;

    pub async fn list_sessions(
        &self,
    ) -> Result<Vec<SessionInfo>, AgentError>;

    pub async fn create_session(
        &mut self,
        request: CreateSession,
    ) -> Result<SessionInfo, AgentError>;

    pub async fn open_session(
        &mut self,
        session_id: SessionId,
    ) -> Result<SessionInfo, AgentError>;

    pub async fn close_session(
        &mut self,
        session_id: SessionId,
    ) -> Result<(), AgentError>;

    pub async fn delete_session(
        &mut self,
        session_id: SessionId,
    ) -> Result<(), AgentError>;

    pub fn session_state(
        &self,
        session_id: SessionId,
    ) -> Result<SessionState, AgentError>;

    pub async fn send(
        &mut self,
        request: SendMessage,
    ) -> Result<TurnRef, AgentError>;

    pub fn cancel(
        &self,
        turn: TurnRef,
    ) -> Result<bool, AgentError>;

    pub fn turn_handle(
        &self,
        turn: TurnRef,
    ) -> Result<TurnHandle, AgentError>;

    pub async fn answer(
        &self,
        request: AnswerInteraction,
    ) -> Result<(), AgentError>;

    pub async fn transcript(
        &self,
        request: GetTranscript,
    ) -> Result<TranscriptPage, AgentError>;

    pub async fn shutdown(
        self,
    ) -> Result<(), AgentError>;
}
```

## 6.3 为什么 API 使用 `&mut self`

v0.1 的 RPC reader 按顺序处理请求。

因此：

- create/open/close/send 对 Agent 使用 `&mut self`；
- 不需要内部全局 Mutex；
- 不需要 Loading/Closing 状态机；
- 不需要并发 map reservation；
- SessionRuntime 内部仍然异步并发执行。

多个 Session 仍可同时 Running：

```text
send A → 立即返回 TurnRef
send B → 立即返回 TurnRef
A/B 各自在独立 SessionRuntime 中运行
```

`turn.wait` 不通过 `&mut Agent` 长时间等待，而是先 clone `TurnHandle`，再由 RPC server 单独 spawn waiter。

未来如果 GUI 需要并发直接调用，可以在 Agent 外部增加：

```text
Arc<Mutex<Agent>>
```

或将 Agent 放入单独 task；v0.1 不提前实现。

---

# 7. Sessions

## 7.1 类型

```rust
pub struct Sessions {
    loaded: std::collections::HashMap<SessionId, LoadedSession>,
}
```

```rust
struct LoadedSession {
    runtime: SessionRuntime,
    handle: SessionHandle,

    active_turn: Option<TurnHandle>,

    event_task: tokio::task::JoinHandle<()>,
    state_task: tokio::task::JoinHandle<()>,
}
```

## 7.2 不增加复杂 lifecycle enum

v0.1 不实现：

```text
Loading
Ready
Closing
Failed
Evicting
```

原因：

- RPC request 顺序执行；
- `open_session` 完成后才插入 map；
- `close_session` 先 remove，再 shutdown；
- 不存在两个请求同时 open 同一 Session。

## 7.3 打开 Session

流程：

```text
检查 loaded map
→ Store 读取 SessionRecord
→ Profiles 查找 profile
→ Workspaces 打开本地 workspace
→ Models 查找 Model
→ Tools 构造 ToolSet
→ Context 构造 ContextProvider
→ Policy 构造 ToolPolicy
→ Store 打开 LocalSessionLog
→ 构造 SessionSpec / SessionBindings / SessionRuntimeOptions
→ SessionRuntime::load
→ take_events
→ handle
→ spawn event task
→ spawn state task
→ 插入 loaded map
→ 返回 SessionInfo
```

任一步失败：

- 不插入 map；
- 已创建 SessionRuntime 时必须 shutdown；
- 已创建普通对象直接 drop；
- 不实现多层 rollback framework。

## 7.4 创建 Session

流程：

```text
校验 profile
→ 校验 workspace
→ 生成 SessionId
→ Store 创建 SessionRecord 和空 Session directory
→ 构造能力
→ Store 打开 empty LocalSessionLog
→ SessionRuntime::create
→ spawn pumps
→ 插入 loaded map
```

若 Core create 失败：

```text
best-effort 删除刚创建的 Session directory
→ 返回原始错误
```

不实现 `Creating` durable 状态。

## 7.5 关闭 Session

```rust
pub async fn close_session(&mut self, id: SessionId) -> Result<(), AgentError> {
    let loaded = self.sessions.remove(id)?;
    loaded.runtime.shutdown().await?;
    loaded.event_task.await?;
    loaded.state_task.await?;
    Ok(())
}
```

要求：

- 先从 map remove；
- 不持有 map borrow 跨 await；
- `SessionRuntime::shutdown` 是 durability barrier；
- task 应在 Core stream/watch 关闭后自然退出；
- JoinError 映射为 Agent internal error。

## 7.6 active Turn

`send()`：

1. 找到 loaded Session；
2. 如果 `active_turn` 已完成，清除旧 handle；
3. 调用 `SessionHandle::submit`；
4. 保存 TurnHandle；
5. clone TurnHandle，spawn completion notification；
6. 返回 TurnRef。

```rust
pub struct TurnRef {
    pub session_id: SessionId,
    pub instance_id: SessionInstanceId,
    pub turn_id: TurnId,
}
```

不需要在 waiter 完成时回写 map。

下一次：

```text
send
cancel
turn.wait
```

读取 active Turn 时调用 `is_finished()` 做 lazy cleanup。

每个 Session 只保存一个 TurnHandle，符合 MiniCore 单 active Turn 语义。

---

# 8. Agent Event

## 8.1 类型

```rust
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum AgentEvent {
    SessionOpened {
        session: SessionInfo,
    },

    SessionClosed {
        session_id: SessionId,
    },

    SessionState {
        state: SessionState,
    },

    TurnStarted {
        turn: TurnRef,
    },

    OutputDelta {
        turn: TurnRef,
        channel: OutputChannel,
        delta: String,
        dropped_before: u64,
    },

    ToolStarted {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        tool_name: String,
        dropped_before: u64,
    },

    ToolProgress {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        progress: ToolProgressView,
        dropped_before: u64,
    },

    ToolFinished {
        turn: TurnRef,
        tool_call_id: ToolCallId,
        result: ToolResultView,
        dropped_before: u64,
    },

    InteractionRequested {
        session_id: SessionId,
        interaction: PendingInteraction,
    },

    InteractionResolved {
        session_id: SessionId,
        interaction_id: InteractionId,
    },

    TurnFinished {
        turn: TurnRef,
        outcome: TurnOutcome,
    },
}
```

## 8.2 单消费者

```rust
pub struct AgentEventStream {
    receiver: mpsc::Receiver<AgentEvent>,
}
```

提供：

```rust
pub async fn recv(&mut self) -> Option<AgentEvent>;
```

可实现 `Stream`，但不是第一版强制项。

RPC server 是唯一消费者。

不实现 fan-out。

## 8.3 Core Event 映射

event task 消费 `SessionEventStream`：

```text
TurnStarted           → AgentEvent::TurnStarted
OutputDelta           → AgentEvent::OutputDelta
ToolStarted           → AgentEvent::ToolStarted
ToolProgress          → AgentEvent::ToolProgress
ToolFinished          → AgentEvent::ToolFinished
InteractionRequested  → AgentEvent::InteractionRequested
InteractionResolved   → AgentEvent::InteractionResolved
HealthChanged         → 由 state watch 发送 SessionState
TurnFinished          → 不转发
```

`TurnFinished` 由 Agent 自己对 `TurnHandle::wait()` 的结果发送，保证使用 durable outcome。

Core envelope 的：

```text
session_id
instance_id
dropped_before
```

必须保留。

## 8.4 state task

每个 loaded Session：

```rust
let mut state = handle.watch_state();

send initial state

loop {
    if state.changed().await.is_err() {
        break;
    }

    send AgentEvent::SessionState {
        state: state.borrow().clone(),
    }
}
```

不保存额外 `AgentSessionLiveView`。

RPC 客户端自行维护展示状态。

---

# 9. Store

## 9.1 第一版使用具体 `Store`

```rust
pub struct Store {
    root: PathBuf,
}
```

不定义 `Store` trait。

以后需要 SQLite 或 Remote Store 时再抽象。

## 9.2 目录结构

```text
<data_dir>/
└── sessions/
    └── <session-id>/
        ├── session.json
        ├── manifest.json
        └── conversation.log
```

## 9.3 SessionRecord

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub session_id: SessionId,
    pub title: Option<String>,
    pub profile: String,
    pub workspace: PathBuf,
    pub created_at: String,
    pub updated_at: String,
}
```

`session.json` 只保存 Agent 层 metadata。

`manifest.json` 由 `LocalSessionLog` 实现 MiniCore `SessionLog` 时维护。

## 9.4 Store API

```rust
impl Store {
    pub async fn open(root: PathBuf) -> Result<Self, StoreError>;

    pub async fn list_sessions(
        &self,
    ) -> Result<Vec<SessionRecord>, StoreError>;

    pub async fn create_session(
        &self,
        record: SessionRecord,
    ) -> Result<LocalSessionLog, StoreError>;

    pub async fn load_record(
        &self,
        session_id: SessionId,
    ) -> Result<SessionRecord, StoreError>;

    pub async fn open_log(
        &self,
        session_id: SessionId,
    ) -> Result<LocalSessionLog, StoreError>;

    pub async fn touch(
        &self,
        session_id: SessionId,
    ) -> Result<(), StoreError>;

    pub async fn delete_session(
        &self,
        session_id: SessionId,
    ) -> Result<(), StoreError>;
}
```

## 9.5 单进程假设

v0.1 明确假设：

> 同一个 `data_dir` 同时只由一个 Agent 进程使用。

因此不实现：

```text
OS file lock
cross-process lease
lock heartbeat
stale lock recovery
```

`Sessions` map 防止同一进程重复 load。

README 必须写明这个限制。

---

# 10. LocalSessionLog

## 10.1 类型

```rust
pub struct LocalSessionLog {
    directory: PathBuf,
    manifest: Option<SessionManifest>,
    entries: Vec<ConversationEntry>,
    file: Option<tokio::fs::File>,
    head: ConversationSeq,
    initialized: bool,
    closed: bool,
}
```

实现：

```rust
impl SessionLog for LocalSessionLog
```

## 10.2 conversation.log 使用 batch line

不要每个 entry 写一行。

每次 `SessionLog::append` 的整个 batch 写成一个 JSON line：

```rust
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogBatch {
    previous_head: ConversationSeq,
    new_head: ConversationSeq,
    entries: Vec<ConversationEntry>,
}
```

文件：

```text
{"previous_head":0,"new_head":1,"entries":[...]}
{"previous_head":1,"new_head":3,"entries":[...,...]}
```

原因：

- MiniCore settlement 可能一次 append 多个 entry；
- 一个 append batch 必须在物理日志中保持原子语义；
- crash 时最终半行可以整体丢弃；
- 不能出现 batch 中第一条 entry durable、第二条丢失。

## 10.3 initialize

```text
确认未 initialized
确认 Session directory 已存在
使用 temp file 写 manifest.json
flush + sync
rename
创建 conversation.log
sync
设置内存 manifest/head
返回 head=0
```

## 10.4 append

步骤：

1. 检查未 closed；
2. 检查 `expected_head == self.head`；
3. 检查 entries 非空；
4. 检查 entries first/last seq 与 expected/new head；
5. 序列化一个 `LogBatch`；
6. 追加 `\n`；
7. `write_all`；
8. `flush`；
9. `sync_data`；
10. 只有成功后更新 `self.entries` 和 `self.head`；
11. 返回 AppendReceipt。

错误分类：

```text
expected head 不匹配
    → SessionLogError::Conflict

文件不存在/权限
    → Unavailable 或 Internal

write 已开始后失败
flush 失败
sync_data 失败
    → UnknownOutcome
```

不得在 unknown 后继续使用该 LocalSessionLog。

MiniCore 会将 Session 标记 Degraded。

## 10.5 load

打开时：

1. 读取并解析 manifest.json；
2. 读取 conversation.log bytes；
3. 查找最后一个 `\n`；
4. 最终非空残片视为 crash partial tail，截断；
5. 逐行解析 `LogBatch`；
6. 验证 batch head 连续；
7. flatten entries 到内存；
8. 打开 file append；
9. 设置 head。

如果中间完整行无法解析：

```text
SessionLogError::Corrupt
```

不尝试扫描跳过。

## 10.6 read_page

使用内存 `entries`：

```text
after=None → 从第一条开始
after=Some(seq) → 从 seq 后开始
limit → 截断
next_after → 返回页最后 seq 或 None
observed_head → self.head
```

不每次重新读取文件。

## 10.7 close

```text
如果已经 closed → Ok
flush
sync_data
drop file
closed = true
```

close 失败按 MiniCore SessionLog 语义返回。

## 10.8 不实现日志压缩

v0.1 不实现：

```text
file rewrite
snapshot
index
vacuum
log rotation
```

---

# 11. Profiles

## 11.1 简单内存配置

```rust
pub struct Profiles {
    values: BTreeMap<String, Profile>,
}
```

不定义 Profile Store。

所有 Profile 从 Agent TOML 启动配置加载。

## 11.2 Profile

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub model: String,

    #[serde(default)]
    pub reasoning: ReasoningPreference,

    pub system_prompt: String,

    pub tools: Vec<String>,

    #[serde(default = "default_tool_rounds")]
    pub max_tool_rounds: u16,

    #[serde(default)]
    pub approval: ApprovalMode,

    #[serde(default)]
    pub compaction: ProfileCompaction,
}
```

## 11.3 v0.1 Profile 不是版本系统

允许：

```text
default
review
readonly
```

不实现：

```text
revision
migration
immutable history
profile database
```

修改配置后，旧 Session 下次 load 使用当前同名 Profile。

这是 v0.1 的明确简化。

SessionRecord 保存：

```text
profile name
```

MiniCore SessionManifest 仍保存实际：

```text
ModelRef
ReasoningPreference
enabled tools
system prompt
compaction config
```

如果配置变更导致当前 bindings 与 durable SessionSpec 不匹配，`SessionRuntime::load` 返回配置错误。

Agent 不自动改写历史 SessionSpec。

## 11.4 ProfileCompaction

```rust
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ProfileCompaction {
    #[default]
    Disabled,

    Model {
        trigger_tokens: u64,
        target_tokens: u64,
    },
}
```

第一阶段只要求 `Disabled`。

`Model` 模式在后续 Phase 实现。

---

# 12. Models

## 12.1 类型

```rust
pub struct Models {
    values: BTreeMap<String, Arc<dyn Model>>,
}
```

```rust
impl Models {
    pub async fn from_config(
        config: &BTreeMap<String, ModelConfig>,
    ) -> Result<Self, ModelConfigError>;

    pub fn get(
        &self,
        id: &str,
    ) -> Result<Arc<dyn Model>, ModelConfigError>;
}
```

v0.1 启动时构造全部 model adapter。

不做懒加载。

## 12.2 ModelConfig

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum ModelConfig {
    OpenAiResponses {
        model: String,
        base_url: String,
        api_key_env: String,

        physical_context_window: u32,
        output_budget_tokens: u32,
        safety_margin_tokens: u32,

        supported_reasoning: BTreeSet<ReasoningPreference>,
        supports_tools: bool,

        #[serde(default)]
        request_timeout_seconds: Option<u64>,
    },
}
```

不保存 API key 本身。

启动时从：

```text
api_key_env
```

读取。

## 12.3 ModelRef

Core `ModelRef` 使用 Agent model profile ID：

```text
openai-main
review-model
fast-model
```

不要直接使用 endpoint URL。

```rust
let model_ref: ModelRef = model_profile_id.parse()?;
```

## 12.4 上下文窗口

当前 MiniCore descriptor 没有独立 output reserve。

Agent adapter 向 Core 暴露：

```text
effective_context_window
=
physical_context_window
- output_budget_tokens
- safety_margin_tokens
```

要求：

```text
physical_context_window > output_budget_tokens + safety_margin_tokens
effective_context_window > 0
```

Provider request 使用：

```text
output_budget_tokens
```

作为上游最大输出预算。

Core 只看到安全输入窗口。

## 12.5 Reasoning

Core `ReasoningPreference`：

```text
auto
disabled
low
medium
high
```

OpenAI adapter 映射：

```text
Disabled → none 或供应商支持的最低值
Low      → low
Medium   → medium
High     → high
Auto     → 省略或使用 ModelConfig 默认
```

Provider 不支持的值必须在 ModelDescriptor 的：

```text
supported_reasoning
```

中排除。

## 12.6 不实现价格

ModelEvent Usage 正常返回 MiniCore。

Agent v0.1 不计算：

```text
美元价格
套餐价格
缓存折扣
```

---

# 13. OpenAI Responses Model

## 13.1 类型

```rust
pub struct OpenAiResponsesModel {
    descriptor: ModelDescriptor,
    client: reqwest::Client,

    model: String,
    base_url: String,
    api_key: String,
    output_budget_tokens: u32,
}
```

实现：

```rust
impl Model for OpenAiResponsesModel
```

## 13.2 请求转换

将 MiniCore：

```text
ModelRequest.messages
ModelRequest.tools
ModelRequest.reasoning
ModelRequest.limits
```

转换为 Responses request。

要求：

```text
stream = true
truncation = disabled
max_output_tokens = Agent ModelConfig.output_budget_tokens
```

不要使用 Provider 端自动丢弃历史的 truncation。

消息转换：

```text
System    → developer/system instruction
User      → user input item
Assistant → assistant output item
Tool      → function_call_output
```

工具转换：

```text
ToolSpec.name
ToolSpec.description
ToolSpec.input_schema
```

为 function tool。

Assistant ToolCall 必须保留 MiniCore `ToolCallId` 与 arguments。

## 13.3 Stream 转换

适配器必须输出 MiniCore typed stream：

```text
Text delta
Reasoning delta/summary
ToolCall started
ToolCall arguments delta
ToolCall completed
Usage
Finished
```

不要把原始 Provider JSON 暴露给 MiniCore。

## 13.4 Finish mapping

至少映射：

```text
normal completion  → Stop
function calls     → ToolCalls
max output/context → Length
content filter     → ContentFiltered
refusal            → Refused
其他               → Unknown
```

## 13.5 Error delivery

严格、保守映射。

### NotStarted

只有明确在请求发送前失败：

```text
DNS
TCP connect
TLS connect
本地请求构造
明确由 Provider 拒绝且未创建 response
```

才返回：

```rust
ModelError::not_started(...)
```

### Unknown

以下默认 Unknown：

```text
请求 body 已发送后的 timeout
HTTP connection reset
未收到首 event 的 transport failure
无法确认 Provider 是否处理
```

### Started

收到任何有效 stream event 后的错误：

```rust
ModelError::started(...)
```

### Retry

只有：

```text
NotStarted
+
明确 retryable
```

允许 MiniCore 自动 retry。

不要把所有 5xx 都标记为 NotStarted。

## 13.6 Cancellation

`ModelCallContext.cancellation` 取消时：

- 停止读取 stream；
- drop response body；
- 返回 Cancelled；
- 不创建 detached HTTP task。

## 13.7 Context overflow

Provider 请求前拒绝输入过大：

```text
ModelErrorKind::ContextOverflow
DeliveryState::NotStarted
RetryHint::Never
```

MiniCore 当前 forced compaction recovery 根据其已有语义处理。

生成中达到 max output：

```text
Finished(Length)
```

不要伪装成 NotStarted error。

## 13.8 测试

使用本地 mock HTTP server 或自定义 mock transport覆盖：

```text
text final
reasoning + text
single tool call
multiple sequential tool calls
arguments chunk split
usage
refusal
length
connect failure
timeout before stream
failure after stream start
cancellation
```

真实 API smoke test默认 ignored。

---

# 14. Workspace

## 14.1 类型

```rust
#[derive(Clone)]
pub struct Workspace {
    root: Arc<PathBuf>,
}
```

## 14.2 打开

```rust
pub async fn open(
    root: PathBuf,
) -> Result<Self, WorkspaceError>;
```

要求：

- root 必须存在；
- root 必须是 directory；
- canonicalize root；
- 保存 canonical root。

## 14.3 路径规则

所有 Tool path：

- 必须是相对路径；
- 禁止空 path，根目录读取除外；
- 禁止 absolute path；
- 禁止 `..`；
- 禁止 NUL；
- existing path canonicalize 后必须位于 root；
- new file 必须 canonicalize parent；
- 不允许通过 symlink 逃逸 root。

实现一个统一方法：

```rust
impl Workspace {
    fn resolve_existing(
        &self,
        path: &str,
    ) -> Result<PathBuf, WorkspaceError>;

    fn resolve_for_write(
        &self,
        path: &str,
    ) -> Result<PathBuf, WorkspaceError>;

    fn resolve_directory(
        &self,
        path: &str,
    ) -> Result<PathBuf, WorkspaceError>;
}
```

所有 Tool 必须复用。

## 14.4 不实现 capability hierarchy

v0.1 不定义：

```text
ReadWorkspace trait
WriteWorkspace trait
WorkspaceBackend
WorkspaceLease
```

权限由 ToolSet 和 Policy 控制。

---

# 15. Tools

## 15.1 构造

```rust
pub fn build_tools(
    profile: &Profile,
    workspace: Arc<Workspace>,
) -> Result<ToolSet, AgentError>;
```

根据 profile.tools 注册。

未知工具名直接配置错误。

每个 Tool 自己捕获：

```text
Arc<Workspace>
```

`ToolContext` 只用于：

```text
cancellation
deadline
progress
```

## 15.2 共用限制

建议 Agent tool 常量：

```text
MAX_READ_BYTES          512 KiB
MAX_WRITE_BYTES         512 KiB
MAX_PATCH_BYTES         512 KiB
MAX_COMMAND_OUTPUT      1 MiB
DEFAULT_COMMAND_TIMEOUT 120s
MAX_COMMAND_TIMEOUT     1800s
```

这些是 Agent adapter 限制，不修改 MiniCore SemanticLimits。

最终 ToolOutput 仍必须满足 MiniCore configured output limit。

## 15.3 错误输出

普通工具业务失败建议返回：

```rust
Err(ToolError::Unavailable/Internal/InvalidInvocation)
```

不要把所有失败包装成 success 文本。

但命令非零退出是一次正常执行结果：

```text
ToolExecutionOutcome::Completed
```

内容中包含 exit code。

---

# 16. `read` Tool

## 16.1 输入

```json
{
  "path": "src/lib.rs",
  "offset": 1,
  "limit": 400
}
```

字段：

```text
path    required
offset  optional，1-based line，默认 1
limit   optional，默认 400，最大 2000
```

## 16.2 文件行为

返回带行号文本：

```text
1: use ...
2: ...
```

超过限制时在末尾注明：

```text
[truncated]
```

## 16.3 目录行为

如果 path 指向 directory：

- 列出一层；
- 名称稳定排序；
- directory 后加 `/`；
- 最多 1000 项；
- 不递归。

因此不单独增加 `list` Tool。

## 16.4 二进制

检测明显二进制内容时返回工具错误或简短说明，不把原始 bytes 放入 ToolOutput。

---

# 17. `write` Tool

## 17.1 输入

```json
{
  "path": "src/new.rs",
  "content": "..."
}
```

## 17.2 行为

- 校验 write path；
- content 不超过 `MAX_WRITE_BYTES`；
- 自动创建父目录；
- 写临时文件；
- flush；
- rename 覆盖目标；
- 返回：

```text
wrote <bytes> bytes to <path>
```

不保留备份。

---

# 18. `edit` Tool

## 18.1 输入

```json
{
  "path": "src/lib.rs",
  "old_text": "...",
  "new_text": "...",
  "replace_all": false
}
```

## 18.2 行为

- 读取 UTF-8 文件；
- `old_text` 不能为空；
- 没有匹配 → error；
- `replace_all=false` 且多于一个匹配 → error；
- 替换；
- 使用 Workspace 的 atomic write；
- 返回替换次数。

不实现 regex。

---

# 19. `apply_patch` Tool

## 19.1 第一版只处理单文件

输入：

```json
{
  "path": "src/lib.rs",
  "patch": "@@ ... unified diff ..."
}
```

使用 `diffy` 或等价纯 Rust实现，将 unified patch 应用于该文件内容。

行为：

- path 单独提供；
- patch 不能修改其他文件；
- patch 必须完整应用；
- 失败不写文件；
- 成功后 atomic write；
- 返回修改前后字节数。

不实现：

```text
multi-file patch
rename
binary patch
git index
partial apply
```

模型需要多文件修改时多次调用。

---

# 20. `bash` Tool

## 20.1 输入

```json
{
  "command": "cargo test",
  "cwd": ".",
  "timeout_seconds": 120
}
```

字段：

```text
command required
cwd optional relative path
timeout_seconds optional
```

## 20.2 shell

Unix：

```text
/bin/sh -lc <command>
```

Windows：

```text
powershell.exe -NoProfile -NonInteractive -Command <command>
```

## 20.3 环境

继承 Agent 进程环境。

增加：

```text
MINICORE_AGENT=1
```

cwd 必须位于 Workspace。

## 20.4 输出

同时读取 stdout/stderr，避免 pipe deadlock。

输出格式：

```text
exit_code: 0
stdout:
...

stderr:
...
```

单流和总输出有界。

超限时截断并注明。

## 20.5 timeout / cancellation

使用：

```text
ToolContext.deadline
ToolContext.cancellation
tool input timeout
```

取最早截止。

取消时 kill direct child。

v0.1 明确限制：

> 只保证终止 direct child，不保证所有孙进程和 daemon 被完整回收。

README 必须说明。

不为 v0.1 引入 Unix process group / Windows Job Object。

---

# 21. Context

## 21.1 类型

```rust
pub struct ProjectContext {
    workspace: Arc<Workspace>,
}
```

实现：

```rust
impl ContextProvider for ProjectContext
```

## 21.2 行为

每次调用读取：

```text
<workspace>/AGENTS.md
```

不存在：

```text
返回空 ContextBundle
```

存在：

```text
返回一个 ProjectInstructions ContextBlock
```

```rust
ContextBlock {
    source: "agents-md",
    slot: ContextSlot::ProjectInstructions,
    priority: 100,
    content,
}
```

内容受 `remaining_context_budget` 和 Agent 最大字节限制约束。

如果 AGENTS.md 过大：

- 截断到预算；
- 添加 `[truncated]`；
- 不终止 Turn。

读取错误：

- Permission/IO error → `ContextError::Unavailable`；
- cancellation → Cancelled；
- deadline → DeadlineExceeded。

## 21.3 不实现

```text
递归 AGENTS.md
Skills
Memory
RAG
Git status
IDE selection
post-compaction hook
```

以后可将 `ProjectContext` 改成组合 Context，但 v0.1 不预建 Context source trait。

---

# 22. Policy

## 22.1 ApprovalMode

```rust
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    Auto,
    #[default]
    Ask,
    ReadOnly,
}
```

## 22.2 工具分类

```text
read         read-only
write        mutating
edit         mutating
apply_patch  mutating
bash         mutating
```

## 22.3 决策

### Auto

```text
所有已注册 Tool → Allow
```

### Ask

```text
read → Allow
其他 → RequireApproval
```

### ReadOnly

```text
read → Allow
其他 → Deny
```

## 22.4 类型

```rust
pub struct Policy {
    mode: ApprovalMode,
}
```

实现 MiniCore `ToolPolicy`。

approval prompt：

```text
Allow tool `<name>` for this call?
```

不把完整 arguments 放入 prompt，以免敏感信息进入事件。

## 22.5 不持久化审批

Core answer：

```text
AllowOnce
Deny
```

Agent 不实现：

```text
allow for session
allow for project
approval cache
```

---

# 23. Compaction

## 23.1 Phase 1

第一版主路径：

```rust
CompactionConfig::Disabled
```

先跑通完整 Model/Tool Loop。

## 23.2 Phase 2 可选实现

如果本轮需要长 Session，再实现：

```rust
pub struct ModelCompaction {
    model: Arc<dyn Model>,
}
```

它使用同一个 Model adapter进行一次无工具总结。

固定提示：

```text
Summarize the durable conversation for continuation.
Preserve goals, constraints, decisions, modified files,
pending work, errors, and important tool results.
Do not invent facts.
```

返回：

```rust
CompactionProposal {
    through_seq,
    summary,
}
```

必须：

- 使用 request.candidate；
- 只生成 summary；
- 不修改 Store；
- 不自行追加 Conversation；
- 响应 cancellation/deadline。

但 v0.1 验收可以在 Compaction disabled 下通过。

---

# 24. AgentConfig

## 24.1 类型

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub data_dir: PathBuf,

    #[serde(default = "default_event_capacity")]
    pub event_capacity: usize,

    pub default_profile: String,

    pub profiles: BTreeMap<String, Profile>,
    pub models: BTreeMap<String, ModelConfig>,

    #[serde(default)]
    pub kernel: KernelOverrides,
}
```

```rust
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KernelOverrides {
    pub command_capacity: Option<usize>,
    pub runner_capacity: Option<usize>,
    pub event_capacity: Option<usize>,

    pub model_call_timeout_seconds: Option<u64>,
    pub tool_call_timeout_seconds: Option<u64>,
    pub context_timeout_seconds: Option<u64>,
}
```

其余使用 `KernelConfig::default_checked()`。

## 24.2 示例配置

```toml
data_dir = "./.minicore-agent"
default_profile = "coding"
event_capacity = 512

[profiles.coding]
model = "main"
reasoning = "medium"
system_prompt = """
You are a coding agent. Inspect the workspace, use tools when useful,
make focused changes, run relevant checks, and explain the result.
"""
tools = ["read", "write", "edit", "apply_patch", "bash"]
max_tool_rounds = 32
approval = "ask"

[profiles.coding.compaction]
mode = "disabled"

[profiles.readonly]
model = "main"
reasoning = "medium"
system_prompt = "Review the workspace without modifying it."
tools = ["read", "bash"]
max_tool_rounds = 16
approval = "read_only"

[profiles.readonly.compaction]
mode = "disabled"

[models.main]
provider = "open_ai_responses"
model = "your-model-id"
base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"

physical_context_window = 200000
output_budget_tokens = 24000
safety_margin_tokens = 4000

supported_reasoning = ["auto", "disabled", "low", "medium", "high"]
supports_tools = true
request_timeout_seconds = 600
```

## 24.3 校验

启动时一次性校验：

- data dir；
- default profile 存在；
- Profile model 存在；
- Profile tools 全部已知；
- context window 预算合法；
- API key env 存在；
- max tool rounds 非零；
- Agent event capacity 非零。

配置错误直接启动失败。

---

# 25. SessionSpec 和 Bindings 构造

## 25.1 Create

从 Profile：

```rust
let spec = SessionSpec::new(
    model.descriptor().model_ref.clone(),
    profile.reasoning,
    BoundedText::new(profile.system_prompt.clone())?,
    enabled_tool_names,
    profile.max_tool_rounds,
    compaction_config,
)?;
```

具体构造签名以 MiniCore 当前 Public API 为准。

## 25.2 Bindings

```rust
let bindings = SessionBindings::new(
    model,
    tool_set,
    Some(Arc::new(policy)),
    Some(Arc::new(context)),
    compaction,
);
```

Profile 没有 tools 时：

- ToolSet empty；
- policy 可以 None。

## 25.3 Runtime options

```rust
let options = SessionRuntimeOptions::new(
    kernel_config,
    bindings,
    self.task_runtime.clone(),
)?;
```

Agent 使用一个 Tokio runtime handle 运行全部 SessionRuntime。

---

# 26. RPC 协议

## 26.1 Transport

v0.1 使用：

```text
JSON-RPC 2.0
+
NDJSON
+
stdin/stdout
```

一行一个 JSON frame。

stdout 只能输出 RPC。

日志全部写 stderr。

## 26.2 为什么不用 WebSocket

第一版测试最简单：

```bash
minicore-agent --config agent.toml --stdio
```

客户端 spawn process，通过 stdin/stdout 调用。

未来可以在相同 Agent API 外增加 WebSocket，不修改 Agent。

## 26.3 Request

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "session.create",
  "params": {}
}
```

ID 支持：

```text
integer
string
```

不支持 batch request。

## 26.4 Response

成功：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {}
}
```

失败：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32001,
    "message": "session not found",
    "data": {
      "kind": "session_not_found"
    }
  }
}
```

## 26.5 Event notification

```json
{
  "jsonrpc": "2.0",
  "method": "agent.event",
  "params": {
    "type": "output_delta",
    "data": {}
  }
}
```

事件没有 request ID。

---

# 27. RPC Methods

## 27.1 `agent.ping`

Params：

```json
{}
```

Result：

```json
{
  "version": "0.1.0"
}
```

## 27.2 `agent.shutdown`

Params：

```json
{}
```

Result：

```json
{
  "ok": true
}
```

server 完成 Agent shutdown 后退出进程。

## 27.3 `profile.list`

Result：

```json
{
  "profiles": [
    {
      "id": "coding",
      "model": "main",
      "reasoning": "medium",
      "tools": ["read", "write", "edit", "apply_patch", "bash"],
      "approval": "ask"
    }
  ]
}
```

## 27.4 `model.list`

不返回 secret/base URL。

```json
{
  "models": [
    {
      "id": "main",
      "model_ref": "main",
      "context_window": 172000,
      "supports_tools": true,
      "supported_reasoning": ["auto", "disabled", "low", "medium", "high"]
    }
  ]
}
```

## 27.5 `session.list`

Result：

```json
{
  "sessions": [
    {
      "session_id": "...",
      "title": "My task",
      "profile": "coding",
      "workspace": "/path",
      "loaded": true,
      "created_at": "...",
      "updated_at": "..."
    }
  ]
}
```

## 27.6 `session.create`

Params：

```json
{
  "workspace": "/absolute/project/path",
  "profile": "coding",
  "title": "Optional title"
}
```

Result：

```json
{
  "session": { ... }
}
```

Create 后 Session 保持 loaded。

## 27.7 `session.open`

Params：

```json
{
  "session_id": "..."
}
```

如果已经 loaded：

```text
直接返回当前 SessionInfo
```

不报错。

## 27.8 `session.close`

Params：

```json
{
  "session_id": "..."
}
```

关闭 active Turn并执行 Core shutdown。

## 27.9 `session.delete`

要求 Session 未 loaded。

Params：

```json
{
  "session_id": "..."
}
```

## 27.10 `session.state`

Params：

```json
{
  "session_id": "..."
}
```

返回当前 MiniCore SessionState 的 RPC DTO。

## 27.11 `session.transcript`

Params：

```json
{
  "session_id": "...",
  "after": null,
  "limit": 100
}
```

Session 必须 loaded。

v0.1 不实现 unloaded transcript 直接读取 Store。

## 27.12 `turn.send`

Params：

```json
{
  "session_id": "...",
  "text": "Inspect the project and run tests."
}
```

Result：

```json
{
  "turn": {
    "session_id": "...",
    "instance_id": "...",
    "turn_id": "..."
  }
}
```

快速返回，不等待模型完成。

## 27.13 `turn.cancel`

Params：

```json
{
  "session_id": "...",
  "instance_id": "...",
  "turn_id": "..."
}
```

Result：

```json
{
  "cancelled": true
}
```

旧 instance 或错误 Turn 返回 domain error。

## 27.14 `turn.wait`

Params：

```json
{
  "session_id": "...",
  "instance_id": "...",
  "turn_id": "..."
}
```

Result 为 durable TurnOutcome。

RpcServer 对这个方法：

1. 从 Agent clone TurnHandle；
2. spawn waiter task；
3. request reader 继续处理其他请求；
4. waiter 完成后通过 outbound writer channel发送原 ID response。

因此 response 可以乱序到达。

## 27.15 `interaction.answer`

Params：

```json
{
  "session_id": "...",
  "interaction_id": "...",
  "answer": {
    "type": "approval",
    "decision": "allow_once"
  }
}
```

或：

```json
{
  "answer": {
    "type": "approval",
    "decision": "deny"
  }
}
```

Tool input：

```json
{
  "answer": {
    "type": "text",
    "text": "..."
  }
}
```

映射到 MiniCore typed `InteractionAnswer`。

---

# 28. RPC Server

## 28.1 类型

```rust
pub struct RpcServer {
    agent: Agent,

    outbound_tx: mpsc::Sender<RpcOutbound>,
    outbound_rx: mpsc::Receiver<RpcOutbound>,
}
```

## 28.2 输出单写者

启动一个 writer task：

```text
RpcResponse
AgentEvent notification
async turn.wait response
```

全部发送到一个 bounded outbound channel。

只有 writer task 写 stdout。

避免响应和事件 JSON 交叉。

## 28.3 Request loop

```text
read line
→ reject oversized line
→ parse request
→ dispatch
→ send response
→ next line
```

除 `turn.wait` 外请求按顺序处理。

## 28.4 行长度

最大 request line：

```text
1 MiB
```

超过直接返回 parse/invalid request error，并可关闭 stdin loop。

## 28.5 EOF

stdin EOF：

```text
Agent.shutdown
→ close outbound
→ await writer
→ exit
```

## 28.6 Ctrl-C

binary 捕获：

```text
SIGINT / Ctrl-C
```

执行相同 shutdown。

## 28.7 不实现认证

stdio RPC 与启动进程的用户同权限。

---

# 29. RPC Error

## 29.1 标准 code

```text
-32700 parse error
-32600 invalid request
-32601 method not found
-32602 invalid params
-32603 internal error
```

## 29.2 Domain code

```text
-32001 session_not_found
-32002 session_not_loaded
-32003 session_busy
-32004 session_closed
-32005 invalid_state
-32006 interaction_not_found
-32007 turn_not_found
-32008 profile_not_found
-32009 model_not_found
-32010 workspace_error
-32011 store_error
-32012 provider_error
-32013 core_error
```

`data`：

```json
{
  "kind": "core_error",
  "retryable": false
}
```

不发送：

```text
API key
完整 Tool arguments
完整 raw provider response
内部 filesystem path之外的 secret
panic payload
```

---

# 30. Error 类型

## 30.1 AgentError

```rust
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("invalid configuration")]
    InvalidConfiguration,

    #[error("session not found")]
    SessionNotFound,

    #[error("session is not loaded")]
    SessionNotLoaded,

    #[error("session is already loaded")]
    SessionAlreadyLoaded,

    #[error("turn not found")]
    TurnNotFound,

    #[error("interaction not found")]
    InteractionNotFound,

    #[error("profile not found")]
    ProfileNotFound,

    #[error("model not found")]
    ModelNotFound,

    #[error("workspace error")]
    Workspace(#[from] WorkspaceError),

    #[error("store error")]
    Store(#[from] StoreError),

    #[error("core error")]
    Core(CoreErrorView),

    #[error("internal error")]
    Internal,
}
```

不为每个 MiniCore error variant重复建立一层复杂枚举。

使用一个转换函数提取：

```text
kind
retryable
safe message
```

## 30.2 不捕获所有 panic

RPC request dispatch 可以在 task boundary 捕获 panic并返回 internal error，但不建立全局 panic recovery系统。

---

# 31. Binary

## 31.1 CLI

只实现：

```bash
minicore-agent --config ./agent.toml --stdio
```

可选：

```bash
minicore-agent --version
```

不实现交互式 CLI。

## 31.2 main

流程：

```text
解析 args
初始化 tracing 到 stderr
读取 AgentConfig
Agent::open
Agent::take_events
启动 RpcServer
等待 stdio EOF / agent.shutdown / Ctrl-C
Agent shutdown
退出
```

## 31.3 Tokio

```rust
#[tokio::main(flavor = "multi_thread")]
```

默认 worker threads。

全部 SessionRuntime 使用：

```rust
tokio::runtime::Handle::current()
```

---

# 32. 文件级实施方案

## 32.1 `src/lib.rs`

Public export：

```rust
pub use agent::{
    Agent,
    AnswerInteraction,
    CreateSession,
    GetTranscript,
    SendMessage,
    SessionInfo,
    TurnRef,
};

pub use config::AgentConfig;
pub use error::AgentError;
pub use event::{AgentEvent, AgentEventStream};
```

不 export：

```text
Store internals
LoadedSession
OpenAI raw DTO
Workspace path resolver internals
RpcServer internals
```

## 32.2 `src/agent.rs`

实现：

```text
Agent struct
open
take_events
session methods
turn methods
answer
transcript
shutdown
Session capability assembly
Core error mapping
```

可将 capability assembly 提取为一个私有方法：

```rust
async fn build_session(
    &self,
    record: &SessionRecord,
) -> Result<SessionBuild, AgentError>;
```

不要新增 `Assembler` 类型。

## 32.3 `src/sessions.rs`

实现：

```text
Sessions
LoadedSession
insert
get/get_mut
remove
list loaded
shutdown_all
lazy active Turn cleanup
```

不放 Store/Profile/Workspace 逻辑。

## 32.4 `src/store.rs`

实现：

```text
Store
SessionRecord
LocalSessionLog
LogBatch
atomic metadata write
append batch
tail repair
read_page
close
```

## 32.5 `src/models.rs`

实现：

```text
Models
ModelConfig
from_config
get
safe ModelInfo
```

## 32.6 `src/models/openai.rs`

实现：

```text
OpenAiResponsesModel
request DTO
stream event DTO/parser
Model trait
error mapping
usage mapping
```

Raw provider DTO全部 private。

## 32.7 `src/workspace.rs`

实现：

```text
Workspace
open
resolve_existing
resolve_for_write
resolve_directory
read_text
write_atomic
```

Tool 不重复写路径逻辑。

## 32.8 `src/tools/mod.rs`

实现：

```text
build_tools
known tool names
mutating tool classification
ToolSet registration
```

## 32.9 `src/context.rs`

实现：

```text
ProjectContext
AGENTS.md read
ContextProvider
```

## 32.10 `src/policy.rs`

实现：

```text
ApprovalMode
Policy
ToolPolicy
```

## 32.11 `src/rpc/protocol.rs`

只定义 wire DTO：

```text
RpcRequest
RpcResponse
RpcError
RpcOutbound
method params/result
AgentEvent notification
```

## 32.12 `src/rpc/server.rs`

实现：

```text
stdio reader
writer task
dispatch
turn.wait spawned response
shutdown
```

---

# 33. 实施阶段

## Phase 0：骨架

提交：

```text
feat(agent): add rpc-first agent crate skeleton
```

完成：

- Cargo；
- config；
- error；
- Agent 空架构；
- RPC ping；
- tests compile。

## Phase 1：Store

提交：

```text
feat(store): persist minicore sessions in an append-only local log
```

完成：

- Store；
- SessionRecord；
- LocalSessionLog；
- batch line；
- tail repair；
- tests。

## Phase 2：Fake Agent Loop

提交：

```text
feat(agent): run multiple minicore session runtimes
```

完成：

- Sessions；
- Agent create/open/close/send/cancel/answer/transcript；
- Fake Model；
- Fake Tool；
- event/state pump；
- restart test。

此阶段不依赖真实 Provider。

## Phase 3：Workspace 和 Tools

建议按工具拆小提交：

```text
feat(workspace): add rooted local workspace
feat(tools): add read and write tools
feat(tools): add edit and apply-patch tools
feat(tools): add cancellable bash tool
```

## Phase 4：Context 和 Policy

```text
feat(context): inject AGENTS.md project instructions
feat(policy): add minimal approval modes
```

## Phase 5：RPC

```text
feat(rpc): expose agent operations over stdio json-rpc
```

完成全部 methods、notifications 和 process test。

## Phase 6：OpenAI Model

```text
feat(model): add an OpenAI Responses model adapter
```

先 mock server测试，再真实 ignored smoke。

## Phase 7：收口

```text
docs(agent): document rpc protocol and local limitations
test(agent): close the first complete coding loop
```

---

# 34. 测试策略

## 34.1 不访问真实网络的默认测试

默认：

```bash
cargo test --all-targets
```

必须：

- 不要求 API key；
- 不访问互联网；
- 不修改用户真实目录；
- 使用 temp directory；
- 使用 Fake Model 或 mock HTTP server。

## 34.2 Store tests

覆盖：

```text
initialize
manifest roundtrip
single append
multi-entry atomic batch
expected head conflict
read pages
close
reload
partial final line truncate
middle corrupt line reject
unknown write outcome fake
```

## 34.3 Workspace tests

覆盖：

```text
relative file
directory read
absolute path reject
.. reject
symlink escape reject
new file parent resolution
atomic write
```

Unix symlink test使用 cfg(unix)。

## 34.4 Tool tests

覆盖：

```text
read lines
read directory
read truncation
write
edit no match
edit ambiguous
edit replace all
patch success
patch failure no write
bash success
bash nonzero
bash stdout/stderr
bash timeout
bash cancellation
bash output truncation
```

## 34.5 Agent loop tests

### Test A：文本完成

```text
User
→ Model final text
→ Turn Completed
```

### Test B：read tool

```text
User
→ Model ToolCall(read)
→ read ToolResult
→ Model final text
```

### Test C：write approval

```text
User
→ Model ToolCall(write)
→ Policy RequireApproval
→ Agent receives InteractionRequested
→ RPC answer AllowOnce
→ write
→ Model final
```

### Test D：deny

```text
write approval Deny
→ ToolResult Denied
→ Model continues
```

### Test E：bash cancel

```text
Model calls long bash
→ turn.cancel
→ Turn Cancelled
→ Session Idle
```

### Test F：restart

```text
start Turn
→ terminate Agent without graceful finish
→ reopen Agent
→ open Session
→ MiniCore repairs unfinished Turn
→ Session Idle
→ new Turn works
```

### Test G：two Sessions

```text
Session A Model pending
Session B Model pending
cancel A
B unaffected
```

## 34.6 RPC process test

spawn binary：

```text
stdin piped
stdout piped
stderr piped
```

测试：

```text
agent.ping
session.create
turn.send
receive output_delta
receive turn_finished
session.transcript
session.close
agent.shutdown
process exits 0
```

## 34.7 Provider smoke

```rust
#[ignore]
#[tokio::test]
async fn openai_live_smoke()
```

只在：

```text
OPENAI_API_KEY
MINICORE_AGENT_LIVE_MODEL
```

存在时执行。

---

# 35. 验收矩阵

## Agent API

| ID | 验收 |
|---|---|
| AG-01 | Agent 成功加载 TOML 配置 |
| AG-02 | Agent event stream 只能取一次 |
| AG-03 | list sessions 不加载 SessionRuntime |
| AG-04 | create 后 Session loaded 且 Idle |
| AG-05 | open 已 loaded Session 幂等返回 |
| AG-06 | close 执行 SessionRuntime shutdown |
| AG-07 | delete loaded Session 被拒绝 |
| AG-08 | shutdown 关闭全部 loaded Session |

## Store

| ID | 验收 |
|---|---|
| AG-09 | Session metadata 和 Core manifest 分离 |
| AG-10 | 一个 append batch 对应一个 log line |
| AG-11 | partial final line 可截断 |
| AG-12 | middle corruption 被拒绝 |
| AG-13 | expected head conflict 正确返回 |
| AG-14 | append receipt 与 entries 一致 |
| AG-15 | reload 后 transcript 一致 |

## Loop

| ID | 验收 |
|---|---|
| AG-16 | User → Model → Final |
| AG-17 | User → Model → Tool → Model → Final |
| AG-18 | 多个顺序 ToolCall 可执行 |
| AG-19 | TurnHandle durable outcome 被 Agent 转发 |
| AG-20 | cancel exact Turn |
| AG-21 | 两个 Session 可并发 |
| AG-22 | 一个 Session Busy 不影响另一个 |
| AG-23 | restart 后 unfinished Turn 被 Core repair |

## Tools

| ID | 验收 |
|---|---|
| AG-24 | read 不能越过 Workspace |
| AG-25 | write 使用 atomic rename |
| AG-26 | edit ambiguous 时不写文件 |
| AG-27 | patch 失败不产生部分修改 |
| AG-28 | bash 支持 stdout/stderr/exit code |
| AG-29 | bash timeout/cancel 终止 direct child |
| AG-30 | Tool output 遵守限制 |

## Context/Policy

| ID | 验收 |
|---|---|
| AG-31 | AGENTS.md 进入 ProjectInstructions |
| AG-32 | 无 AGENTS.md 返回空 Context |
| AG-33 | Auto 模式允许工具 |
| AG-34 | Ask 模式对 mutating Tool 请求审批 |
| AG-35 | ReadOnly 模式拒绝 mutating Tool |
| AG-36 | approval 不跨重启恢复 |

## RPC

| ID | 验收 |
|---|---|
| AG-37 | 一行一个 JSON-RPC frame |
| AG-38 | stdout 不出现日志 |
| AG-39 | response 与 event 共用单 writer |
| AG-40 | turn.send 快速返回 |
| AG-41 | turn.wait 不阻塞 request reader |
| AG-42 | turn.finished notification 使用 TurnHandle outcome |
| AG-43 | stdin EOF 触发 shutdown |
| AG-44 | agent.shutdown 正常退出 |
| AG-45 | invalid method/params 返回标准 error |

## Model

| ID | 验收 |
|---|---|
| AG-46 | ModelRef 使用 config model ID |
| AG-47 | effective context window 扣除输出预算和安全余量 |
| AG-48 | request truncation disabled |
| AG-49 | Tool schema 正确转换 |
| AG-50 | streamed ToolCall arguments 正确组装 |
| AG-51 | 收到 event 后的失败标记 Started |
| AG-52 | 不确定请求状态标记 Unknown |
| AG-53 | cancellation 停止 stream |
| AG-54 | Usage 正确映射 |

---

# 36. 代码质量要求

## 36.1 简洁原则

优先：

```text
具体 struct
普通函数
清晰 enum
直接 match
小模块
行为测试
```

避免：

```text
过多 trait
Factory
Repository
Manager graph
Builder 套 Builder
Service 层套 Client 层
万能 Context
万能 Extension
Hook bus
状态机框架
宏生成 RPC
```

## 36.2 防御边界

必须严格的地方：

```text
Workspace path
SessionLog durable append
Store corruption
Provider delivery state
Tool output bounds
RPC JSON parsing
SessionRuntime shutdown
secret redaction
```

可以简单的地方：

```text
单 RPC client
顺序 request dispatch
单 Agent/data_dir
Profile 从启动配置读取
Agent event 不 replay
approval 不恢复
Loaded Session 不 idle eviction
shutdown sessions 顺序执行
```

## 36.3 锁

Agent v0.1 不需要全局 async Mutex。

RPC server 顺序拥有：

```rust
&mut Agent
```

不要为了未来 GUI 并发提前使用 DashMap 或复杂 lock graph。

## 36.4 错误

- 对外错误短且稳定；
- 详细错误写 tracing stderr；
- 不在 RpcError 中返回 Debug dump；
- 不为每个下游错误复制几十个 variant。

## 36.5 注释

只注释：

- 所有权；
- durable 语义；
- 为什么不能重试；
- 为什么使用 batch line；
- 已知平台限制。

不要逐行解释显而易见代码。

---

# 37. README 必须说明

```text
MiniCore Agent v0.1 是本地单客户端 RPC Agent
一个 Agent 进程管理多个 MiniCore SessionRuntime
同一 data_dir 不支持多进程同时使用
重启不恢复 active Turn、approval 和 UI 状态
SessionRuntime::load 会恢复 durable Conversation 并 repair unfinished Turn
bash 只保证 kill direct child
stdio stdout 专用于 JSON-RPC
日志写 stderr
默认工具和权限
如何配置 Provider
如何运行 smoke test
```

---

# 38. 完成定义

只有同时满足以下条件，v0.1 才算完成：

```text
一个 crate 同时提供 Agent library 和 stdio RPC binary
Agent 能管理多个 SessionRuntime
本地 Store 满足 MiniCore SessionLog durable batch contract
create/open/close/delete/list 可用
turn.send/cancel/wait 可用
interaction.answer 可用
transcript 可用
read/write/edit/apply_patch/bash 可用
Workspace path 不可逃逸
AGENTS.md 可注入
Policy approval 可完成
一个真实 OpenAI Responses adapter 可用
RPC 可流式收到 output/tool/interaction/turn events
默认离线测试不访问网络
真实 Provider smoke 可手动运行
重启后可以从 durable SessionLog load 并继续新 Turn
不实现多客户端同步
不实现瞬时状态恢复
不实现插件系统
不修改 MiniCore Runtime
```

最终调用示例：

```text
spawn minicore-agent --stdio
→ session.create
→ turn.send
→ output/tool event
→ interaction.answer（需要时）
→ turn.finished
→ session.transcript
→ session.close
→ agent.shutdown
```

---

# 39. 交给代码 Agent 的执行指令

请根据本文档实现 `minicore-agent` v0.1。文档在当前目录下的md文件

初始化本仓库为本地git仓库

minicore-runtime在本机目录，分支dev为最新分支：/Users/zzq/Develops/minicore-runtime

使用2个固定subagent来实施
implementer：使用gpt-5.6-luna，思考级别max，来实施和执行
reviewer：使用gpt-5.6-luna，思考级别max，来review implement的代码
两者循环，直到代码符合要求

你自己来调度安排，并对reviewer最终的结果进行复审

执行顺序：

1. 固定并验证 MiniCore v0.3 Public API。
2. 创建单 crate，不拆 workspace。
3. 先实现 Store 和 Fake Model 的完整 Agent Loop。
4. 再实现 Workspace、五个 Tool、Context 和 Policy。
5. 再实现 stdio JSON-RPC。
6. 最后实现真实 OpenAI Responses Model。
7. 每个 Phase 独立提交并保持测试通过。
8. 不创建插件框架、Factory/Repository 分层或多客户端 EventHub。
9. 不修改 MiniCore 来迁就 Agent 实现；能力通过现有 Ports 注入。
10. 不删除 MiniCore 的 durable、cancellation 或 error semantics。
11. 不把 API key、Tool arguments 和 raw Provider error 输出到 RPC。
12. 最终报告：
    - 起始和最终 commit；
    - 文件清单；
    - RPC methods；
    - Tool 清单；
    - Store format；
    - 测试结果；
    - 已知限制；
    - 未实现项。
