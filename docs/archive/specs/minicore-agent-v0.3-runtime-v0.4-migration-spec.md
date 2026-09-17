# MiniCore Agent v0.3：Runtime v0.4 迁移与 Session Ownership 重构实施规格

## 0. 文档定位

本文档用于指导代码 Agent 对当前 `minicore-agent` 进行一次明确的 breaking migration，使其适配新的 `minicore-runtime 0.4`。

新的分层已经确定：

```text
minicore-runtime
    只负责一次 live AgentLoop

minicore-agent
    负责 Session、完整 History、JSONL、长期配置、Workspace、Model、Tool 和 RPC

TUI / GUI / CLI
    通过 Agent Rust API 或 stdio JSON-RPC 使用 minicore-agent
```

本次开发的目标不是增加更多产品功能，而是：

1. 删除 `minicore-agent` 对 Runtime v0.3 Session API 的全部依赖；
2. 让 Agent 层成为 Session 和 History 的唯一 owner；
3. 使用一次性的 `AgentLoop` 执行每个用户任务；
4. 完成多轮会话、流式事件、Tool、取消、Interaction、Steer 和模型热更新；
5. 将旧的 durable SessionLog 改为简单的 Agent-owned JSONL；
6. 大幅删除已经失去意义的适配、状态同步与持久化协议；
7. 为后续 TUI 开发提供稳定、清楚、最小的 Agent API。

本次只修改：

```text
minicore-agent
```

明确不修改：

```text
minicore-runtime
TUI / GUI / CLI
其他 Provider 仓库
其他独立工具仓库
```

---

# 1. 开发基线

## 1.1 `minicore-agent`

```text
repository:
https://github.com/zqcli/minicore-agent

branch:
dev

reviewed commit:
6d5e963031159c458212a92c690e515a2ac3761b
```

当前版本：

```text
0.2.0
```

当前代码仍依赖 Runtime v0.3 的：

```text
SessionRuntime
SessionHandle
TurnHandle
SessionSpec
SessionManifest
SessionLog
ConversationEntry
ConversationSeq
TranscriptPage
SessionEventStream
```

这些类型在 Runtime v0.4 已经删除，因此本次不是局部适配，而是 Agent Session 层的 ownership 重构。

## 1.2 `minicore-runtime`

```text
repository:
https://github.com/zqcli/minicore-runtime

branch:
refactor/v0.4-flex-agent-loop

required revision:
87f3cf92b9b5980b0f468174a319cf53427d858e

runtime version:
0.4.0
```

该 revision 已包含：

```text
ToolOutput checked-construction 收口
非法 terminal response 在写入 History 前拒绝
```

开发时必须 pin 到这个 revision，除非 Runtime 分支后来产生经过明确 Review 的新 commit。

## 1.3 开始开发前

```bash
git fetch --all --prune
git switch dev
git pull --ff-only
git rev-parse HEAD
git status --short
```

然后创建独立开发分支：

```bash
git switch -c refactor/v0.3-runtime-v0.4
```

实际分支名称可以调整，但不能直接在未保护的旧版本分支上覆盖开发。

如果 `dev` HEAD 已经前进：

1. 检查本文要求是否已经实现；
2. 保留后续正确实现；
3. 不机械恢复旧代码；
4. 最终报告记录实际起始 HEAD。

---

# 2. 版本和兼容策略

## 2.1 Agent 版本

目标版本：

```text
minicore-agent 0.3.0
```

这是 breaking release，因为：

```text
Runtime public API 完全变化
TurnRef wire shape变化
Transcript API替换为History API
Session JSONL格式变化
旧Session数据不再由新Runtime读取
新增Steer和Session Update
```

## 2.2 不保留 Runtime v0.3 兼容层

最终代码中不得出现：

```text
type SessionRuntime = ...
旧Runtime feature flag
Runtime v0.3和v0.4双依赖
SessionLog compatibility adapter
ConversationEntry到HistoryItem的运行时桥接
旧Manifest reader继续参与Session执行
```

## 2.3 旧 Agent 数据

本阶段不自动迁移 v0.2 Session 数据。

旧格式：

```text
session.json
manifest.json
conversation.log
```

新格式：

```text
session.json
history.jsonl
```

新版本：

- 不覆盖旧 `conversation.log`；
- 不尝试把旧 Manifest 转换为新 History；
- `session.list` 可以跳过旧格式目录；
- 显式 `session.open` 返回 Store/UnsupportedFormat；
- 不删除旧目录；
- 后续如果确实需要迁移，单独实现离线 migration command。

不要把旧格式迁移逻辑塞进正常 Session open 路径。

---

# 3. 目标架构

```text
┌──────────────────────────────────────────────┐
│              TUI / GUI / CLI                 │
│                                              │
│  input / rendering / keymap / navigation     │
└──────────────────────┬───────────────────────┘
                       │ Rust API / stdio RPC
┌──────────────────────▼───────────────────────┐
│               minicore-agent                 │
│                                              │
│  Agent                                       │
│  ├── Models                                  │
│  ├── Profiles                                │
│  ├── Store                                   │
│  ├── AgentEventStream                        │
│  └── Sessions                                │
│      └── Session                             │
│          ├── SessionRecord                   │
│          ├── Workspace                       │
│          ├──完整 History                     │
│          ├──长期 ExecutionConfig             │
│          ├──LoopOptions                      │
│          ├──Option<ActiveLoop>               │
│          └──JSONL I/O serialization          │
└──────────────────────┬───────────────────────┘
                       │ Rust API
┌──────────────────────▼───────────────────────┐
│             minicore-runtime 0.4             │
│                                              │
│  AgentLoop                                   │
│  ├──当前Loop working delta                  │
│  ├──Model Request                           │
│  ├──Tool batch                              │
│  ├──pending steer                           │
│  ├──pending ExecutionConfig                 │
│  ├──cancel / interaction                    │
│  ├──LoopEventStream                         │
│  └──LoopReport                              │
└──────────────────────────────────────────────┘
```

---

# 4. 核心职责边界

## 4.1 `minicore-runtime`

只负责：

```text
一次AgentLoop
一个Loop内多次Model Request
Model stream
ToolCall / ToolResult
ToolPolicy
Interaction
Steer
request-boundary ExecutionConfig update
Cancellation
best-effort LoopEvent
LoopReport
当前Loop局部一致性
```

不负责：

```text
Session ID
Session列表
Session create/open/close/delete
跨用户消息的完整History
JSONL
Session metadata
Workspace
Profile
Model registry
持久化Summary
RPC
```

## 4.2 `minicore-agent::Session`

负责：

```text
一个产品Session
多个用户Prompt
完整History
当前Model和reasoning
System prompt
Tool列表
Approval模式
Workspace
LoopOptions
当前ActiveLoop
LoopReport持久化
JSONL加载
```

## 4.3 顶层 `Agent`

负责：

```text
多个Session
Models
Profiles
Store root
Session生命周期
ExecutionConfig装配
全局AgentEvent
RPC入口
```

## 4.4 TUI

负责：

```text
展示
用户输入
模型选择UI
reasoning选择UI
Session选择UI
流式buffer
Tool卡片
Interaction弹窗
Event gap提示
```

TUI 不直接引用 `minicore-runtime`。

---

# 5. 不变量

重构完成后必须保持以下不变量。

## 5.1 单一 History owner

```text
完整Session History
只能由 minicore-agent::Session 持有
```

Runtime：

```text
接收Arc<[HistoryItem]>
只返回LoopReport::appended
```

不得在 Agent 和 Runtime 之间长期维护两份完整 Transcript。

## 5.2 一个用户任务一个 AgentLoop

```text
turn.send
→ 创建一个新的AgentLoop

该Loop完成
→ 将appended合并到Session History

下一次turn.send
→ 再创建一个新的AgentLoop
```

一个 `AgentLoop` 不能重复 prompt。

## 5.3 一个 Session 同时最多一个 ActiveLoop

如果 Session 已经有未完成的 ActiveLoop：

```text
turn.send
→ SessionBusy
```

不排队，不自动 cancel，不创建 follow-up queue。

## 5.4 Runtime config boundary

运行中更新配置：

```text
不影响当前in-flight Model Request
不影响该Request产生的Tool batch
下一次Model Request使用新配置
```

## 5.5 Session config boundary

Session 长期设置属于 Agent：

```text
Idle时更新
→ 下一次AgentLoop使用

Running时更新
→ 持久化Session设置
→ 调用LoopHandle::update
→ 下一个Model Request使用
```

## 5.6 Steer

```text
turn.steer
→ Runtime有界FIFO queue
→ 下一个Model Request前应用
→ 已应用后进入LoopReport::appended
→ Agent成功持久化后进入Session History
```

Agent 不增加第二套 steer queue。

## 5.7 Product completion

```text
Runtime LoopReport
≠
Agent Turn完成
```

Agent Turn完成顺序：

```text
Runtime join
→ 生成可持久化History
→ append history.jsonl
→ 更新Session内存History
→ 发布Agent级completion
```

`turn.wait` 等待的是 Agent 级 completion，而不是直接等待 `LoopHandle::wait()`。

---

# 6. 最终模块结构

推荐最终目录：

```text
src/
├── lib.rs
├── main.rs
├── agent.rs
├── config.rs
├── error.rs
├── event.rs
├── history.rs
├── ids.rs
├── models.rs
├── policy.rs
├── profiles.rs
├── prompt.rs
├── sessions.rs
├── store.rs
├── workspace.rs
│
├── models/
│   ├── openai.rs
│   └── openai/
│       └── tests.rs
│
├── rpc/
│   ├── mod.rs
│   ├── protocol.rs
│   ├── server.rs
│   └── server/
│       └── tests.rs
│
└── tools/
    ├── mod.rs
    ├── read.rs
    ├── write.rs
    ├── edit.rs
    ├── apply_patch.rs
    └── bash.rs
```

删除：

```text
src/context.rs
```

替换为：

```text
src/prompt.rs
```

不要新增：

```text
services/
repositories/
domain/
application/
managers/
ports/
adapters/
session_actor/
supervisor/
```

当前项目规模不需要这些分层。

---

# 7. Cargo 修改

## 7.1 `Cargo.toml`

修改：

```toml
[package]
version = "0.3.0"
```

Runtime：

```toml
minicore-runtime = {
    git = "https://github.com/zqcli/minicore-runtime",
    rev = "87f3cf92b9b5980b0f468174a319cf53427d858e",
    version = "0.4.0"
}
```

新增：

```toml
getrandom = "=0.3.3"
```

原因：

```text
SessionId现在由Agent拥有
继续使用ses_<32 lower hex>格式
```

不新增 UUID、ULID 或数据库依赖。

保留：

```text
Rust 1.85
edition 2024
unsafe_code=forbid
```

## 7.2 删除不再使用的依赖

迁移完成后运行：

```bash
cargo machete
```

如果环境没有 `cargo machete`，通过源码搜索手工确认。

只删除确认无引用的依赖。

不要为了本次迁移更换：

```text
reqwest
tokio
serde
thiserror
tracing
```

的主要版本。

---

# 8. Agent-owned `SessionId`

## 8.1 新文件

```text
src/ids.rs
```

## 8.2 类型

保留现有 wire 格式：

```text
ses_<32 lower-case hexadecimal chars>
```

```rust
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId([u8; 16]);
```

公开：

```rust
impl SessionId {
    pub fn new() -> Result<Self, SessionIdError>;

    pub const fn as_bytes(&self) -> &[u8; 16];
}
```

实现：

```text
Display
Debug
FromStr
Serialize
Deserialize
```

## 8.3 错误

```rust
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SessionIdError {
    InvalidPrefix,
    InvalidLength,
    InvalidHex,
    ZeroPayload,
    EntropyUnavailable,
}
```

## 8.4 实现来源

可以从 Runtime v0.3 的 `SessionId` 实现中复制和收敛：

- 只保留 SessionId；
- 删除 SessionInstanceId、TurnId、ContextSourceId 等类型；
- 使用 `getrandom::fill`；
- 不建立通用 ID registry；
- 不增加宏，只有一个类型时普通实现更易读。

## 8.5 兼容范围

SessionId 字符串格式保持，因此：

```text
URL
RPC参数
目录名
用户书签
```

不需要因为 Runtime v0.4 改成新前缀。

这不表示旧 Session 存储格式兼容。

---

# 9. Session 持久设置

## 9.1 Profile 是创建模板

Profile 继续定义新 Session 默认值：

```text
model
reasoning
system_prompt
tools
max_tool_rounds
approval
```

创建完成后，将这些值复制进 SessionRecord。

现有 Session 不再依赖 Profile 当前内容。

## 9.2 `SessionRecord`

在 `src/store.rs`：

```rust
const SESSION_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionRecord {
    pub(crate) format_version: u32,

    pub(crate) session_id: SessionId,
    pub(crate) title: Option<String>,
    pub(crate) profile: String,
    pub(crate) workspace: PathBuf,

    pub(crate) model: String,
    pub(crate) reasoning: ReasoningPreference,
    pub(crate) system_prompt: String,
    pub(crate) tools: Vec<String>,
    pub(crate) max_tool_rounds: u16,
    pub(crate) approval: ApprovalMode,

    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
```

## 9.3 为什么保存完整设置

不是重新引入 Runtime `SessionSpec`。

区别：

```text
旧SessionSpec：
    Runtime不可变执行契约

新SessionRecord：
    Agent拥有、可修改的产品Session状态
```

保存完整设置可以保证：

```text
Profile修改后旧Session仍能打开
Profile删除后旧Session仍能打开
旧Session不会突然获得新的Tool
System prompt不会因配置文件变化而暗中替换
```

## 9.4 修改模型

`session.update` 允许改变：

```text
model
reasoning
```

不允许第一版动态修改：

```text
workspace
system_prompt
tools
max_tool_rounds
approval
profile label
```

这些能力后续可以通过完整 Session Settings update设计，不在本轮扩张。

---

# 10. Profile 和 Config 收敛

## 10.1 删除未实现的 Compaction 配置

删除：

```rust
ProfileCompaction
Profile.compaction
ConfigError::UnsupportedCompaction
```

删除对应配置、测试、README 示例。

原因：

```text
Runtime已不提供durable CompactionStrategy
Agent当前也没有持久化Compaction实现
保留一个永远fail-fast的字段属于死设计
```

未来 Compaction 在 Agent 层重新设计：

```text
请求级Context fitting
→ PromptProvider

长期Summary
→ Session History + JSONL
```

## 10.2 保留 Approval

保留：

```text
Auto
Ask
ReadOnly
```

本轮不新增审批功能。

现有 TUI 基础测试可继续使用：

```text
approval=auto
```

现有 Interaction 流程继续可用。

## 10.3 `KernelOverrides` 改名

删除：

```rust
KernelOverrides
command_capacity
runner_capacity
context_timeout_seconds
```

新增：

```rust
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopOverrides {
    pub event_capacity: Option<usize>,
    pub max_pending_steers: Option<usize>,

    pub prompt_timeout_seconds: Option<u64>,
    pub model_timeout_seconds: Option<u64>,
    pub policy_timeout_seconds: Option<u64>,
    pub tool_timeout_seconds: Option<u64>,

    pub model_retry_attempts: Option<u8>,
    pub model_retry_base_delay_millis: Option<u64>,
}
```

在 `AgentConfig`：

```rust
#[serde(default, rename = "loop")]
pub loop_options: LoopOverrides,
```

顶层现有：

```rust
event_capacity
```

继续表示 Agent 全局 Event channel 容量。

```text
loop.event_capacity
```

表示每个 Runtime AgentLoop 的 Event channel 容量。

## 10.4 `AgentConfig::loop_options`

新增：

```rust
pub(crate) fn loop_options(
    &self,
    max_tool_rounds: u16,
) -> Result<LoopOptions, ConfigError>;
```

实现：

1. `LoopOptions::default_checked()`；
2. 设置 Session 的 `max_tool_rounds`；
3. 应用 LoopOverrides；
4. 调用 `validate()`；
5. 返回。

本轮不暴露 Runtime `LoopLimits` 的配置覆盖。

使用 Runtime 默认安全上限。

## 10.5 Config 校验

继续检查：

```text
data_dir
Agent event capacity
default profile
Profile ID
Profile model存在
reasoning能力
Tool能力
Tool名称无重复
system prompt安全和大小
max_tool_rounds
Model配置
LoopOverrides
```

System prompt 建议限制：

```rust
const MAX_PROFILE_SYSTEM_PROMPT_BYTES: usize = 128 * 1024;
```

不要只检查 `BoundedText::MAX_BYTES`，因为还要与 AGENTS.md 合并成一个 Model system message。

---

# 11. Session 内部结构

## 11.1 `src/sessions.rs`

彻底删除旧：

```text
SessionRuntime
SessionHandle
TurnHandle
SessionPump
CompletionReady
MetadataWorker
SessionEventStream
SessionSpec
```

## 11.2 新结构

```rust
pub(crate) struct Sessions {
    loaded: HashMap<SessionId, Session>,
}
```

```rust
#[derive(Clone)]
pub(crate) struct Session {
    shared: Arc<SessionShared>,
}
```

```rust
struct SessionShared {
    inner: Mutex<SessionInner>,

    /// Serializes history append and session.json updates for this Session.
    io: tokio::sync::Mutex<()>,

    store: Store,
    events: AgentEventSink,
}
```

```rust
struct SessionInner {
    record: SessionRecord,

    workspace: Arc<Workspace>,
    history: Arc<[HistoryItem]>,

    config: ExecutionConfig,
    options: LoopOptions,

    active: Option<ActiveLoop>,
    blocked: Option<SessionBlockReason>,
}
```

## 11.3 为什么需要两个锁

`inner`：

```text
std::sync::Mutex
短临界区
不await
保存内存状态
```

`io`：

```text
tokio::sync::Mutex
只序列化同一个Session的：
    history append
    session.json update
```

不建立 Session actor。

不增加 command channel。

## 11.4 锁规则

`inner` 内禁止：

```text
await
Store I/O
Workspace I/O
AgentLoop::join
JoinHandle::await
模型/Tool调用
```

允许：

```text
clone Arc
读取/替换record
读取/替换config
读取active handle
更新blocked
合并History
```

`io` 可以跨文件 I/O await，但不得在持有 `io` 时长时间等待模型或 Tool。

---

# 12. `ActiveLoop`

## 12.1 类型

```rust
struct ActiveLoop {
    turn: TurnRef,
    handle: LoopHandle,

    completion:
        watch::Receiver<Option<TurnCompletion>>,

    task: Option<JoinHandle<()>>,
}
```

```rust
#[derive(Clone)]
enum TurnCompletion {
    Finished(Arc<TurnResult>),
    Internal,
}
```

## 12.2 Agent 产品 Turn

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct TurnRef {
    pub session_id: SessionId,
    pub loop_id: LoopId,
}
```

删除：

```text
SessionInstanceId
TurnId
```

一个产品 Turn 对应一个 Runtime AgentLoop。

## 12.3 TurnResult

```rust
#[derive(Clone)]
pub struct TurnResult {
    pub turn: TurnRef,
    pub report: Arc<LoopReport>,
    pub persistence: TurnPersistence,
}
```

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnPersistence {
    Persisted,
    Failed,
}
```

Runtime `LoopOutcome::Failed` 是正常 TurnResult，不映射为 AgentError。

只有以下情况返回 AgentError：

```text
TurnRef不存在
background task内部失败
completion channel异常
```

---

# 13. Session 状态

## 13.1 类型

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    WaitingForInput,
    Finishing,
    Blocked,
}
```

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionBlockReason {
    Persistence,
    Internal,
}
```

```rust
#[derive(Clone, Debug)]
pub struct SessionState {
    pub session_id: SessionId,
    pub status: SessionStatus,

    pub active_loop: Option<LoopState>,
    pub block_reason: Option<SessionBlockReason>,
}
```

## 13.2 状态映射

没有 active：

```text
blocked=None  → Idle
blocked=Some  → Blocked
```

有 active：

```text
Runtime Starting/RunningModel/RunningTools
→ Running

Runtime WaitingForInput
→ WaitingForInput

Runtime Finishing
→ Finishing

Runtime Finished但Agent持久化尚未完成
→ Finishing

Agent completion已经发布
→ Idle或Blocked
```

## 13.3 为什么 Agent 需要 Blocked

如果 Runtime 已执行 Tool 和模型，但：

```text
history.jsonl append失败
```

继续执行下一个用户 Turn 会导致：

```text
内存History与JSONL出现缺口
后续记录依赖一个没有保存的中间Turn
```

因此最小安全行为是：

```text
返回TurnResult(persistence=failed)
Session进入Blocked
后续turn.send拒绝
用户close/open后从已保存History继续
```

这不是 Runtime 的 Degraded 状态，也不重新引入 durable ledger。

只是 Agent 对自己拥有的 JSONL 失败作出最小处理。

不实现：

```text
pending persistence retry queue
自动重写整个History
数据库事务
恢复未保存Loop
```

---

# 14. Session 启动 Loop

## 14.1 `Session::start_loop`

建议签名：

```rust
pub(crate) async fn start_loop(
    &self,
    input: UserInput,
) -> Result<TurnRef, AgentError>;
```

Session 内已经保存：

```text
history
config
options
store
event sink
```

## 14.2 开始前 cleanup

首先调用：

```rust
self.cleanup_finished().await?;
```

行为：

```text
无active
→继续

active task未结束
→SessionBusy

active task已结束
→take task
→锁外await
→清理active
→如果blocked则返回SessionBlocked
```

使用：

```rust
JoinHandle::is_finished()
```

判断 Agent 级 background task，而不是只判断 Runtime LoopHandle。

原因：

```text
Runtime可能已经Finished
但Agent仍在写JSONL
```

## 14.3 Start snapshot

短锁内复制：

```text
Arc<[HistoryItem]>
ExecutionConfig
LoopOptions
```

检查：

```text
blocked=None
active=None
```

## 14.4 创建 Runtime Loop

```rust
let request = LoopRequest::new(
    Arc::clone(&history),
    input,
    config,
);

let mut agent_loop =
    AgentLoop::start(request, options)
        .map_err(map_loop_start_error)?;

let handle = agent_loop.handle();
let events = agent_loop.take_events()
    .map_err(|_| AgentError::Internal)?;

let turn = TurnRef {
    session_id,
    loop_id: handle.id(),
};
```

## 14.5 安装 ActiveLoop 与启动 task

避免 task先完成而Session尚未记录active：

1. 创建 completion watch；
2. 先把 `ActiveLoop { task: None }` 放入 SessionInner；
3. `tokio::spawn(run_active_loop(...))`；
4. 再把 JoinHandle写入对应active；
5. 如果步骤4发现active不匹配，立即 abort task并返回 Internal。

不需要启动 gate 或额外 oneshot。

---

# 15. ActiveLoop 后台任务

## 15.1 一个 task

每个 ActiveLoop 只创建一个 Agent-owned task：

```rust
async fn run_active_loop(
    session: Session,
    turn: TurnRef,
    agent_loop: AgentLoop,
    mut events: LoopEventStream,
    completion_tx:
        watch::Sender<Option<TurnCompletion>>,
)
```

它同时负责：

```text
转发Runtime Event
等待AgentLoop::join
持久化LoopReport
更新Session History
发送Agent Turn completion
```

不要再拆：

```text
EventPump task
Completion task
Metadata task
Persistence task
```

## 15.2 Join 与 Event select

```rust
let join = agent_loop.join();
tokio::pin!(join);

loop {
    tokio::select! {
        envelope = events.recv() => {
            match envelope {
                Some(envelope) => {
                    forward_loop_event(
                        turn.session_id,
                        envelope,
                        &session,
                    );
                }
                None => {
                    // Events结束不影响join。
                    break到仅等待join的路径。
                }
            }
        }

        result = &mut join => {
            break result;
        }
    }
}
```

Event receiver关闭：

```text
不取消Loop
继续等待join
```

## 15.3 Runtime `Finished` Event

Runtime `LoopEvent::Finished` 不直接映射成 Agent `TurnFinished`。

原因：

```text
Runtime Finished
→ 只表示执行结束
→ Agent JSONL可能尚未保存
```

处理：

```text
先合并envelope.dropped_before
忽略Runtime Finished payload
最终由Agent在持久化结束后发TurnFinished
```

## 15.4 Join success

得到：

```rust
Arc<LoopReport>
```

然后：

1. `sanitize_history(report.appended)`；
2. 构造 `StoredLoopRecord`；
3. 获取 Session `io` lock；
4. append `history.jsonl`；
5. 成功则更新 Session内存 History；
6. best-effort更新 `session.json.updated_at`；
7. 释放 `io`；
8. 生成 `TurnResult`；
9. completion watch写入；
10. best-effort发送 Agent `TurnFinished`；
11. best-effort发送最新 `SessionState`。

## 15.5 Append success

顺序：

```text
history.jsonl完整行写入成功
→ Session history合并
→ persistence=Persisted
→ blocked=None
```

Session history必须使用：

```text
sanitize后的、与JSONL完全一致的HistoryItem
```

不能使用 raw Runtime report作为下一轮base history。

## 15.6 Append failure

行为：

```text
不合并Session history
blocked=Persistence
persistence=Failed
仍返回包含raw LoopReport的TurnResult
不自动重试append
```

这样用户仍能看到本次执行结果，但下一次普通消息被拒绝，避免产生有缺口的日志链。

## 15.7 Join failure

如果：

```text
AgentLoop::join → LoopJoinError
```

行为：

```text
blocked=Internal
completion=Internal
AgentError::Internal
不构造伪造LoopReport
```

## 15.8 Completion 顺序

权威 completion应先于 best-effort事件：

```text
更新Session状态
→ completion_tx.send_replace(...)
→ AgentEvent::TurnFinished
```

因此：

```text
turn.wait response可能早于TurnFinished Event
```

RPC文档必须保留这一事实。

---

# 16. Session History 合并

## 16.1 内存格式

```rust
history: Arc<[HistoryItem]>
```

每个新 Loop 只 clone `Arc`，不 clone完整History。

成功持久化后：

```rust
let mut merged = Vec::with_capacity(
    old.len() + appended.len()
);

merged.extend(old.iter().cloned());
merged.extend(appended.iter().cloned());

inner.history = merged.into();
```

这是每个完成 Turn 一次 O(n) 合并。

第一版可接受。

不要实现：

```text
rope
persistent vector
chunked history tree
database cursor
copy-on-write custom collection
```

## 16.2 History 上限

Runtime 在下一个 `AgentLoop::start` 时检查：

```text
max_history_items
max_history_bytes
```

如果超限：

```text
turn.send → AgentError::HistoryTooLarge
```

本轮不自动压缩。

---

# 17. History 持久化清洗

## 17.1 原因

Runtime `HistoryItem::Assistant` 可以包含：

```text
ReasoningContent.encrypted
ReasoningContent.signature
```

这些数据用于当前 Loop 内 Provider continuation，但不应默认进入：

```text
Agent JSONL
RPC History
下一用户Turn
日志
```

继续保持当前 Agent 的安全边界：

> Provider opaque continuation 只在当前 live Loop 内存中使用。

## 17.2 新文件

```text
src/history.rs
```

## 17.3 `sanitize_history`

```rust
pub(crate) fn sanitize_history(
    items: &[HistoryItem],
) -> Result<Arc<[HistoryItem]>, AgentError>;
```

规则：

### User

原样 clone。

### ToolResult

原样 clone。

### Summary

原样 clone。

### Assistant

遍历 `AssistantPart`：

```text
Text
→ 保留

ToolCall
→ 保留，包括arguments

Reasoning
→ 保留text和summary
→ encrypted=None
→ signature=None
```

如果一个 Reasoning part只有 opaque字段：

```text
删除该part
```

如果清洗后整个 Assistant content为空：

```text
不把该AssistantHistory加入持久化History
```

这种情况的 raw response仍存在于当前 `TurnResult.report`，但不会进入下一轮上下文。

## 17.4 为什么 ToolCall arguments 必须保留

Runtime `DefaultPromptProvider` 需要：

```text
Assistant ToolCall
+
ToolResult
```

重新构造下一轮历史。

因此 JSONL 必须保留 ToolCall arguments。

RPC安全视图仍不展示 arguments。

## 17.5 加载时再次清洗

即使用户手工修改JSONL加入 opaque字段，Store加载后仍执行一次：

```text
sanitize_history
```

保证 Agent内存History和RPC视图不会保留这些字段。

## 17.6 不修改 Runtime Report

不要复制或重写：

```rust
LoopReport
```

清洗只作用于：

```text
Agent持久化版本
Agent Session History
```

---

# 18. Store 新格式

## 18.1 目录

```text
<data_dir>/
└── sessions/
    └── <session-id>/
        ├── session.json
        └── history.jsonl
```

删除：

```text
manifest.json
conversation.log
```

## 18.2 `StoredLoopRecord`

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredLoopRecord {
    loop_id: LoopId,

    outcome: StoredLoopOutcome,

    items: Vec<HistoryItem>,
    usage: Usage,

    requests: u32,
    tool_rounds: u16,
    final_config_revision: ConfigRevision,

    completed_at: String,
}
```

## 18.3 `StoredLoopOutcome`

仅用于记录，不重新构造 Runtime `LoopReport`：

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StoredLoopOutcome {
    Completed,

    Cancelled {
        reason: StoredCancelReason,
    },

    Failed {
        kind: String,
        model_error: Option<StoredModelError>,
    },
}
```

`kind` 使用固定安全字符串。

## 18.4 `StoredModelError`

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredModelError {
    kind: String,
    delivery: String,
    retryable: bool,
    retry_after_millis: Option<u64>,
}
```

不保存：

```text
Provider response body
error body
API key
Diagnostic text
Prompt
Tool arguments的额外日志副本
```

## 18.5 一行一个 Loop

```text
每个AgentLoop完成
→ 一个StoredLoopRecord
→ 一行JSON
```

不按 token、Event、ToolResult逐行写。

## 18.6 写入

```rust
pub(crate) async fn append_loop(
    &self,
    session_id: SessionId,
    record: &StoredLoopRecord,
) -> Result<(), StoreError>;
```

实现：

1. `serde_json::to_vec`；
2. 检查单行绝对上限；
3. push `\n`；
4. `OpenOptions::append(true)`；
5. `write_all`；
6. `flush`；
7. 返回。

不调用：

```text
fsync
sync_data
directory fsync
expected head
AppendReceipt
```

## 18.7 行大小

建议：

```rust
const MAX_LOOP_RECORD_BYTES: usize = 16 * 1024 * 1024;
```

Runtime默认 base history上限约8MiB；16MiB足够容纳一次Loop delta和JSON结构。

如果序列化超过：

```text
StoreError::RecordTooLarge
```

## 18.8 加载

使用：

```rust
tokio::io::BufReader
AsyncBufReadExt::read_until(b'\n', ...)
```

逐行读取，不一次性 `fs::read` 整个 history文件。

行为：

```text
完整合法行
→解析

最终不带newline的partial line
→忽略并截断到最后完整newline

中间非法JSON
→Corrupt

完整行超过MAX_LOOP_RECORD_BYTES
→Corrupt
```

## 18.9 最小 tail repair

打开可写 Session 时，如果发现最终 partial line：

```text
set_len(last_complete_offset)
```

这是允许的唯一 repair。

不实现：

```text
中间记录跳过
checksum
事务日志
备份
unknown outcome reconciliation
```

## 18.10 `Store` Public/Private API

```rust
impl Store {
    pub(crate) async fn open(root: PathBuf)
        -> Result<Self, StoreError>;

    pub(crate) async fn list_sessions(
        &self,
    ) -> Result<Vec<SessionRecord>, StoreError>;

    pub(crate) async fn create_session(
        &self,
        record: &SessionRecord,
    ) -> Result<(), StoreError>;

    pub(crate) async fn load_session(
        &self,
        session_id: SessionId,
    ) -> Result<StoredSession, StoreError>;

    pub(crate) async fn write_record(
        &self,
        record: &SessionRecord,
    ) -> Result<(), StoreError>;

    pub(crate) async fn append_loop(...);

    pub(crate) async fn delete_session(...);
}
```

```rust
pub(crate) struct StoredSession {
    pub(crate) record: SessionRecord,
    pub(crate) history: Arc<[HistoryItem]>,
}
```

删除：

```text
LocalSessionLog
SessionLog trait实现
AppendReceipt
LogBatch
InitializationState
UnknownOutcome
CleanupFailed
SessionLogErrorKind
```

---

# 19. Store 安全边界

保留最小必要的路径防护：

```text
data_dir非空
sessions root必须是directory
SessionId是canonical，不接受任意路径
显式Session directory拒绝symlink
session.json拒绝symlink
history.jsonl拒绝symlink
```

不需要恢复旧 Store 中复杂的：

```text
多层目录sync
unknown outcome reconciliation
多阶段cleanup error
测试注入的每一个文件系统失败点
```

创建失败时：

```text
best-effort remove新Session目录
返回原始Store错误
```

---

# 20. Session create/open/list

## 20.1 创建

`Agent::create_session`：

```text
resolve Profile
→应用model/reasoning override
→验证Model和Tools
→打开Workspace
→构造完整SessionRecord
→构造ExecutionConfig
→构造LoopOptions
→Store create
→构造Session
→插入Sessions map
→best-effort SessionOpened Event
```

所有可预知配置错误必须在 Store create前失败。

## 20.2 打开

```text
Store::load_session
→读取完整SessionRecord
→加载并清洗History
→打开Workspace
→根据record构造ExecutionConfig
→构造LoopOptions
→创建无active的Session
→插入map
```

不读取 Profile 当前内容。

`record.profile` 只是创建来源和显示标签。

如果 Profile 已删除：

```text
Session仍然可以打开
```

只要：

```text
record.model仍存在
record.tools仍是当前已知Tool
Workspace可打开
```

## 20.3 列表

`session.list`：

```text
只读取SessionRecord
不读取完整history.jsonl
```

Loaded Session使用内存中的最新 record。

未加载 Session使用磁盘 record。

单个旧格式或损坏 Session：

```text
跳过并写脱敏warn
```

显式 open仍严格失败。

## 20.4 关闭

```text
从Sessions map移除
→如果active：cancel
→await Agent-owned loop task
→best-effort SessionClosed
```

关闭不会删除JSONL。

## 20.5 删除

Loaded Session：

```text
SessionAlreadyLoaded
```

未加载：

```text
Store::delete_session
```

---

# 21. ExecutionConfig 装配

## 21.1 位置

建议在：

```text
src/agent.rs
```

保留一个普通 helper：

```rust
fn execution_config(
    &self,
    record: &SessionRecord,
    workspace: Arc<Workspace>,
) -> Result<ExecutionConfig, AgentError>;
```

不要创建：

```text
ExecutionConfigFactory
SessionBuilder
CapabilityRegistry
```

## 21.2 流程

```text
record.model
→ Models::get

record.tools
→ build_tools(workspace)

record.approval
→ Policy

record.system_prompt + Workspace
→ ProjectPromptProvider

record.reasoning
→ ExecutionConfig::new
```

## 21.3 ToolSet

每个 Session open/create时构造一次 ToolSet。

`SessionInner.config` 长期持有这份 `ExecutionConfig`。

每次用户 Turn：

```text
clone ExecutionConfig
```

不重复创建 Tool对象。

## 21.4 模型更新

更新 model/reasoning时：

```text
复用Session Workspace
复用record tools/system prompt/approval
构造一份新的完整ExecutionConfig
```

Runtime只接受完整配置原子替换。

---

# 22. ProjectPromptProvider

## 22.1 替换 `ProjectContext`

删除：

```text
ContextProvider
ContextRequest
ContextBundle
ContextBlock
ContextSlot
remaining_context_budget
旧Context token residue算法
```

新增：

```text
src/prompt.rs
```

## 22.2 类型

```rust
pub(crate) struct ProjectPromptProvider {
    workspace: Arc<Workspace>,
    system_prompt: BoundedText,
}
```

实现：

```rust
PromptProvider
```

## 22.3 请求流程

每个 Model Request：

1. 检查 cancellation/deadline；
2. 读取 Workspace根目录的 `AGENTS.md`；
3. NotFound → 无项目说明；
4. 其他 I/O失败 → `PromptError::InvalidHistory`；
5. UTF-8、CRLF和控制字符验证；
6. 超限时UTF-8安全截断并添加 `[truncated]`；
7. 合并 Session system prompt；
8. 创建 `DefaultPromptProvider`；
9. 调用默认投影；
10. 返回 `PreparedPrompt`。

## 22.4 大小边界

```rust
const MAX_AGENTS_BYTES: usize = 64 * 1024;
const MAX_PROFILE_SYSTEM_PROMPT_BYTES: usize = 128 * 1024;
```

组合格式：

```text
<session system prompt>

[minicore-project-instructions source=AGENTS.md]
<AGENTS.md>
```

总大小必须小于 Runtime单条ModelMessage绝对上限。

如果 AGENTS内容太大：

```text
截断AGENTS部分
不截断Session system prompt
```

## 22.5 不实现 token fitting

本阶段不重新实现旧的：

```text
remaining context budget
byte residue token proof
binary search token fitting
```

如果完整 History 对目标模型过大：

```text
OpenAI Model返回ContextOverflow
LoopReport保留结构化ModelError
```

下一阶段再实现 Agent compaction。

## 22.6 动态文件

AGENTS.md 每个 Model Request重新读取。

因此在当前 Loop运行中修改 AGENTS.md：

```text
下一个Model Request可见
```

不增加文件缓存或 watcher。

---

# 23. OpenAI Adapter 迁移

## 23.1 Context 类型变化

旧：

```text
session_id
instance_id
turn_id
round
```

新：

```text
loop_id
request_index
```

## 23.2 类型修改

在：

```text
src/models/openai.rs
```

删除 imports：

```text
SessionId
SessionInstanceId
TurnId
```

使用：

```text
LoopId
```

修改：

```rust
type ContinuationStore =
    Arc<Mutex<HashMap<LoopId, LoopContinuation>>>;
```

```rust
struct RequestTraceContext {
    loop_id: LoopId,
    request_index: u32,
}
```

```rust
struct ProviderRequestReplay {
    request_index: u32,
    tool_call_ids: Vec<ToolCallId>,
    output_items: Arc<[Value]>,
    output_item_bytes: usize,
}
```

命名：

```text
TurnContinuation → LoopContinuation
ProviderRoundReplay → ProviderRequestReplay
MAX_CONTINUATION_BYTES_PER_TURN
→ MAX_CONTINUATION_BYTES_PER_LOOP
```

## 23.3 Continuation 语义

```text
request_index=0
→清理同LoopId旧状态

request_index连续
→允许当前模型实例重放

request_index跳号
→清理旧continuation

reasoning=disabled
→不使用continuation
```

## 23.4 模型热切换

如果 Loop 从模型A切到模型B：

```text
B使用自己的Model实例和continuation store
不会读取A的数据
```

如果随后从B切回A：

```text
A看到request_index不连续
→清理A旧continuation
→干净请求
```

不要建立跨模型 continuation transfer。

## 23.5 清理

Loop结束时 Runtime会取消根 cancellation token。

Continuation store在后续访问时移除 cancelled entry。

保留现有容量和字节边界。

不要增加全局 cleanup task。

## 23.6 Tracing

替换字段：

```text
session_id / instance_id / turn_id / round
```

为：

```text
loop_id / request_index
```

仍不记录：

```text
prompt
tool arguments
raw response
API key
encrypted reasoning
```

## 23.7 Live smoke

现有：

```text
text smoke
reasoning + read Tool smoke
```

迁移为：

```text
Agent Session
→ AgentLoop
→ LoopReport
```

验证：

```text
同一个Loop内Tool continuation
最终History持久化不含encrypted/signature
```

---

# 24. Tool 和 Policy

## 24.1 Tool 实现

以下文件主体保留：

```text
src/tools/read.rs
src/tools/write.rs
src/tools/edit.rs
src/tools/apply_patch.rs
src/tools/bash.rs
src/workspace.rs
```

Runtime v0.4 Tool接口基本延续：

```text
Tool
ToolContext
ToolInvocation
ToolOutput
ToolExecutionOutcome
ToolSpec
```

需要做的主要工作：

```text
更新import
更新少量错误映射
删除Session相关测试fixture
```

## 24.2 ToolContext

使用 Runtime v0.4：

```text
cancellation
deadline
progress
```

Tool不得访问：

```text
Session
History
Store
Agent内部map
```

## 24.3 Policy

`Policy` 保持现有：

```text
Auto
Ask
ReadOnly
```

本轮不实现新的审批摘要。

只适配 Runtime v0.4 `ToolPolicyRequest`。

## 24.4 Bash

继续保留：

```text
配置Model credential env removal
cancel
timeout
bounded output
非PTY
非sandbox
```

不在本次迁移实现实时 stdout。

---

# 25. Session 模型和 reasoning 热更新

## 25.1 Public request

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSession {
    pub session_id: SessionId,

    #[serde(default)]
    pub model: Option<String>,

    #[serde(default)]
    pub reasoning: Option<ReasoningPreference>,
}
```

至少一个字段必须存在。

## 25.2 Result

```rust
#[derive(Clone, Debug, Serialize)]
pub struct SessionUpdateResult {
    pub session: SessionInfo,
    pub active_revision: Option<ConfigRevision>,
}
```

## 25.3 更新顺序

1. 读取当前 SessionRecord和Workspace；
2. 生成候选record；
3. 校验Model/reasoning/Tool能力；
4. 构造新ExecutionConfig；
5. 获取Session `io` lock；
6. 原子写session.json；
7. 更新内存record和config；
8. clone当前active LoopHandle；
9. 释放inner lock；
10. 如果active尚存在，调用：
   ```rust
   handle.update(new_config)
   ```
11. 返回 revision或None。

## 25.4 `NotActive`

Loop可能在写metadata期间已经final seal。

如果：

```rust
LoopHandle::update → UpdateError::NotActive
```

Session持久更新仍然成功：

```text
active_revision=None
下一次用户Turn使用新设置
```

不回滚 SessionRecord。

## 25.5 `InvalidConfig`

Agent在步骤3/4已完整验证。

如果 Runtime仍返回：

```text
InvalidConfig
```

映射：

```text
AgentError::Internal
```

不静默降级。

## 25.6 生效确认

RPC/TUI不能仅凭 `session.update` 返回就假设当前 request已切换。

实际生效通过：

```text
AgentEvent::RequestStarted.config_revision
AgentEvent::RequestStarted.model
AgentEvent::RequestStarted.reasoning
```

确认。

---

# 26. Persistence failure 语义

## 26.1 为什么不直接返回 StoreError

Runtime已产生一个完整 LoopReport。

如果 Agent只返回：

```text
StoreError
```

用户可能失去：

```text
最终模型文本
Tool结果
Usage
错误原因
```

因此：

```text
turn.wait始终优先返回TurnResult
```

只要 Runtime Report存在。

## 26.2 `TurnPersistence::Failed`

表示：

```text
Runtime执行完成
但history.jsonl未确认写入
Session内存History未合并
Session已Blocked
```

## 26.3 后续行为

Blocked Session：

```text
turn.send → SessionBlocked
session.update → SessionBlocked
steer/cancel → 如果active尚在则按active处理
history → 返回最后成功持久化History
close → 允许
open → 从磁盘重新加载，清除Blocked
delete → close后允许
```

不提供：

```text
session.retry_persistence
session.accept_data_loss
```

第一版通过 close/open恢复即可。

---

# 27. History API

## 27.1 替换 Transcript

删除：

```text
GetTranscript
TranscriptPage
ConversationSeq
session.transcript
```

新增：

```rust
pub struct GetHistory {
    pub session_id: SessionId,
    pub offset: usize,
    pub limit: usize,
}
```

```rust
pub struct HistoryPage {
    pub items: Vec<IndexedHistoryItem>,
    pub next_offset: Option<usize>,
    pub total: usize,
}
```

```rust
pub struct IndexedHistoryItem {
    pub index: usize,
    pub item: HistoryItemView,
}
```

## 27.2 Pagination

```text
offset = 第一个要读取的item index
limit = 1..=100
```

结果：

```text
next_offset=None
→ complete

next_offset=Some(n)
→ 下一页从n开始
```

不增加 cursor token。

History在 Session loaded生命周期内只追加，因此 index稳定。

## 27.3 安全 View

RPC和Public Agent Event不直接序列化 raw `HistoryItem`。

```rust
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum HistoryItemView {
    User(UserHistoryView),
    Assistant(AssistantHistoryView),
    ToolResult(ToolResultHistoryView),
    Summary(SummaryHistoryView),
}
```

### User

```text
loop_id
kind
text
```

### Assistant

```text
loop_id
request_index
model
reasoning_level
text
reasoning
tool_calls
usage
finish_reason
```

其中：

```text
text part按顺序拼接
reasoning只拼接text/summary
不展示encrypted/signature
```

### ToolCall

展示：

```text
tool_call_id
name
call_index
```

不展示 arguments。

### ToolResult

展示：

```text
loop_id
request_index
tool_call_id
tool_name
outcome
content
```

### Summary

展示 content。

---

# 28. Agent Event 重构

## 28.1 EventMeta

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EventMeta {
    pub session_id: SessionId,
    pub loop_id: Option<LoopId>,
    pub dropped_before: u64,
}
```

删除：

```text
instance_id
turn_id
```

## 28.2 AgentEvent

```rust
pub enum AgentEvent {
    SessionOpened {
        session: SessionInfo,
        meta: EventMeta,
    },

    SessionClosed {
        session_id: SessionId,
        meta: EventMeta,
    },

    SessionState {
        state: SessionState,
        meta: EventMeta,
    },

    TurnStarted {
        turn: TurnRef,
        meta: EventMeta,
    },

    RequestStarted {
        turn: TurnRef,
        request_index: u32,
        config_revision: ConfigRevision,
        model: String,
        reasoning: ReasoningPreference,
        meta: EventMeta,
    },

    OutputDelta {
        turn: TurnRef,
        request_index: u32,
        channel: OutputChannel,
        delta: String,
        meta: EventMeta,
    },

    ToolStarted {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        tool_name: String,
        meta: EventMeta,
    },

    ToolProgress {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        progress: ToolProgressView,
        meta: EventMeta,
    },

    ToolFinished {
        turn: TurnRef,
        request_index: u32,
        tool_call_id: ToolCallId,
        result: ToolResultView,
        meta: EventMeta,
    },

    InteractionRequested {
        turn: TurnRef,
        interaction: PendingInteraction,
        meta: EventMeta,
    },

    InteractionResolved {
        turn: TurnRef,
        interaction_id: InteractionId,
        meta: EventMeta,
    },

    TurnFinished {
        turn: TurnRef,
        outcome: TurnOutcomeSummaryView,
        persistence: TurnPersistence,
        meta: EventMeta,
    },
}
```

不增加单独：

```text
PersistenceFailed
LoopFinished
ConfigUpdated
SteerQueued
```

已有字段足够。

## 28.3 Runtime Event 映射

| Runtime LoopEvent | AgentEvent |
|---|---|
| Started | TurnStarted |
| StateChanged | SessionState |
| RequestStarted | RequestStarted |
| OutputDelta | OutputDelta |
| ToolStarted | ToolStarted |
| ToolProgress | ToolProgress |
| ToolFinished | ToolFinished |
| InteractionRequested | InteractionRequested |
| InteractionResolved | InteractionResolved |
| Finished | 不直接转发 |

## 28.4 Drop count

每个 Runtime envelope先执行：

```rust
agent_sink.record_core_drops(
    envelope.dropped_before
);
```

即使 Runtime `Finished` 被忽略，也要记录其 `dropped_before`。

Agent自己的bounded channel满时继续累计。

下一条成功 AgentEvent包含：

```text
Runtime内部progress drops
Runtime LoopEvent queue drops
AgentEvent queue drops
```

不实现来源拆分。

## 28.5 SessionState

Runtime StateChanged到达时：

```text
根据Session + LoopState构造Agent SessionState
```

即使事件丢失，`session.state`查询仍通过当前 `LoopHandle::state()`获得最新状态。

---

# 29. Public Agent API

## 29.1 `Agent`

保留一个具体顶层类型。

不新增：

```text
AgentService
AgentClient
AgentFactory
AgentManager
```

## 29.2 目标 API

```rust
impl Agent {
    pub async fn open(
        config: AgentConfig,
    ) -> Result<Self, AgentError>;

    pub fn config(&self) -> &AgentConfig;

    pub const fn ping(&self) -> PingResponse;

    pub fn list_profiles(&self) -> Vec<ProfileInfo>;

    pub fn list_models(&self) -> Vec<ModelInfo>;

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

    pub async fn update_session(
        &mut self,
        request: UpdateSession,
    ) -> Result<SessionUpdateResult, AgentError>;

    pub async fn send(
        &mut self,
        request: SendMessage,
    ) -> Result<TurnRef, AgentError>;

    pub fn steer(
        &self,
        request: SteerMessage,
    ) -> Result<(), AgentError>;

    pub fn cancel(
        &self,
        turn: TurnRef,
    ) -> Result<bool, AgentError>;

    pub async fn wait_turn(
        &self,
        turn: TurnRef,
    ) -> Result<Arc<TurnResult>, AgentError>;

    pub fn answer(
        &self,
        request: AnswerInteraction,
    ) -> Result<(), AgentError>;

    pub fn history(
        &self,
        request: GetHistory,
    ) -> Result<HistoryPage, AgentError>;

    pub fn take_events(
        &mut self,
    ) -> Result<AgentEventStream, AgentError>;

    pub async fn shutdown(
        self,
    ) -> Result<(), AgentError>;
}
```

## 29.3 新请求类型

```rust
pub struct SteerMessage {
    pub turn: TurnRef,
    pub text: String,
}
```

```rust
pub struct AnswerInteraction {
    pub turn: TurnRef,
    pub interaction_id: InteractionId,
    pub answer: InteractionAnswer,
}
```

## 29.4 暂不增加 `AgentHandle`

未来 Subagent Tool需要可克隆的窄 `AgentHandle`。

但当前 TUI迁移不需要。

本轮只通过内部 cloneable `Session` 支持 background task。

不要提前公开 AgentHandle。

---

# 30. Agent 顶层字段

目标：

```rust
pub struct Agent {
    config: AgentConfig,

    store: Store,
    profiles: Profiles,
    models: Models,

    command_environment: CommandEnvironment,

    sessions: Sessions,

    events_tx: mpsc::Sender<AgentEvent>,
    events_rx: Option<mpsc::Receiver<AgentEvent>>,
}
```

删除：

```text
task_runtime
KernelConfig
```

Runtime `AgentLoop::start` 使用当前 Tokio context。

Agent `open` 和 loop start仍在 Tokio Runtime中执行。

---

# 31. Session model update 实现位置

Agent负责：

```text
Model lookup
record candidate
ExecutionConfig构造
```

Session负责：

```text
串行Store write
内存config替换
active LoopHandle::update
```

建议：

```rust
impl Session {
    pub(crate) async fn update(
        &self,
        record: SessionRecord,
        config: ExecutionConfig,
    ) -> Result<Option<ConfigRevision>, AgentError>;
}
```

Agent不直接操作 `SessionInner`。

---

# 32. Error 模型

## 32.1 `AgentError`

目标：

```rust
#[derive(Debug, Error)]
pub enum AgentError {
    Config(ConfigError),

    SessionNotFound,
    SessionNotLoaded,
    SessionAlreadyLoaded,
    SessionBusy,
    SessionBlocked,

    TurnNotFound,

    InteractionNotFound,
    InvalidInteraction,

    InvalidInput,
    HistoryTooLarge,

    ProfileNotFound,
    ModelNotFound,
    InvalidSessionSettings,

    Workspace,
    Store,

    Runtime(RuntimeErrorView),
    Internal,

    EventStreamTaken,
    InvalidArguments,
    RpcSerialization,
    Io(io::Error),
}
```

删除：

```text
SessionSpecMismatch
SessionClosed
SessionDegraded
ModelNotImplemented
CoreErrorView
```

`CoreErrorView` 改名：

```rust
RuntimeErrorView
```

## 32.2 Runtime start/control错误

安全映射：

```text
LoopStartError::HistoryTooLarge
→ AgentError::HistoryTooLarge

LoopStartError::InvalidInput
→ InvalidInput

LoopStartError::InvalidConfig
→ InvalidSessionSettings

LoopStartError::NoTokioRuntime
→ Internal

SteerError::QueueFull
→ AgentError::SteerQueueFull

SteerError::WaitingForInput
→ InvalidState

SteerError::NotActive
→ TurnNotFound

UpdateError::NotActive
→ 当前Session update成功，active_revision=None

AnswerError::InteractionNotFound
→ InteractionNotFound
```

## 32.3 Loop failure不是AgentError

```text
Model failure
Prompt failure
OutputLimit
Refused
ContentFiltered
MaxToolRounds
Cancelled
```

全部通过：

```text
TurnResult.report.outcome
```

返回。

---

# 33. StoreError

收敛为：

```rust
#[derive(Debug, Error)]
pub(crate) enum StoreError {
    InvalidRoot,
    InvalidRecord,
    UnsupportedFormat,

    SessionNotFound,
    SessionAlreadyExists,

    Corrupt,
    RecordTooLarge,
    Unavailable,
}
```

删除：

```text
UnknownOutcome
CleanupFailed
Internal
Log(SessionLogError)
```

如果 serde或I/O内部失败无法进一步分类：

```text
Corrupt或Unavailable
```

不增加泛化 source tree。

---

# 34. RPC 方法

## 34.1 保留

```text
agent.ping
agent.shutdown

profile.list
model.list

session.list
session.create
session.open
session.close
session.delete
session.state

turn.send
turn.cancel
turn.wait

interaction.answer
```

## 34.2 新增

```text
session.update
session.history
turn.steer
```

## 34.3 删除

```text
session.transcript
```

不保留 alias。

---

# 35. RPC 参数

## 35.1 `session.update`

```json
{
  "session_id": "ses_...",
  "model": "deep",
  "reasoning": "high"
}
```

model/reasoning至少一个存在。

## 35.2 `turn.send`

```json
{
  "session_id": "ses_...",
  "text": "Fix the parser"
}
```

## 35.3 `turn.steer`

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "text": "Do not modify config files"
}
```

## 35.4 `turn.cancel` / `turn.wait`

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_..."
}
```

## 35.5 `interaction.answer`

```json
{
  "session_id": "ses_...",
  "loop_id": "lup_...",
  "interaction_id": "int_...",
  "answer": {
    "type": "approval",
    "decision": "allow_once"
  }
}
```

## 35.6 `session.history`

```json
{
  "session_id": "ses_...",
  "offset": 0,
  "limit": 100
}
```

---

# 36. RPC Result

## 36.1 `turn.send`

```json
{
  "turn": {
    "session_id": "ses_...",
    "loop_id": "lup_..."
  }
}
```

## 36.2 `session.update`

```json
{
  "session": {
    "...": "..."
  },
  "active_revision": 2
}
```

Idle或Active已经sealed：

```json
{
  "active_revision": null
}
```

## 36.3 `turn.wait`

```json
{
  "turn": {
    "session_id": "ses_...",
    "loop_id": "lup_..."
  },
  "outcome": {
    "type": "completed"
  },
  "usage": {
    "...": "..."
  },
  "requests": 2,
  "tool_rounds": 1,
  "final_config_revision": 1,
  "persistence": "persisted"
}
```

Failed Model示例：

```json
{
  "outcome": {
    "type": "failed",
    "kind": "model",
    "model_error": {
      "kind": "rate_limited",
      "delivery": "not_started",
      "retryable": true,
      "retry_after_millis": 1000
    }
  }
}
```

不返回：

```text
raw ModelError diagnostic正文
raw Provider body
LoopReport::appended
```

History通过 `session.history`读取。

---

# 37. RPC Error codes

继续保留通用 JSON-RPC codes。

Agent domain建议：

```text
-32001 session_not_found
-32002 session_not_loaded
-32003 session_busy
-32004 session_blocked
-32005 invalid_state
-32006 interaction_not_found
-32007 turn_not_found
-32008 profile_not_found
-32009 model_not_found
-32010 workspace_error
-32011 store_error
-32012 provider_error（仅Agent open/build provider）
-32013 runtime_error
-32014 invalid_session_settings
-32015 history_too_large
-32016 steer_queue_full
```

不要为 Runtime每个 LoopFailureKind增加 RPC error code。

LoopFailure通过成功的 `turn.wait` result返回。

---

# 38. RPC server 并发

## 38.1 Deferred wait保留

`turn.wait` 继续使用 `JoinSet` deferred response。

但是等待对象改为 Agent级：

```text
TurnWaiter / completion watch
```

不直接 clone Runtime `LoopHandle`等待。

## 38.2 Reader 不能被 wait阻塞

运行中的：

```text
turn.wait
```

不能阻止处理：

```text
turn.steer
turn.cancel
session.update
interaction.answer
```

## 38.3 Shutdown

顺序：

```text
Agent::shutdown
→所有Session cancel active Loop
→await所有Agent-owned loop task
→drop Agent event sender
→event pump结束
→等待RPC waiters
→写shutdown response
→关闭writer
```

---

# 39. `src/event.rs` 视图

保留并重写：

```text
ToolProgressView
ToolResultView
InteractionKindView
PendingInteractionView
DiagnosticView
```

删除：

```text
SessionHealthView
TurnTerminalView
ConversationSeq
SessionInstanceId
TurnId
旧TurnOutcomeView
```

新增：

```text
LoopOutcomeView
ModelErrorView
SessionStatusView
SessionBlockReasonView
LoopStateView
TurnResultView
HistoryItemView
```

不把 Runtime `LoopReport`直接 Serialize。

---

# 40. `src/lib.rs`

最终公开：

```rust
pub use agent::{
    Agent,
    AnswerInteraction,
    CreateSession,
    GetHistory,
    HistoryPage,
    PingResponse,
    SendMessage,
    SessionInfo,
    SessionState,
    SessionStatus,
    SessionUpdateResult,
    SteerMessage,
    TurnPersistence,
    TurnRef,
    TurnResult,
    UpdateSession,
};

pub use config::{
    AgentConfig,
    ApprovalMode,
    ConfigError,
    LoopOverrides,
    Profile,
};

pub use error::{
    AgentError,
    RuntimeErrorView,
};

pub use event::{
    AgentEvent,
    AgentEventStream,
    EventMeta,
};

pub use ids::{
    SessionId,
    SessionIdError,
};

pub use models::{
    ModelConfig,
    ModelInfo,
};

pub use profiles::ProfileInfo;
pub use rpc::run_stdio;
pub use workspace::{Workspace, WorkspaceError};
```

不 re-export Runtime 的完整模块。

调用方需要 Runtime Model/History type时可以直接依赖 Runtime；TUI RPC用户不需要。

---

# 41. 文件级改造清单

## `Cargo.toml`

```text
Agent 0.3.0
Runtime 0.4 revision
getrandom
删除无用dependency
```

## `src/lib.rs`

更新模块和Public export。

## `src/ids.rs`

新增 Agent-owned SessionId。

## `src/config.rs`

```text
KernelOverrides → LoopOverrides
删除ProfileCompaction
AgentConfig::loop_options
Runtime新imports
```

## `src/profiles.rs`

```text
删除ProfileCompaction
Profile保持创建模板
```

## `src/models.rs`

```text
适配Runtime v0.4 Model接口
ModelInfo保持
```

## `src/models/openai.rs`

```text
Session/Turn identity → LoopId/request_index
continuation命名和key更新
日志字段更新
```

## `src/prompt.rs`

新增 ProjectPromptProvider。

## `src/context.rs`

删除。

## `src/history.rs`

新增：

```text
History sanitize
History pagination
RPC/Public safe views的内部转换
Stored outcome转换helper
```

视代码组织也可以把纯RPC View留在protocol.rs。

不要在两个文件重复转换。

## `src/store.rs`

完整重写为：

```text
SessionRecord
StoredLoopRecord
session.json
history.jsonl
tail repair
list/create/load/write/append/delete
```

删除旧 SessionLog全部代码和测试注入框架。

## `src/sessions.rs`

完整重写为：

```text
Sessions
Session
SessionShared
SessionInner
ActiveLoop
TurnCompletion
start/update/wait/cancel/steer/answer/shutdown
run_active_loop
```

## `src/agent.rs`

重写 Runtime装配与Facade方法。

删除所有旧 SessionRuntime方法和错误映射。

## `src/event.rs`

重写 Runtime LoopEvent映射和Agent views。

## `src/error.rs`

删除 SessionLog/Degraded错误，增加新Agent/Store错误。

## `src/policy.rs`

只更新import和编译适配。

## `src/tools/*`

只更新Runtime import/API适配。

## `src/rpc/protocol.rs`

```text
新SessionId
新TurnRef
session.history
session.update
turn.steer
new outcome/history views
```

## `src/rpc/server.rs`

```text
新dispatch
Agent级waiter
shutdown顺序
```

## `README.md`

完整更新v0.3架构和安全边界。

## `docs/rpc.md`

更新新协议。

## `example.agent.toml`

更新 `[loop]`，删除compaction和旧kernel字段。

---

# 42. 应删除的旧代码

明确删除，不迁移：

```text
SessionPump
CompletionReady
MetadataWorker

SessionRuntime create/load
SessionHandle
TurnHandle
SessionSpec
SessionManifest
KernelConfig

LocalSessionLog
SessionLog trait implementation
AppendReceipt
LogBatch
expected head
ConversationSeq
conversation.log
manifest.json
durability unknown
recovery/repair
Store fault injection tests only servingold durability

ContextBundle
ContextSlot
remaining_context_budget
old token-residue context tests

SessionHealth
Degraded
TurnTerminal
CancelledByRestart
SessionInstanceId
```

不要留下 dead-code attributes掩盖迁移残留。

---

# 43. 不应删除的现有能力

必须保留并迁移：

```text
Workspace path safety
read/write/edit/apply_patch/bash
Bash credential env removal
OpenAI Responses SSE
delivery-aware ModelError
reasoning Tool continuation
ToolPolicy
Interaction
Agent event redaction
stdout JSON-RPC / stderr tracing隔离
model/profile列表
多Session
三平台CI
Rust 1.85
```

---

# 44. 实施顺序

## Phase 0：冻结基线

记录：

```text
Agent起始HEAD
Runtime revision
当前测试结果
当前代码行数
```

建议保存 tag：

```text
v0.2.0-runtime-v0.3
```

## Phase 1：Runtime依赖与基础 DTO

实现：

```text
Cargo update
SessionId
SessionRecord新格式
TurnRef
SessionState
TurnResult
LoopOverrides
ProfileCompaction删除
```

这一阶段可以与 Phase 2放在一个临时工作分支中完成。

不要为了让每个中间文件编译而引入 Runtime双版本依赖。

## Phase 2：Store 重写

实现并独立测试：

```text
session.json
history.jsonl
tail repair
append/load/list/delete
old format拒绝
```

## Phase 3：Prompt和Provider

实现：

```text
ProjectPromptProvider
OpenAI LoopId/request_index迁移
Model/Tool/Policy编译适配
```

## Phase 4：Session核心

实现：

```text
Session
ActiveLoop
run_active_loop
completion
persistence
history merge
blocked state
```

这是本轮核心阶段。

## Phase 5：Agent Facade

迁移：

```text
create/open/close/delete/list
send/steer/update/cancel/wait/answer/history
shutdown
```

## Phase 6：Event

完成：

```text
Runtime Event mapping
drop count合并
TurnFinished after persistence
SessionState
```

## Phase 7：RPC

完成：

```text
new wire types
session.history
session.update
turn.steer
deferred wait
error mapping
```

## Phase 8：测试和文档

完成全部回归、Live smoke、README、RPC文档和CI。

---

# 45. 推荐提交

由于 Runtime API完全breaking，不要为追求“小提交全部可编译”引入临时兼容架构。

建议：

## Commit 1

```text
refactor(agent): own sessions and history over runtime v0.4 loops
```

包含：

```text
Runtime dependency
SessionId
Store
Session
Agent facade
PromptProvider基础迁移
```

这是允许的主要breaking spine提交。

## Commit 2

```text
feat(agent): steer and update active loop configuration
```

## Commit 3

```text
refactor(openai): key continuations by loop requests
```

## Commit 4

```text
refactor(events): forward loop events through agent sessions
```

## Commit 5

```text
refactor(rpc): expose history steer and session updates
```

## Commit 6

```text
test(agent): cover multi-turn persistence and active loop control
```

## Commit 7

```text
docs: document the runtime v0.4 agent boundary
```

每个完成后的提交应编译；开发过程中的临时不编译状态不要推送为正式 commit。

---

# 46. 核心测试：Store

## AG3-001 Create

```text
创建session.json和空history.jsonl
```

## AG3-002 Record round-trip

完整 SessionRecord往返。

## AG3-003 Append/load

两个StoredLoopRecord：

```text
load后按顺序flatten History
```

## AG3-004 Final partial line

```text
最后半行被忽略并截断
之后append新行仍合法
```

## AG3-005 Middle corrupt

中间非法JSON：

```text
Corrupt
```

## AG3-006 Oversized line

拒绝。

## AG3-007 Old format

旧session.json：

```text
list跳过
explicit open UnsupportedFormat/Store
不修改旧文件
```

## AG3-008 Symlink

显式 Session路径和两个文件拒绝symlink。

## AG3-009 List tolerance

无关文件和坏Session不破坏健康列表。

## AG3-010 Delete

只删除目标Session目录。

---

# 47. 核心测试：Session

## AG3-011 Create/open without Runtime Session

创建和open不会启动AgentLoop。

## AG3-012 Single active

第二次send返回Busy。

## AG3-013 Basic completion

```text
Text final
→JSONL append
→history merge
→wait persisted
→Idle
```

## AG3-014 Tool loop

```text
Model ToolCall
→ToolResult
→Model final
→单个StoredLoopRecord
```

## AG3-015 Multi-turn

连续三个send：

```text
每次创建新LoopId
History依次增长
第三次模型看到前两次历史
```

## AG3-016 Cancel

```text
cancel
→TurnResult Cancelled
→记录已完成delta
→Session可继续
```

## AG3-017 Close active

close取消、await、无orphan task。

## AG3-018 Event stream absent

AgentEventStream不消费时，Loop仍完成并保存。

## AG3-019 Wait authority

TurnFinished Event丢失：

```text
turn.wait仍返回
```

## AG3-020 cleanup

完成Turn未显式wait：

```text
下一次send先cleanup
→可以启动下一Loop
```

---

# 48. 核心测试：Persistence failure

## AG3-021 Append failure

```text
Runtime Completed
Store append失败
→TurnResult.persistence=Failed
→History不合并
→Session Blocked
```

## AG3-022 Blocked send

返回 SessionBlocked。

## AG3-023 Blocked history

只返回最后成功持久化History。

## AG3-024 Close/open recovery

```text
close
open
→从磁盘历史恢复
→Blocked清除
```

## AG3-025 Metadata failure

history append成功但updated_at写失败：

```text
Turn仍Persisted
History已合并
仅warn
```

---

# 49. 核心测试：Steer

## AG3-026 Steer during Model

```text
steer
→Runtime下一个request应用
→LoopReport含UserMessageKind::Steering
→JSONL含Steering
```

## AG3-027 Multiple steers

按FIFO顺序持久化。

## AG3-028 Queue full

映射 `SteerQueueFull`。

## AG3-029 WaitingForInput

映射 InvalidState，不改 Interaction。

## AG3-030 Stale TurnRef

旧 LoopId拒绝。

## AG3-031 Applied before failure

已经应用的steer即使后续模型失败，也跟随LoopReport保存。

## AG3-032 Not applied

Loop在boundary前结束：

```text
steer不进入History
```

---

# 50. 核心测试：模型热更新

## AG3-033 Idle update

更新record/config，下一Loop使用新模型。

## AG3-034 Running update

当前request保持模型A：

```text
下一request使用模型B
```

## AG3-035 Tool batch snapshot

A产生的ToolCall继续使用旧ToolSet/Policy；

下一request使用新Model config。

本轮只更新Model/reasoning，因此ToolSet实例可相同。

## AG3-036 Update revision

返回 Runtime revision。

## AG3-037 Final race

Runtime已sealed：

```text
Session设置更新成功
active_revision=None
下一Loop使用新设置
```

## AG3-038 Invalid model/reasoning

不写session.json，不改内存，不调用Runtime update。

## AG3-039 Update persistence failure

不改内存config，不调用active update。

## AG3-040 RequestStarted

Event显示实际revision/model/reasoning。

---

# 51. 核心测试：Prompt

## AG3-041 No AGENTS.md

只有Session system prompt和History。

## AG3-042 AGENTS.md

项目说明进入system prompt。

## AG3-043 Dynamic AGENTS

同一Loop两个request之间修改文件：

```text
第二request读取新内容
```

## AG3-044 Truncate

超过64KiB：

```text
UTF-8安全
有[truncated]
```

## AG3-045 Invalid UTF-8/control

Prompt failure，Loop正常返回Failed。

## AG3-046 Cancellation

读取期间cancel及时返回。

## AG3-047 No compaction

History过大不自动Summary。

---

# 52. 核心测试：OpenAI

## AG3-048 Context migration

Trace只有loop_id/request_index。

## AG3-049 Tool continuation

同一Loop request 0→1精确重放。

## AG3-050 Model switch

A→B：

```text
B没有A continuation
```

## AG3-051 Switch back

A→B→A：

```text
A发现request_index不连续
→清理旧continuation
```

## AG3-052 Loop cancellation

Continuation最终可被清理。

## AG3-053 No opaque persistence

JSONL、history API、RPC、stderr均不含：

```text
encrypted_content
signature
provider opaque marker
```

## AG3-054 Live text smoke

默认ignored。

## AG3-055 Live reasoning Tool smoke

默认ignored。

---

# 53. 核心测试：Events

## AG3-056 Runtime drop merge

`LoopEventEnvelope.dropped_before`进入Agent Event。

## AG3-057 Agent drop merge

全局Agent channel满时累计。

## AG3-058 Ignored Finished drop

Runtime Finished携带drops，即使事件本身忽略，drops进入下一Agent Event。

## AG3-059 TurnFinished after persistence

事件中的 persistence准确。

## AG3-060 Event gap不影响结果

wait/history权威。

## AG3-061 State

Running/Waiting/Finishing/Idle映射正确。

## AG3-062 Request metadata

request_index/config revision/model/reasoning正确。

---

# 54. 核心测试：RPC

## AG3-063 Capability discovery

profile/model/session list。

## AG3-064 Create/open/history

新wire shape。

## AG3-065 Send/wait

Deferred wait不阻塞reader。

## AG3-066 Steer while wait

`turn.wait` pending时仍可 `turn.steer`。

## AG3-067 Update while wait

仍可 `session.update`。

## AG3-068 Cancel while wait

仍可 cancel。

## AG3-069 Interaction while wait

仍可 answer。

## AG3-070 Event and response interleave

按request ID/TurnRef关联。

## AG3-071 History safe view

不显示 Tool arguments和opaque reasoning。

## AG3-072 Persistence failure view

turn.wait成功返回 persistence=failed，Session state blocked。

## AG3-073 Shutdown

等待active Session task，无orphan。

---

# 55. 现有能力回归

## AG3-074 Workspace escape

保持。

## AG3-075 Atomic write/edit

保持。

## AG3-076 Apply patch

保持。

## AG3-077 Bash cancel/timeout

保持。

## AG3-078 Bash credential removal

保持。

## AG3-079 stdout/stderr隔离

保持。

## AG3-080 Config fail-fast

保持。

## AG3-081 Provider error redaction

保持。

## AG3-082 Model list能力

保持。

## AG3-083 多Session并发

一个Session阻塞不影响另一个。

---

# 56. 删除和重写旧测试

删除只验证 Runtime v0.3职责的测试：

```text
SessionLog AppendReceipt
Manifest identity
ConversationSeq
expected head
durability unknown
Session degraded
restart repair
SessionPump completion barrier
MetadataWorker fault matrix
SessionRuntime open cancellation
TurnTerminal durable ordering
```

保留并迁移：

```text
Workspace
Tools
Provider
RPC framing
Event redaction
Profile/Model config
真实TUI flow
```

不要把旧测试仅改类型名后保留。

---

# 57. TUI Flow 验收

最终 Agent应支持以下完整流程：

```text
spawn minicore-agent --stdio
→agent.ping
→model.list
→profile.list
→session.list

→session.create
→session.history

→turn.send
→立即turn.wait
→同时消费agent.event
→Text/Reasoning streaming
→Tool lifecycle

运行中：
→turn.steer
→session.update model/reasoning
→turn.cancel
→interaction.answer

完成：
→turn.wait TurnResult
→session.history durable内容
→下一次turn.send
```

TUI不需要理解：

```text
AgentLoop owner
LoopReport raw History
Store line
ExecutionConfig
```

---

# 58. 不实现的功能

本轮明确不实现：

```text
Compaction
Memory
RAG
Skills
MCP
Plugin system
Subagent Tool
AgentHandle
Session fork
Session branch
历史编辑
多客户端同步
Event replay
Event ACK
实时Bash stdout
PTY
Sandbox
跨进程data_dir锁
自动旧Session迁移
持久化retry queue
数据库
```

这些能力后续均可在新的 Agent Session边界上实现。

---

# 59. 代码精简目标

当前旧 Store、Session和Context包含大量Runtime v0.3适配与fault matrix。

迁移后粗略目标：

```text
src/store.rs
    显著缩减，只保留约500～900行实现与必要测试

src/sessions.rs
    约400～700行实现与测试

src/context.rs
    删除

src/prompt.rs
    约150～300行

src/agent.rs
    删除旧Runtime映射后显著收敛

src/event.rs
    删除SessionHealth/Terminal/Conversation映射
```

整体生产代码预期净减少：

```text
约2,000～4,000行
```

行数不是硬验收，但如果迁移后生产代码反而增长，应检查是否重新建立了：

```text
Actor
Service层
可靠Event
持久化状态机
双History
```

---

# 60. 过度编程禁止项

禁止新增：

```text
AgentService
SessionService
SessionRepository trait
HistoryRepository trait
ExecutionConfigFactory
SessionBuilder
LoopManager
LoopRegistry
TaskSupervisor
PersistenceManager
EventHub
Middleware stack
HookBus
PluginRegistry
State machine library
Generic Store trait
Generic transaction abstraction
```

允许：

```text
一个Session shared state
一个短std Mutex
一个Session I/O async Mutex
一个ActiveLoop task
一个completion watch
一个全局Agent Event channel
```

---

# 61. 验收矩阵

## 架构

| ID | 验收 |
|---|---|
| A3-001 | Runtime revision固定为87f3cf9 |
| A3-002 | Agent不引用SessionRuntime |
| A3-003 | Agent不引用SessionLog |
| A3-004 | Agent不引用SessionSpec/Manifest |
| A3-005 | Agent不引用ConversationSeq/Transcript |
| A3-006 | Session是完整History唯一owner |
| A3-007 | 一个用户Turn创建一个AgentLoop |
| A3-008 | Runtime不重新拥有Session |
| A3-009 | 不增加旧Runtime兼容层 |
| A3-010 | 不修改minicore-runtime |

## Session

| ID | 验收 |
|---|---|
| A3-011 | Session保存record/workspace/history/config/options |
| A3-012 | 一个Session最多一个ActiveLoop |
| A3-013 | Profile只用于创建 |
| A3-014 | Profile删除后已有Session可打开 |
| A3-015 | Session close取消并等待active |
| A3-016 | Session delete拒绝loaded |
| A3-017 | Blocked语义正确 |
| A3-018 | 无Session actor |

## Store

| ID | 验收 |
|---|---|
| A3-019 | 新格式只有session.json/history.jsonl |
| A3-020 | 一Loop一行 |
| A3-021 | 最终partial line repair |
| A3-022 | 中间corrupt严格失败 |
| A3-023 | list跳过坏Session |
| A3-024 | open坏Session严格失败 |
| A3-025 | 旧格式不被覆盖 |
| A3-026 | 无AppendReceipt/expected head |
| A3-027 | 无durability unknown |
| A3-028 | 无fsync协议 |
| A3-029 | 显式路径拒绝symlink |
| A3-030 | append失败不合并History |

## History

| ID | 验收 |
|---|---|
| A3-031 | Session内History使用Arc slice |
| A3-032 | LoopRequest不复制完整History |
| A3-033 | 成功保存后才合并 |
| A3-034 | Report只包含当前delta |
| A3-035 | opaque reasoning不持久化 |
| A3-036 | ToolCall arguments持久化 |
| A3-037 | RPC不展示arguments |
| A3-038 | History分页offset稳定 |
| A3-039 | 不恢复Terminal ledger |
| A3-040 | 多轮History正确 |

## Runtime integration

| ID | 验收 |
|---|---|
| A3-041 | AgentLoop owner由一个task持有 |
| A3-042 | LoopHandle用于control |
| A3-043 | Agent wait发生在持久化之后 |
| A3-044 | Runtime Finished不冒充Agent完成 |
| A3-045 | Runtime Event drop合并 |
| A3-046 | Runtime LoopFailure返回为TurnResult |
| A3-047 | Event不是权威结果 |
| A3-048 | shutdown无orphan task |

## Steer/update

| ID | 验收 |
|---|---|
| A3-049 | turn.steer调用Runtime queue |
| A3-050 | Agent无第二steer queue |
| A3-051 | 多steer顺序持久化 |
| A3-052 | stale LoopId拒绝 |
| A3-053 | idle update影响下一Loop |
| A3-054 | running update影响下一request |
| A3-055 | current Tool batch不被更新 |
| A3-056 | update持久化失败不改runtime |
| A3-057 | final race返回active_revision null |
| A3-058 | RequestStarted确认实际生效 |

## Prompt

| ID | 验收 |
|---|---|
| A3-059 | ContextProvider完全删除 |
| A3-060 | ProjectPromptProvider实现Runtime PromptProvider |
| A3-061 | AGENTS每request读取 |
| A3-062 | AGENTS不存在不失败 |
| A3-063 | AGENTS边界安全 |
| A3-064 | 不实现Compaction |
| A3-065 | ProfileCompaction删除 |
| A3-066 | DefaultPromptProvider复用 |

## Model/Tool

| ID | 验收 |
|---|---|
| A3-067 | OpenAI key改为LoopId |
| A3-068 | continuation使用request_index |
| A3-069 | 模型切换不跨Provider continuation |
| A3-070 | Provider opaque不进Store |
| A3-071 | 五个Tool行为保持 |
| A3-072 | Bash credential removal保持 |
| A3-073 | Policy/Interaction保持 |
| A3-074 | Tool无法访问Session内部 |

## RPC/TUI

| ID | 验收 |
|---|---|
| A3-075 | TurnRef为session_id+loop_id |
| A3-076 | session.history替换transcript |
| A3-077 | turn.steer存在 |
| A3-078 | session.update存在 |
| A3-079 | turn.wait不阻塞reader |
| A3-080 | update/steer/cancel可与wait并行 |
| A3-081 | History view脱敏 |
| A3-082 | TurnResult含persistence |
| A3-083 | Session state含blocked |
| A3-084 | Event/response交错可关联 |

## 工程

| ID | 验收 |
|---|---|
| A3-085 | Agent版本0.3.0 |
| A3-086 | Rust 1.85通过 |
| A3-087 | stable通过 |
| A3-088 | Linux通过 |
| A3-089 | macOS通过 |
| A3-090 | Windows通过 |
| A3-091 | fmt通过 |
| A3-092 | clippy -D warnings通过 |
| A3-093 | rustdoc -D warnings通过 |
| A3-094 | unsafe_code=forbid |
| A3-095 | 无无用dependency |
| A3-096 | README/RPC文档一致 |
| A3-097 | Live smoke默认ignored |
| A3-098 | stdout仍只有JSON-RPC |
| A3-099 | stderr不泄漏内容/凭据 |
| A3-100 | 旧Session迁移明确不包含 |

---

# 62. 验证命令

每个正式提交：

```bash
cargo fmt --all -- --check
```

```bash
cargo test \
  --locked \
  --all-targets
```

```bash
cargo clippy \
  --locked \
  --all-targets \
  -- \
  -D warnings
```

```bash
RUSTDOCFLAGS="-D warnings" \
cargo doc \
  --locked \
  --no-deps
```

如有仓库检查脚本：

```bash
./scripts/check.sh
./scripts/check-architecture.sh
```

Live：

```bash
OPENAI_API_KEY=... \
MINICORE_AGENT_LIVE_MODEL=... \
MINICORE_AGENT_LIVE_REASONING=medium \
cargo test --locked openai_live_reasoning_tool_smoke \
  -- --ignored --nocapture
```

没有凭据时必须写：

```text
未执行
```

不能声称通过。

---

# 63. CI

继续覆盖：

```text
Rust stable / Ubuntu
Rust 1.85 / Ubuntu
stable / macOS
stable / Windows
```

普通CI：

```text
不注入真实Provider key
不运行Live smoke
```

---

# 64. README 必须说明

```text
minicore-agent拥有Session和History
minicore-runtime只运行一次AgentLoop
一个turn.send创建一个新AgentLoop
模型/reasoning可以在Session中热更新
running update在下一个Model Request生效
Steer在下一个Model Request生效
当前Tool batch使用旧snapshot
History由Agent JSONL保存
Runtime Event和Agent Event均可丢失
turn.wait是产品权威结果
TurnResult区分persistence状态
persistence失败后Session blocked
close/open从最后成功JSONL恢复
旧v0.2 Session数据不自动迁移
Bash不是sandbox
同一data_dir只支持一个Agent进程
Compaction尚未实现
Subagent尚未实现
```

---

# 65. 最终交付报告

开发 Agent 必须报告：

```text
实际起始HEAD
最终HEAD
Runtime pinned revision
提交列表
新增文件
删除文件
Public API变化
RPC变化
Store格式
旧数据兼容范围
Session ownership
ActiveLoop lifecycle
Steer/update语义
Persistence failure语义
OpenAI continuation迁移
History清洗规则
删除的旧代码规模
生产/测试代码行数变化
测试结果
CI结果
A3-001～A3-100结果
Live smoke结果或未执行说明
已知限制
```

---

# 66. 完成定义

只有满足以下条件，本次 Agent迁移才完成：

```text
minicore-agent完全编译于Runtime v0.4
不存在Runtime v0.3 Session API引用
Agent Session是完整History唯一owner
一条用户消息对应一个AgentLoop
多轮对话由多个AgentLoop组成
LoopReport成功写JSONL后才进入Session History
持久化失败返回Report但阻止后续Turn
Steer直接使用Runtime queue
running模型更新在下一个request生效
OpenAI continuation以LoopId/request_index隔离
PromptProvider取代ContextProvider
AGENTS.md仍可动态注入
五个Tool和Policy保持
Event drop正确合并
turn.wait等待Agent级持久化completion
RPC支持history/steer/update
不实现Compaction/Subagent/Plugin
旧Session数据不被破坏
生产代码明显收敛
A3-001～A3-100全部通过
```

最终架构：

```text
Agent
└── Sessions
    └── Session
        ├──完整History
        ├──SessionRecord
        ├──ExecutionConfig
        ├──Workspace
        ├──JSONL
        └──ActiveLoop
            ├──AgentLoop owner
            ├──LoopHandle
            ├──LoopEventStream
            └──Agent completion

Runtime
└──一次AgentLoop
```

---

# 67. 给代码 Agent 的最终执行提示

请基于：

```text
minicore-agent:
dev@6d5e963031159c458212a92c690e515a2ac3761b

minicore-runtime:
refactor/v0.4-flex-agent-loop
@87f3cf92b9b5980b0f468174a319cf53427d858e
```

实施本文档。

必须遵守：

1. 只修改 minicore-agent；
2. 不修改 Runtime；
3. Agent升级为0.3.0；
4. Runtime pin到指定0.4 revision；
5. Session成为History和JSONL唯一owner；
6. 每次用户消息创建一个AgentLoop；
7. 不保留SessionRuntime兼容层；
8. 重写Store为session.json + history.jsonl；
9. 一Loop写一行；
10. JSONL成功后才合并内存History；
11. append失败返回TurnResult但Block Session；
12. 不实现持久化retry queue；
13. Steer只调用LoopHandle::steer；
14. 不增加第二套Steer queue；
15. Session model/reasoning更新使用完整ExecutionConfig；
16. running更新调用LoopHandle::update；
17. current Tool batch保持旧snapshot；
18. Runtime Finished不直接作为Agent TurnFinished；
19. turn.wait等待Agent级completion；
20. OpenAI key迁移为LoopId/request_index；
21. opaque reasoning不得持久化或进入RPC；
22. ContextProvider改为ProjectPromptProvider；
23. 删除未实现ProfileCompaction；
24. 保留Workspace、五个Tool、Policy和Provider；
25. 不新增AgentHandle、Subagent、Plugin或Compaction；
26. 不新增Actor、Manager、Repository trait或Supervisor；
27. Event继续best-effort；
28. RPC reader不能被turn.wait阻塞；
29. 旧v0.2 Session数据不自动迁移；
30. 完成后停止扩张Agent功能，先进入TUI联调。
