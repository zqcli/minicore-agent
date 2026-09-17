# MiniCore Agent v0.3：Closeout 修复实施规格

## 0. 文档定位

本文档用于指导代码 Agent 对已经完成 Runtime v0.4 迁移的 `minicore-agent` 做最后一次收口。

本次不是新功能迭代，也不是架构重构。

目标仅包括：

```text
1. 修复 Blocked Session 可能提前丢失已完成 TurnResult 的问题；
2. 补齐“同一个 Loop 的下一个 Model Request 热切换模型”验收测试；
3. 收紧 SessionRecord 的持久化输入校验；
4. 修正文档中 shutdown、cancel reason 和持久化保证的边界描述。
```

完成后即可冻结：

```text
minicore-agent 0.3
```

并进入：

```text
minicore-tui 联调与开发
```

---

# 1. 开发基线

```text
repository:
https://github.com/zqcli/minicore-agent

branch:
dev

reviewed HEAD:
edd1cb670dc72f61cb94f44bfdff8ca38b5a4999
```

Runtime 依赖必须继续固定为：

```text
minicore-runtime 0.4.0
87f3cf92b9b5980b0f468174a319cf53427d858e
```

开始开发前：

```bash
git fetch --all --prune
git switch dev
git pull --ff-only
git rev-parse HEAD
git status --short
```

如果 `dev` 已经前进：

1. 先检查本文问题是否已经修复；
2. 保留已有正确实现；
3. 不机械覆盖后续提交；
4. 最终报告实际起始 HEAD。

建议开发分支：

```text
fix/v0.3-closeout
```

---

# 2. 当前架构不得改变

当前架构已经符合目标：

```text
Agent
└── Sessions
    └── Session
        ├── SessionRecord
        ├──完整History
        ├──Workspace
        ├──ExecutionConfig
        ├──LoopOptions
        ├──history.jsonl
        └──ActiveLoop
            ├──LoopHandle
            ├──AgentLoop owner task
            └──Agent-level completion
```

Runtime 继续只负责：

```text
一次AgentLoop
Model/Tool循环
Steer
request-boundary update
cancel
interaction
LoopEvent
LoopReport
```

Agent 继续负责：

```text
Session
完整History
JSONL
长期model/reasoning
Workspace
RPC
```

本次禁止改变该 ownership。

---

# 3. 优先级总览

| ID | 优先级 | 类型 | 要求 |
|---|---:|---|---|
| FIX-01 | P1 | 生产正确性 | Blocked Session 不得因失败的新 send 丢失旧 TurnResult |
| TEST-01 | P1 | 行为验收 | 证明同一 Loop 的下一 Model Request 使用热更新模型 |
| FIX-02 | P2 | Store 边界 | 完善 SessionRecord 校验 |
| DOC-01 | P3 | 文档 | 明确 Agent::shutdown 才是异步清理屏障 |
| DOC-02 | P3 | 文档 | 明确 close/shutdown 当前映射为 User cancel |
| DOC-03 | P3 | 文档 | 不把当前 Store 描述为事务性、崩溃安全 durability |

只有：

```text
FIX-01
TEST-01
FIX-02
```

需要代码或测试行为修改。

后三项只要求文档和 Rustdoc 收口，不要求引入新运行时机制。

---

# 4. FIX-01：Blocked Session 不得提前丢失 TurnResult

## 4.1 当前问题

当前 `Session::cleanup_finished()` 的大致顺序是：

```text
检查active task/completion
→ take active.task
→ await task
→ inner.active = None
→ 检查inner.blocked
→ 返回SessionBlocked
```

当 `history.jsonl` append 失败时：

```text
Runtime已经返回完整LoopReport
Session.blocked = Persistence
completion中保存TurnResult {
    report,
    persistence = Failed
}
```

此时如果调用方尚未执行 `turn.wait`，而是先尝试发送下一条消息：

```text
turn.send(next)
→ start_loop
→ cleanup_finished
→ 删除active slot
→ 返回SessionBlocked
```

随后调用：

```text
turn.wait(previous)
```

会得到：

```text
TurnNotFound
```

原本已经生成的权威 `TurnResult` 被失败的新 `send` 提前删除。

这违反：

```text
只要Runtime Report存在，turn.wait应能返回完整结果
```

以及：

```text
persistence=Failed必须可被调用方观察
```

---

## 4.2 目标语义

当 Session 已经 Blocked 时：

```text
send
→ SessionBlocked

update
→ SessionBlocked
```

但不得删除：

```text
完成的ActiveLoop
completion watch
TurnResult
```

旧 TurnResult 至少应一直保留到：

```text
session.close
Agent::shutdown
```

因此以下顺序必须成立：

```text
Turn完成
→ persistence失败
→ Session Blocked
→ 尝试新send，返回SessionBlocked
→ 旧turn.wait仍返回TurnResult(persistence=Failed)
```

---

## 4.3 最小修改方案

修改：

```text
src/sessions.rs
Session::cleanup_finished
```

在读取和回收 active task 之前，先检查：

```rust
if inner.blocked.is_some() {
    return Err(AgentError::SessionBlocked);
}
```

推荐结构：

```rust
pub(crate) async fn cleanup_finished(
    &self,
) -> Result<(), AgentError> {
    let (finished, completed) = {
        let inner = self.shared.inner.lock().unwrap();

        if inner.blocked.is_some() {
            return Err(AgentError::SessionBlocked);
        }

        match inner.active.as_ref() {
            None => return Ok(()),
            Some(active) => (
                active
                    .task
                    .as_ref()
                    .is_some_and(JoinHandle::is_finished),
                active.completion.borrow().is_some(),
            ),
        }
    };

    // 保留现有后续逻辑。
}
```

也可以先 clone：

```text
blocked
finished
completed
```

再在锁外判断。

关键不变量只有一个：

> `cleanup_finished()` 在 Blocked 状态下不得 take task，也不得设置 `active = None`。

---

## 4.4 不要修改的行为

正常成功 Turn：

```text
下一次send可以回收旧active并启动新Loop
```

正常取消 Turn：

```text
下一次send可以继续
```

Internal/Persistence Blocked：

```text
拒绝下一次send
保留旧completion
```

`session.close`：

```text
仍可take active
cancel（若仍active）
await task
清理Session
```

`Agent::shutdown`：

```text
仍可回收所有active task
```

---

## 4.5 必须新增测试

建议测试名：

```rust
blocked_send_does_not_discard_previous_turn_result
```

测试流程：

```text
1. 创建Session；
2. 注入下一次history append失败；
3. turn.send得到TurnRef；
4. 不调用turn.wait；
5. 等待session.state == Blocked；
6. 调用第二次turn.send；
7. 断言返回SessionBlocked；
8. 调用第一次TurnRef的turn.wait；
9. 断言返回成功的TurnResult；
10. 断言result.persistence == Failed；
11. 断言result.report.outcome仍是原Runtime结果；
12. 再次turn.wait仍可读取同一结果；
13. session.close成功回收task。
```

等待 Blocked 状态时不得：

```text
sleep固定100ms后直接假设
```

使用：

```text
poll state + deadline
Notify/test gate
或completion readiness
```

避免脆弱时间测试。

---

## 4.6 RPC 回归测试

增加或扩展 RPC 测试：

```text
1. 注入append失败；
2. turn.send；
3. 等待Session blocked；
4. 再turn.send → -32004 session_blocked；
5. 原turn.wait仍成功返回：
   persistence = "failed"
   outcome = 原Runtime outcome
```

确保一次失败的产品操作不会让先前权威结果变成：

```text
-32007 turn_not_found
```

---

# 5. TEST-01：补齐同一 Loop 的 request-boundary 模型热切换测试

## 5.1 当前覆盖缺口

现有测试已经证明：

```text
Running期间session.update被接受
当前in-flight Request继续使用Model A
下一次新Turn使用Model B
```

但尚未完整证明最关键的 Runtime v0.4 契约：

```text
同一个AgentLoop：

Request 0 → Model A
Tool batch
Request 1 → Model B
```

因此目前只能证明：

```text
长期Session设置已更新
```

还不能从 Agent 集成测试层完整证明：

```text
LoopHandle::update确实传递到同一Loop的下一个Model Request
```

---

## 5.2 目标场景

构造两个 Fake Model：

```text
Model A
    Request 0等待gate
    gate释放后返回read ToolCall

Model B
    Request 1返回最终Text + Stop
```

执行：

```text
turn.send
→ Model A进入Request 0并等待gate
→ session.update(model=B)
→ 获得active_revision
→ 释放Model A
→ Runtime执行read Tool
→ 同一个Loop开始Request 1
→ 使用Model B
→ Loop完成
```

---

## 5.3 推荐测试实现

现有测试已经有：

```rust
ModelScript::ToolCallAfterGate(...)
```

优先复用，不增加新的通用 Fake Runtime。

建议测试名：

```rust
running_model_update_applies_to_next_request_in_same_loop
```

大致结构：

```rust
let gate = BlockGate::new();

let model_a = FakeModel::new(
    "main",
    [
        ModelScript::ToolCallAfterGate(
            gate.clone(),
            "read",
            json!({
                "path": "a.txt",
                "limit": 32
            }),
        ),
    ],
);

let model_b = FakeModel::new(
    "other",
    [
        ModelScript::Text("from model b"),
    ],
);

let turn = send_text(
    &mut agent,
    session_id,
    "read then answer",
).await;

gate.entered.notified().await;

let updated = agent
    .update_session(UpdateSession {
        session_id,
        model: Some("other".to_owned()),
        reasoning: None,
    })
    .await
    .unwrap();

let revision = updated
    .active_revision
    .expect("running loop must accept update");

gate.release.notify_waiters();

let result = agent
    .wait_turn(turn)
    .await
    .unwrap();
```

---

## 5.4 必须断言

```text
result.turn.loop_id == 原turn.loop_id
result.report.outcome == Completed
result.report.requests == 2
result.report.tool_rounds == 1
result.report.final_config_revision == active_revision

Model A收到1次request
Model B收到1次request

History中包含：
    Assistant ToolCall（Model A）
    ToolResult
    最终Assistant Text（Model B）

最终AssistantHistory.model == Model B
最终AssistantHistory.request_index == 1
```

如测试同时消费 Event，则再断言：

```text
RequestStarted request_index=0：
    model=A
    config_revision=INITIAL

RequestStarted request_index=1：
    model=B
    config_revision=active_revision
```

Event 是 best-effort，但本测试应：

```text
使用足够大的Event capacity
并发消费Event
不制造channel压力
```

使该测试稳定。

---

## 5.5 当前 Tool batch 契约

该测试还必须确认：

```text
Model A产生的read ToolCall被正常执行
```

这间接证明：

```text
当前Request产生的Tool batch
没有被中途update破坏
```

本阶段只允许更新：

```text
model
reasoning
```

不要为了测试 Tool snapshot 新增动态 ToolSet 更新接口。

---

## 5.6 如果测试失败

只修复最小集成错误，例如：

```text
Session没有调用LoopHandle::update
错误地在Loop结束后才调用
传入了错误ExecutionConfig
active handle匹配错误
```

禁止：

```text
修改Runtime热更新语义
新增Agent config queue
重启整个Loop
取消当前Model Request
重放当前Tool batch
```

---

# 6. FIX-02：完善 SessionRecord 校验

## 6.1 当前问题

当前 `SessionRecord::validate()` 主要检查：

```text
format version
字段长度
workspace非空
max_tool_rounds
时间字符串基本格式
Tool名称文本长度
```

但没有完整校验：

```text
system_prompt不能为空
system_prompt控制字符
Tool是否属于KNOWN_TOOL_NAMES
Tool是否重复
model ID是否符合Agent模型ID约束
```

正常由 Agent 创建的数据不会出现这些问题。

但手工编辑、磁盘损坏或不兼容数据可能导致：

```text
session.list将非法record当作健康Session展示
session.open直到较晚的ExecutionConfig构造阶段才失败
Store边界没有维持SessionRecord自身的不变量
```

---

## 6.2 目标语义

`SessionRecord::validate()` 应保证：

```text
它是一个结构上合法的Agent Session配置快照
```

Store 不需要验证：

```text
model当前是否仍存在于Models registry
model是否支持当前reasoning
model是否支持Tools
workspace当前是否仍存在
```

这些仍由：

```text
Agent::open_session
Agent::execution_config
Workspace::open
```

检查。

---

## 6.3 必须增加的校验

### System prompt

要求：

```text
非空
<= MAX_SYSTEM_PROMPT_BYTES
允许LF和TAB
拒绝其他控制字符
```

不要复用当前会拒绝所有控制字符的通用 `valid_text`。

可以增加窄 helper：

```rust
fn valid_multiline_text(
    value: &str,
    maximum: usize,
    allow_empty: bool,
) -> bool {
    (allow_empty || !value.is_empty())
        && value.len() <= maximum
        && value.chars().all(|character| {
            !character.is_control()
                || matches!(character, '\n' | '\t')
        })
}
```

### Tool names

要求：

```text
每个名称属于KNOWN_TOOL_NAMES
无重复
数量不超过KNOWN_TOOL_NAMES.len()
```

推荐：

```rust
let mut tools = BTreeSet::new();

let valid_tools = self.tools.iter().all(|name| {
    KNOWN_TOOL_NAMES.contains(&name.as_str())
        && tools.insert(name.as_str())
});
```

不要增加 Tool Registry trait。

### Model ID

复用现有唯一的 Model ID 规则。

优先调用已有：

```rust
Models::model_ref(&self.model)
```

或将一个窄的 `valid_model_id` helper放在 `models.rs` 供 Config和Store复用。

不要：

```text
分别在config.rs和store.rs复制两套格式规则
增加regex依赖
```

### Profile

继续要求：

```text
非空
有界
无控制字符
```

不需要要求当前 Profiles registry中仍存在。

---

## 6.4 不要在 Store 校验的内容

不要增加：

```text
Workspace::open
Model registry lookup
reasoning capability lookup
Tool对象构造
Policy构造
PromptProvider构造
网络Provider初始化
```

Store validation必须保持：

```text
纯同步
无I/O
无Provider副作用
```

---

## 6.5 必须新增测试

### 合法记录

```text
多行system prompt（含LF/TAB）
已知且不重复的Tool
合法model ID
→ validate成功
```

### 空 system prompt

```text
→ InvalidRecord
```

### system prompt非法控制字符

例如：

```text
NUL
U+0001
裸CR
```

预期：

```text
InvalidRecord
```

CRLF是否允许：

```text
SessionRecord正常由Config生成时建议已规范化；
如果未规范化，则可允许CRLF或统一拒绝裸CR。
```

不要无意拒绝普通换行。

### 未知 Tool

```text
["read", "unknown"]
→ InvalidRecord
```

### 重复 Tool

```text
["read", "read"]
→ InvalidRecord
```

### 非法 Model ID

```text
空格
控制字符
不满足ModelRef/Agent model ID格式
→ InvalidRecord
```

### List/Open 行为

手工写入非法 `session.json`：

```text
session.list
→ 跳过该Session

session.open
→ Store错误
```

不得 panic。

---

# 7. DOC-01：明确 Agent::shutdown 是清理屏障

## 7.1 当前边界

可靠清理路径是：

```rust
Agent::shutdown(self).await
```

它会：

```text
遍历loaded Sessions
取消active Loop
等待Agent-owned worker
等待持久化收尾
```

但直接：

```rust
drop(agent)
```

不是异步等待屏障。

一个 active worker可能仍持有：

```text
Session clone
AgentLoop owner
Store
```

并继续运行一段时间。

---

## 7.2 文档修改

在：

```text
README.md
Agent结构Rustdoc
Agent::shutdown Rustdoc
```

加入明确描述：

> `Agent::shutdown` is the cleanup barrier for embedded Rust callers. Dropping an Agent with live turns does not synchronously wait for Agent-owned loop tasks.

中文含义：

```text
Rust嵌入方必须调用Agent::shutdown
直接Drop不保证活动Turn已经停止和收尾
```

---

## 7.3 本次禁止增加

不要增加：

```rust
impl Drop for Agent
```

去模拟异步 shutdown。

Drop不能：

```text
await
可靠join task
确认JSONL持久化
```

增加 best-effort cancel也无法替代明确的异步清理屏障，且可能产生双重控制。

因此本项只改文档。

---

# 8. DOC-02：明确 close/shutdown 的取消原因

## 8.1 当前行为

当前 Session shutdown路径调用：

```rust
LoopHandle::cancel()
```

因此 Runtime最终 outcome通常记录为：

```text
CancelReason::User
```

即使触发来源是：

```text
session.close
agent.shutdown
RPC EOF
Ctrl-C
```

这是一项当前 v0.3 的简化限制。

---

## 8.2 文档修改

在：

```text
README.md
docs/rpc.md
Agent::close_session Rustdoc
Agent::shutdown Rustdoc
```

写明：

> MiniCore Agent v0.3 uses the Runtime user-cancellation path when closing or shutting down an active Session; it does not currently preserve a distinct shutdown cancellation reason.

不要宣称：

```text
session.close
→ CancelReason::Shutdown
```

除非代码真实实现。

---

## 8.3 本次不修改生产语义

不要为区分 cancel reason 新增：

```text
shutdown oneshot
第二控制channel
worker command enum
AgentLoop owner proxy
新的Runtime API
```

该分类差异不影响：

```text
TUI基本功能
History正确性
Loop停止
task回收
```

以后若产品确实需要来源区分，再单独设计。

---

# 9. DOC-03：收紧持久化保证描述

## 9.1 当前实际保证

当前 Store 提供：

```text
session.json临时文件 + rename
history.jsonl append + flush
最终partial line repair
中间损坏严格失败
```

当前实现不是：

```text
数据库事务
跨文件原子提交
checksum ledger
崩溃后强durability证明
跨进程协调
```

`session.json` 可能调用：

```text
file.sync_all
```

但这不等于：

```text
整个Store拥有端到端事务性durability协议
```

---

## 9.2 文档修改

将容易过度承诺的：

```text
durable
durably completed
crash-safe transaction
```

改成更准确的：

```text
persisted
successfully appended
persistent session record
best-effort crash tail repair
```

README建议说明：

> `persistence: persisted` means the Agent's append operation completed successfully in the running process. The Store is not a transactional ledger and does not provide an end-to-end crash-durability proof.

---

## 9.3 不要求修改 I/O 实现

本次不要仅为了文案一致性：

```text
删除sync_all
增加目录fsync
增加history sync_data
建立事务协议
```

保持现有 I/O即可。

---

# 10. 明确不修的内容

以下不是本次收尾任务：

```text
旧Turn长期结果registry
多个历史Turn都可wait
TurnResult持久化查询API
AgentHandle
Subagent
Compaction
Memory
RAG
Skills
MCP
Plugin系统
实时Bash stdout
PTY
Sandbox
Event ACK/replay
跨进程Store锁
Store自动retry
Session unblock RPC
旧v0.2数据迁移
新Provider
Tool热更新
System prompt热更新
Workspace热更新
```

---

# 11. 架构限制

禁止新增：

```text
TurnManager
LoopRegistry
CompletionRepository
Session actor
Agent command channel
Persistence supervisor
Background retry worker
Result cache service
Generic Store trait
Generic Validator framework
```

FIX-01 应是：

```text
cleanup_finished中的局部顺序修正
+
测试
```

FIX-02 应是：

```text
SessionRecord::validate中的局部检查
+
测试
```

TEST-01 应主要是：

```text
测试代码
```

生产代码如通过测试，不应修改。

---

# 12. 预计改动规模

合理范围：

```text
生产代码：
约30～100行

测试：
约120～300行

README/Rustdoc：
约20～60行
```

不应出现：

```text
新文件超过300行
新增依赖
大范围模块移动
RPC wire breaking change
Runtime修改
```

---

# 13. 推荐提交

## Commit 1

```text
fix(session): preserve blocked turn completion
```

包含：

```text
FIX-01
Agent API测试
RPC回归测试
```

## Commit 2

```text
test(agent): verify same-loop request-boundary model updates
```

包含：

```text
TEST-01
```

如果测试发现真实生产bug，可在同一提交做最小修正，并在提交说明中解释。

## Commit 3

```text
fix(store): validate persisted session settings
```

包含：

```text
FIX-02
```

## Commit 4

```text
docs(agent): clarify shutdown and persistence boundaries
```

包含：

```text
DOC-01
DOC-02
DOC-03
```

不要混入无关格式化或重命名。

---

# 14. 必须执行的验证

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

旧 Runtime API检查：

```bash
rg \
  'SessionRuntime|SessionSpec|SessionManifest|SessionLog|AppendReceipt|ConversationSeq|SessionInstanceId|TurnId|ContextProvider|CompactionStrategy|SessionPump|MetadataWorker' \
  src
```

生产代码预期：

```text
零命中
```

Legacy文件名只允许在：

```text
Store旧格式识别
迁移说明
测试
```

中出现。

---

# 15. CI 验收

必须通过：

```text
Quality
Ubuntu stable tests
macOS stable tests
Windows stable tests
Rust 1.85 tests
```

Live OpenAI smoke保持：

```text
#[ignore]
```

没有真实凭据时报告：

```text
未执行
```

不得声称通过。

---

# 16. 完整验收矩阵

## FIX-01

| ID | 验收 |
|---|---|
| C-001 | append失败后Session进入Blocked |
| C-002 | Blocked Session新send返回SessionBlocked |
| C-003 | 失败的新send不清除旧active slot |
| C-004 | 旧turn.wait仍返回TurnResult |
| C-005 | TurnResult.persistence == Failed |
| C-006 | Runtime outcome完整保留 |
| C-007 | 重复wait仍可读取结果 |
| C-008 | close可回收Blocked active task |
| C-009 | shutdown无orphan task |
| C-010 | RPC新send错误后旧wait仍成功 |

## TEST-01

| ID | 验收 |
|---|---|
| C-011 | Request 0由Model A处理 |
| C-012 | Running update返回active_revision |
| C-013 | Request 0产生ToolCall |
| C-014 | Tool batch正常执行 |
| C-015 | 同一Loop Request 1由Model B处理 |
| C-016 | report.requests == 2 |
| C-017 | report.tool_rounds == 1 |
| C-018 | final_config_revision == active_revision |
| C-019 | 最终AssistantHistory.model == B |
| C-020 | 最终AssistantHistory.request_index == 1 |
| C-021 | 没有启动第二个Turn |
| C-022 | 当前Request没有被update取消 |

## FIX-02

| ID | 验收 |
|---|---|
| C-023 | 合法SessionRecord通过 |
| C-024 | 空system prompt拒绝 |
| C-025 | 非法控制字符拒绝 |
| C-026 | 合法LF/TAB保留 |
| C-027 | 未知Tool拒绝 |
| C-028 | 重复Tool拒绝 |
| C-029 | 非法Model ID拒绝 |
| C-030 | list跳过非法record |
| C-031 | explicit open非法record失败 |
| C-032 | Store校验无I/O/Provider副作用 |

## 文档

| ID | 验收 |
|---|---|
| C-033 | README说明Agent::shutdown是清理屏障 |
| C-034 | Rustdoc说明直接Drop不等待 |
| C-035 | 文档不再声称close产生Shutdown reason |
| C-036 | 文档说明close/shutdown当前使用User cancel路径 |
| C-037 | 文档不声称Store是事务ledger |
| C-038 | persistence=Persisted定义准确 |
| C-039 | 不修改现有I/O协议 |
| C-040 | 不增加新的控制channel |

## 工程

| ID | 验收 |
|---|---|
| C-041 | Runtime revision不变 |
| C-042 | Agent版本不因closeout无故升级 |
| C-043 | 无新增依赖 |
| C-044 | 无RPC breaking change |
| C-045 | 无Runtime修改 |
| C-046 | fmt通过 |
| C-047 | clippy通过 |
| C-048 | rustdoc通过 |
| C-049 | 全测试通过 |
| C-050 | Rust 1.85/macOS/Windows/Linux通过 |

---

# 17. 最终完成定义

只有满足以下条件才可冻结 Agent v0.3：

```text
Blocked Session不会因失败的新send丢失旧TurnResult
同一个Loop的下一request热切换测试通过
SessionRecord持久化边界校验完整
Agent::shutdown清理边界文档准确
close/shutdown cancel reason文档准确
Store持久化保证文档不过度承诺
没有修改Runtime
没有新增架构层
没有新增依赖
CI全部通过
```

---

# 18. 给代码 Agent 的直接执行提示

请基于：

```text
minicore-agent:
dev@edd1cb670dc72f61cb94f44bfdff8ca38b5a4999

minicore-runtime:
87f3cf92b9b5980b0f468174a319cf53427d858e
```

完成 v0.3 closeout。

严格要求：

1. 先修复 `Session::cleanup_finished()`；
2. Blocked时不得take task或清除active；
3. 新send失败后旧turn.wait必须继续成功；
4. 补Agent和RPC两层回归测试；
5. 增加同一Loop A→Tool→B热切换测试；
6. 必须验证同一Loop Request 1使用Model B；
7. 不用下一次新Turn代替该验收；
8. 完善SessionRecord system prompt、Tool和Model ID校验；
9. Store校验保持同步纯函数；
10. 不增加Validator框架；
11. 文档说明Agent::shutdown才是清理屏障；
12. 不增加异步Drop实现；
13. 文档说明close/shutdown当前使用User cancel路径；
14. 不增加shutdown控制channel；
15. 文档说明Store不是事务ledger；
16. 不修改现有fsync/flush实现；
17. 不修改RPC wire；
18. 不修改Runtime；
19. 不增加依赖；
20. 完成后运行全部CI。
