# MiniCore Agent：GUI/TUI 共用数据能力开发蓝图

日期：2026-09-13  
性质：后续开发蓝图，不是已实现功能的验收报告。  
目标：完善 Agent 的数据、执行控制和查询能力，不承担 GUI/TUI 的呈现逻辑。

## 1. 基线、范围与已确定的决策

本次核对的 `minicore-agent/dev`：

```text
8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0
```

当前压缩计划引用的 Runtime 基线：

```text
minicore-runtime 0.4.1
6cd2bdbc634437dea925495c61c7eb0be10ba171
```

开发开始时记录实际 HEAD，对照后续增量，不覆盖已经正确完成的实现。

本轮范围：

| 工作包 | 目标 |
|---|---|
| Session 只读访问 | `session.read`，查看会话不等于打开执行环境 |
| 完整压缩 | 手动、启动前、请求级自动、明确上下文拒绝后的一次恢复 |
| Workspace 查询 | `workspace.files/read/search` |
| 改动审查 | `workspace.status`、`changes.list/diff`，区分工作区变化与工具变更 |
| Bash 过程 | stdout/stderr 流、结构化退出状态、取消、输出补读 |
| 工具数据 | 开始时发送参数与目标，执行中报告真实阶段，结束后提供可查询结果 |

`turn.result`、`tool.read`、`tool.output` 是上述能力的必要结果查询入口，不是额外的任务编排产品。

继续保留：一次用户任务一个 Runtime AgentLoop；Agent Session 拥有完整历史；Event best-effort；标准 Tool 执行流程；模型配置的 request-boundary 更新。

本轮不实现 TUI/GUI、Subagent 重构、后台服务进程管理、PTY、stdin 交互、文件回滚、Git 提交/暂存、数据库、通用插件框架、持久任务恢复或多客户端同步。

关于已讨论的 Subagent，后续仍是主 Runtime 调用普通 SubagentTool，由工具派发完整 minicore-agent 实例；这里不继续扩张当前无状态子 Loop 体系。

### 1.1 当前状态必须如实区分

`docs/compaction-plan.md` 已不再是纯粹的空白计划：摘要快照基础已按仓库记录完成验收，手动 Agent/RPC 草稿已经提交但尚未验收；自动压缩与上游溢出恢复仍待完成。[S1][S2]

`src/compaction.rs` 已有 Session-local 状态、摘要来源边界、SHA-256 校验和加载失败回退；`src/prompt.rs` 已使用摘要投影。但当前 `Session::start_loop` 仍从完整 `inner.history` 构造 LoopRequest，因此不能把“已有 Prompt 投影”认定为完整解决长会话。[S3][S4][S5]

现有 Presentation 已有工具参数提取和结果缓存，但详细事件主要在工具返回后发送；read/write/edit 尚未普遍上报中间进度；Bash 已并行收集 stdout/stderr，却主要在收集完成后返回。[S6][S7][S8][S9]

这些是迁移起点，不是要求推倒重写。

## 2. 总体架构：执行、会话数据、前端呈现分开

```text
GUI / TUI
  输入、导航、折叠、滚动、语法高亮、颜色、diff 布局
                      |
              Rust API / stdio RPC
                      |
minicore-agent
  Agent facade
    Session / Store                完整历史、普通运行、压缩操作
    Workspace                      文件列表、读取、搜索、Git 状态
    Tool execution data            调用信息、阶段、输出、退出状态
    Changes                        已观察的文件修改和 diff 数据
    Compaction                     派生摘要、执行历史投影、请求预算
                      |
minicore-runtime
  一次 AgentLoop、请求边界、Tool 调用、取消、最终 Report
```

这是职责划分，不是创建六个 Service、Manager 或 Actor 的要求。优先使用现有具体类型和文件。

### 2.1 一项能力通常有两个不同入口

```text
模型 read Tool ───────────┐
                        ├── 同一个 Workspace 底层实现
前端 workspace.read ────┘
```

模型 ToolCall 进入 AgentLoop 与对话历史；用户浏览文件的只读查询不启动 Loop、不生成 ToolCall、不自动追加上下文。

### 2.2 不可破坏的边界

- 全部历史只有一个权威 owner：Session/Store。摘要只是派生数据。
- Runtime 不增加 Session、Git、文件 Review、Bash 进程目录或 UI 字段。
- 运行中事件允许丢失；结果查询必须能说明完整性、保留范围与持久化状态。
- 工具参数表达意图，不等于文件已修改；取消请求不等于进程已退出。
- 新增展示/审查资料失败，不得把实际成功的文件操作“变成没发生”，也不得自动重做工具。
- Workspace 范围限制不是 Bash sandbox；也不承诺抵御同用户并发替换文件路径的全部竞态。

## 3. 第一阶段公共基础：身份、查询与非阻塞入口

### 3.1 一个完整 ToolRef

所有工具数据、Bash 输出、改动关联统一使用：

```text
ToolRef {
  session_id,
  loop_id,
  request_index,
  tool_call_id
}
```

不得只用工具名、最近一次调用、PID 或文件路径关联。工具名、路径和命令都可能重复。

不额外引入全局 ToolRunId；现有身份足够。磁盘文件名由 Agent 根据完整身份生成安全键，不能直接拼接原始 ToolCallId 或用户路径。

### 3.2 稳定查询与提示性事件

| 类型 | 查询 | 事件 |
|---|---|---|
| 会话 | `session.read/state` | 现有 Session 事件 |
| Turn | `turn.wait/result` | 现有 Turn 事件 |
| Tool | `tool.read/output` | started、progress、finished、output delta |
| 压缩 | `session.context`、`session.compact` 结果 | operation progress/result 提示 |
| 改动 | `changes.list/diff` | 可选 `changes.updated` 失效提示 |

查询返回结构化数据，而不是 TUI 已排版文本。前端不需要通过自然语言日志判断状态。

新增字段与接口通过明确的协议版本/能力声明发布；可扩展现有 agent.ping，不必另建能力服务。兼容字段只用于一个明确迁移周期，新客户端不得依赖旧 ToolDisplay 字符串再反向解析数据。

### 3.3 先关闭 RPC 读取问题

重新核对当前 `RpcServer::run/read_frame`：不能在循环 select 的易取消 future 中独占已经消费的半帧。将累计缓冲区提升到循环外；只有完整帧/EOF 才取走。读取长度上限针对整帧累计长度。[S10][S17]

长查询和压缩沿用 deferred response，不能让一次搜索、Git 扫描或摘要调用阻塞 reader 对 cancel、ping、其他 Session 的处理。设置小而明确的并发上限，关闭连接/Agent 时取消并回收所拥有的工作；不要使用无限 spawn。

### 3.4 所有新查询都有真正的边界

统一考虑：条目数、编码后响应字节数、单文件读取量、扫描工作量、超时和结果完整性。

只限制 `limit=100` 不够：一个 History item 或单行源码就可能很大。超过页面字节预算时，返回可继续的 item/part 范围游标或内容引用，不能声称已返回完整对象。

建议沿用现有大小上限，默认页面目标约 256 KiB、输出块约 8–32 KiB；这些是待测试调优的内部默认值，不要求逐项开放 TOML 配置。

## 4. session.read：查看不等于恢复执行

### 4.1 核心语义

```text
session.read
  读取 metadata 和完整历史的一个页面
  不加载 Session 到运行列表
  不初始化 Model、Tools、Workspace 或 PromptProvider
  不启动 AgentLoop、不产生 token 消耗
  不修复/截断 history.jsonl、不刷新 updated_at
```

即使 Workspace 已删除、模型配置已移除、Profile 已变更，也应能查看格式仍然支持的旧对话。

显式 `session.open` 才验证当前执行资源、加载 Session，必要时按既有策略处理损坏尾部。

### 4.2 建议请求/返回

```text
session.read(session_id, cursor?, limit?, max_bytes?)
  -> session metadata
     history items（保留有序 message parts）
     涉及页面的已保存 Turn outcome/usage 概要
     next_cursor
     history_revision / captured_end
     trailing_incomplete（如适用）
```

加载中的 Session 从其已提交历史快照读取；未加载 Session 从 Store 只读扫描。保持同一安全投影与分页实现，避免另写一套 Transcript 转换器。

`history_revision/captured_end` 用于跨页保持一个已经观察到的完整前缀，不是新的 durable ledger。新 Turn 可以在之后追加；后续页不得混入前缀以外内容，调用方刷新后再取新前缀。

尾部未完成 JSONL 行只做只读标记；完整中间行损坏返回明确错误。不能在 `session.read` 中复用会修改尾部文件的加载函数。

### 4.3 与现有 API 的关系

`session.history` 暂时保留为兼容入口，内部复用同一读取/投影 helper。新客户端以 `session.read` 为首选；不需要立即删旧方法。

当前活动 Turn 的尚未保存内容不伪装成会话历史，交给 `turn.result` 或工具输出查询。

### 4.4 turn.result 是必要补充

```text
turn.result(TurnRef, cursor?, max_bytes?)
  -> outcome, persistence, sanitized current-loop items,
     usage, next_cursor, availability
```

必须能读取当前仍保留的 Runtime Report，哪怕本轮 JSONL append 失败。这样“事件丢失 + 持久化失败”时仍可取得答案。

已保存旧 Turn 可通过 StoredLoopRecord 查询；未保存结果在当前进程的明确保留期内可读，close/退出之后不能承诺恢复。

只返回清洗后的数据，不向 RPC 暴露 encrypted reasoning、signature 或 Provider 原始响应。

### 4.5 涉及文件

`src/agent.rs`、`src/store.rs`、`src/history.rs`、`src/sessions.rs`、`src/rpc/protocol.rs`、`src/rpc/server.rs`。

Store 增加纯只读页面读取路径；不要调用 `open_session` 模拟 read。

## 5. 完整压缩：六个连续完成的能力

### 5.1 完成标准

“全功能压缩”在本蓝图中包括：

| 层次 | 必须完成 |
|---|---|
| 手动 | loaded、idle、settled、unblocked Session 可 compact/cancel |
| 重启复用 | 验证并复用 summary.json，坏摘要不阻断原始历史读取 |
| 启动前 | 完整历史超过 Runtime 入参上限时，仍可生成合规执行历史 |
| 请求级 | 每次 Model Request 检查预算，包含工具执行后与模型切换后 |
| 溢出恢复 | 明确 ContextOverflow + NotStarted 后最多重建一次请求 |
| 可观察与预算 | 状态、before/after 估计、来源、摘要调用 usage、失败与取消 |

原有计划中 TUI `/compact` 的 UI 工作不纳入本仓库；本轮只交付 RPC 和数据。

### 5.2 原始历史与摘要

保留：

```text
history.jsonl    完整原始对话，只追加
summary.json    已结束历史前缀的派生摘要
```

沿用现有 snapshot 版本、Session 身份、覆盖 Loop/item 数、前缀 byte offset 与 SHA-256 anchor，不重新设计数据库或 manifest。[S2][S3]

摘要校验失败时：原始历史仍可阅读；执行如超预算，重新摘要或报告需要压缩，不能无条件把不合规全量历史发给模型。

压缩不能删除或重排用户看到的原始历史、时间、ToolCall/ToolResult 与已保存 Turn outcome。

### 5.3 必须补上的启动前入口

当前 Session 仍把完整 `inner.history` 传给 LoopRequest；Runtime 在 `AgentLoop::start` 检查历史项数和字节数，先于 PromptProvider。[S5][S11]

因此目标流程应为：

```text
完整 History + 已验证摘要状态
          |
Session::execution_input（新增普通 helper）
          |
近期完整历史后缀 + 绑定本次 Loop 的摘要上下文
          |
AgentLoop::start
          |
每次 Prompt prepare 再检查当前完整请求预算
```

完整 Session History 不变。LoopRequest 只接收受限的执行后缀；摘要通过本次执行绑定的 PromptProvider 注入，不算成新增用户消息，也不进入本轮 Report delta。

当前 PromptProvider 按 `covered_item_count` 对完整 base 二次切片的逻辑必须相应调整：已经在启动前裁剪过的后缀不能再按完整历史索引裁一次。[S4]

可新增一个窄 `ExecutionInput` 内部数据结构，记录 base、摘要、来源边界和设置 generation。它不是新的 Session owner。

### 5.4 摘要必须仍是数据

当前 Agent 的 `summary_data_message` 使用 user 角色和明确的历史数据边界，可继续复用。[S3]

特别注意：当前 Runtime `DefaultPromptProvider` 把 `HistoryItem::Summary` 投影为 system message。不能直接把用户/工具输出的摘要改成该类型再交给默认投影，否则提升了历史内容的指令优先级。[S12]

应在 Agent 投影中保持摘要为历史数据；真正的 system prompt 和 AGENTS.md 单独保留。标签本身不是抵御 prompt injection 的充分保护，角色和信任边界同样重要。

### 5.5 手动 compact 的生命周期

复用草稿中的 Session-owned operation、cancel token、watch result 和 join 所有权，先通过验收，再扩展。

```text
session.compact(session_id, operation_id)
  -> deferred result
session.compact.cancel(session_id, operation_id)
  -> cancellation accepted / too_late / not_active
session.context(session_id)
  -> current operation, summary coverage, last result, budget estimate
```

不增加全局 OperationManager。最多保留当前与最近一次压缩结果，明确进程内保留范围。

压缩时拒绝同 Session 新 Turn/重复 compact；模型与执行配置更新可以简单返回 busy。rename/read 不应被无意义禁止。close/shutdown 取消并 join；没有锁跨越模型调用。

写入前再次验证捕获的历史来源和配置 generation。在有界提交临界点之前取消可保证不开始提交；进入提交后不承诺回滚。失败保留旧内存投影；可能已 rename 的失败返回 unknown_write，禁止盲目重写。

### 5.6 自动压缩：启动前与运行中两处

自动策略沿用计划中的小配置面：

```toml
[compaction]
enabled = true
trigger_percent = 80
target_percent = 50
```

这是建议默认值，不是已验证最优比例。要求 `0 < target < trigger <= 100`。

启动前自动压缩须先保留 Session admission，防止同时启动两个 Turn。若摘要耗时，`turn.send` 通过 deferred response 等到真正创建 Loop 后才返回 TurnRef；期间 RPC reader 继续服务，`session.context` 可查准备中的 operation，取消该准备操作会终止此次尚未启动的提交。不能为取得 LoopId 先启动一个超预算 Loop。

运行中在每次 request prepare 检查：system、AGENTS.md、摘要、历史后缀、当前 User/Steer、工具 schema、消息 framing，以及 Provider 实际重放的相关内容。

预算从统一的“有效输入预算”获得；如果 Model 配置已经扣过输出余量和安全边界，不能再扣一遍。

`estimated` 与 Provider-reported usage 分开。累计 input tokens 不是当前 context tokens；未知不填 0。

**超时预算也必须对齐。** request 内自动摘要发生在 Runtime 的 PromptProvider deadline 之内；不能仅提高摘要 Model timeout，却继续受一个更短的 prompt_timeout 截断。启用自动压缩时明确 prompt preparation 的整体预算，并让摘要、分块、合并共用剩余时间。溢出恢复发生在 Model::start 的剩余截止时间内，不能每重试一次就重置完整 timeout。启动前准备与手动压缩使用自己的有界 operation deadline。

### 5.7 当前 Loop 大工具结果

已保存前缀摘要可持久化到 summary.json；当前 Loop 的 ToolResult 尚未进入 Store，只能生成绑定 Loop/来源组的临时执行摘要。

按完整工具交换分组：一个 Assistant 的 ToolCalls 及其全部对应 ToolResults。不能保留 call 却丢掉 result，或反过来。尚未完成的工具组不能进入摘要。

原始当前 User 和已经应用的 Steer 不被摘要替换。临时摘要结束后丢弃，不往完整 JSONL 写一个并未发生的模型回答。

模型热更新后重新按新预算评估，保留仍有效的语义摘要，失效模型相关估计/continuation/recovery ticket；不重跑工具。

### 5.8 摘要工具调用本身

使用选定 raw Model，独立 utility 身份，不带 Tool/Policy，不套自动压缩 wrapper，传播取消和总 deadline。

分块输入、合并摘要均有单请求和总量预算；单个超大文件/工具结果也需受限分块。摘要无进展、空输出、超大输出、非正常完成或超时均明确失败，不能机械截断后宣称语义压缩成功。

保留目标、约束、决策、文件改动、已执行工具及结果、未完成事项和必要精确标识。

返回 utility usage 独立计费分类，不混入主 Loop 请求数；缺失 usage 保持未知。

### 5.9 一次 ContextOverflow 恢复

调用方向：

```text
Runtime
  -> 只读模型观察 wrapper
    -> CompactingModel
      -> raw Provider Model
```

只允许结构化 `ContextOverflow + NotStarted`，在同一 logical request 内重建更小请求并再试一次。保留 loop_id/request_index；不重复 User/Steer，不重跑之前 Tool batch。

恢复票据绑定请求内容、工具 schema、模型、reasoning、配置 generation、来源摘要 generation。普通 Driver retry 重新进入 wrapper 也不能重置这次恢复预算。

Unknown delivery、网络中断、已开始流式输出、第二次 overflow 均不能自动重发。不要通过任意错误 message 子串猜 ContextOverflow。

注意 OpenAI 永久 start failure 后 continuation 可能已清理：恢复请求必须重新验证 Tool exchange，不能假设 opaque replay 仍可使用。

### 5.10 不可压缩的请求

system + 当前 User/Steer + Tool schema 本身已超过预算时，返回明确的 `context_uncompressible`；不删除用户约束，也不无限摘要。

大会话的 Runtime 历史项数上限、字节上限与 Provider token 上限是三种独立约束，三者都要验收。

### 5.11 主要修改位置

`src/compaction.rs`、现有 `src/compaction/utility.rs`、`src/prompt.rs`、`src/sessions.rs`、`src/agent.rs`、`src/store.rs`、`src/models/openai.rs`、RPC 与相关测试。

需要时只拆出 `compaction/budget.rs`、`compaction/recovery.rs` 这类职责明确的小文件；不要再造第二套 AgentLoop。

## 6. Workspace 只读数据 API

### 6.1 访问对象

本轮以已加载 SessionId 定位其 Workspace。前端只能请求该 Workspace 下的相对路径，不接受随意的主机绝对根目录。

Session.read 不需要 loaded；Workspace 实际文件查询需要明确的 Workspace owner。这两个规则不冲突。

### 6.2 workspace.files

```text
workspace.files(session_id, directory?, recursive?, query?, cursor?, limit?)
  -> entries {path, kind, optional size}
     next_cursor, truncated, scan_complete, skipped_count
```

默认列目录一层；`@文件` 使用受限递归模式，query 只过滤文件路径，不搜索内容。

遍历复用一个实现。默认尊重仓库 ignore 规则，排除 .git 元数据；非 Git 工作区采用明确的默认排除规则。复用成熟 ignore/遍历能力，不能自己写一份看似支持全部 gitignore 的简陋解析器。

不递归追踪 symlink 目录；可展示 symlink 条目，实际读取再按 Workspace 的规范化边界处理。

不建立文件 watcher、全文索引或常驻扫描任务。前端搜索去抖留在前端；后端仍独立限制扫描工作量和超时。

分页不承诺文件系统快照：返回 observed_at 和 `consistency=live`，并发文件变化可要求调用方刷新，不为此维护全工作区快照。

### 6.3 workspace.read

```text
workspace.read(session_id, path, start_line?, max_lines?, max_bytes?, if_revision?)
  -> path, content, start_line, returned_lines,
     revision, truncated, next_range, encoding/status
```

返回纯内容，不把 `23: ` 这种展示行号拼入正文；行号由 start_line/范围字段表达。

只读普通文本文件；二进制、超大文件或不支持编码返回明确状态与必要 metadata，不能把空字符串当作成功全文。

对于可在现有预算内完整读取的小文件，revision 可使用已有 SHA-256 对所读完整版本计算；不能拿部分内容的 hash 冒充整个文件版本。大文件/并发变化场景使用明确的弱版本或 changed 标记。

不把预览内容自动加进 Session History。用户正式发送附件/选区属于之后的输入契约，不在本轮实现。

### 6.4 workspace.search

```text
workspace.search(session_id, query, paths?, case_sensitive?, cursor?, max_matches?)
  -> matches {path, line_number, match_byte_ranges, line_text}
     scan_complete, truncated, skipped_files, next_cursor
```

第一版支持单行 literal 文本搜索与大小写选择，不默认接受正则，避免顺手扩张成 IDE 搜索引擎。

所有文件范围和 ignore 规则与 files 共用；path 参数不解释成 shell 命令。偏移单位明确为 UTF-8 字节，行号明确从 1 开始。

达到匹配数量、单文件大小、扫描字节或 deadline 时返回部分结果与完整性标记。扫描未完成时不得返回一个看似准确的全局 total。

RPC 使用有界 deferred query，取消/关闭时停止扫描；不要让一次大仓库搜索阻塞当前 Loop 的取消。

### 6.5 主要修改位置

`src/workspace.rs` 保留路径和文件底层；有必要时增加 `src/workspace/query.rs` 承载查询，不另建 WorkspaceRepository。

`src/agent.rs` 和 RPC 暴露只读入口。具体 read Tool 共用底层读取，但继续维持模型需要的独立 ToolOutput 格式。

## 7. 工具执行数据：替代 UI 导向的 Presentation 契约

### 7.1 Agent 只提供事实

建议增量引入：

```text
ToolInvocationData {
  tool_ref, name,
  subject: File | Command | Other,
  validated input fields or bounded input references
}

ToolExecutionData {
  tool_ref,
  state, started_at, finished_at,
  phase?, completed_units?, total_units?, unit?,
  result reference?, process result?, change references?,
  availability, truncation/retention metadata
}
```

命令字段是 script、cwd；读取字段是 path、范围；edit 字段是 old/new 文本或引用；patch 字段是 patch 数据。字符串正文仍是数据，不是日志。

不新增：`collapsed`、`hidden_line_count`、颜色、图标、TUI label、终端宽度、渲染好的左右 diff 或 `~` 路径美化。

已发布的 ToolDisplay 可以留一个迁移期兼容投影，但新 GUI/TUI 契约只依赖上述结构化事实。不要借此一次性重命名整个 presentation.rs。

### 7.2 开始时发布

在实际工具参数完成基本校验、即将调用执行逻辑时发布 invocation 数据；不再等内部 `.await` 返回后才发路径或命令。

状态至少区分请求已知/执行中/终态。工具处于 Policy 等待时，不能提前标记为 running；审批前信息需要单独可获得的调用数据，不能把 execute 提前到审批前。

新事件与 Runtime ToolStarted 到达顺序可能交错，前端按 ToolRef 合并。未知调用身份不能猜测为“最近的工具”。

### 7.3 运行中只上报真实过程

| 工具 | 适合的数据 |
|---|---|
| read | path/range、必要时已读取字节或行数、最终内容引用 |
| write | 输入版本/大小、writing/committing 阶段、最终写入事实 |
| edit/apply_patch | reading/matching/committing、替换数、change_ref |
| bash | stdout/stderr byte chunks、process 状态、exit code |

小文件开始与完成两个事件即可。不要为动画插 sleep、虚构百分比或把原子写入改成逐行可见写入。

普通进度使用现有 ToolContext.progress；原始 stdout/stderr 使用 Agent 级类型化 byte-stream 数据通道，不把任意 JSON 塞进 progress.message，也不把每个输出块塞回模型 History。

### 7.4 最终状态和补读

```text
tool.read(ToolRef)
  -> invocation + current/final state + result metadata + change refs

tool.output(ToolRef, stream, offset, max_bytes)
  -> data, encoding, base_offset, next_offset,
     observed_end, eof, truncated/expired/availability
```

最终 Runtime ToolResult 和进程退出结果分别记录；命令 exit code 非零不是 RPC 传输失败，工具本身成功执行一次命令也不表示测试通过。

查询与 live 事件必须使用同一数据来源。不能从 mutable 文件系统重读结果来冒充那次工具的历史输出。

### 7.5 数据与日志的边界

受信任本地客户端可以读取有界内容，但这些内容不得进入 tracing/error Debug。Agent 传输时做结构和长度校验；前端负责避免 ANSI/控制字符被当作终端指令执行。

不在新数据接口中把真实代码内容改成 escape_default 排版字符串。确需编码的原始字节用显式 encoding 字段表示。

## 8. Bash：流式输出、退出状态、取消与补读

### 8.1 保留执行入口

```text
模型 ToolCall(bash)
  -> 正常 Policy
  -> 现有 Bash Tool
  -> 受 Agent 所有权保护的命令执行
  -> 正常 ToolResult
```

不为实现 streaming 新增 `process.start`、后台守护进程或另一套模型工具执行路径。

### 8.2 同时读取两个流

在现有 stdout/stderr collector 每次读取到 byte chunk 时：先写入有界保留缓冲，再 best-effort 发事件，同时继续读取，直到 EOF/受控终止。[S9][S18]

两个流独立维护 byte offset，保证各自顺序；不承诺 stdout/stderr 之间有真实全序。服务端观测顺序也不能冒充进程内部写入顺序。

不能等换行才发送，以支持没有 newline 的长输出。不能因 UI 慢或缓冲区满停止读取管道，否则会让子进程因 pipe 堵塞而卡住。

原始流推荐以明确 bytes encoding 传输；采用 Base64 时 offsets 始终指原始字节，客户端增量解码 UTF-8。若提供便利文本视图，需显式说明解码替换情况，不能破坏原始 offset 或切断多字节字符。

### 8.3 有界保留与结果存储

每个流保留一个有界尾部窗口（初始可沿用约 1 MiB 级别），并记录 base_offset/observed_end。完成后把当前保留窗口和结构化退出状态保存为本次 Tool 的辅助数据。

总 Session 活动输出内存、已完成缓存、辅助磁盘总量也必须有上限；不能只有“单块上限”却无限堆积块。

请求的 offset 已被淘汰时，返回 `truncated + base_offset`；辅助文件缺失/写失败时返回 unavailable，不能返回空内容冒充没有输出。

重启只承诺读取已成功保存且仍在保留期内的输出窗口；不恢复原进程，不承诺无限输出完整留存。

最终给模型的 ToolOutput 继续有界，UI 补读保留窗口不受模型摘要格式限制。两个输出预算分开定义。

### 8.4 退出数据

```text
CommandResult {
  status: exited | cancelled | timed_out | spawn_failed | failed,
  exit_code: integer | null,
  signal: optional,
  termination_confirmed: bool,
  stdout/stderr retained ranges,
  output_complete, output_truncated
}
```

exit_code 只有实际取得退出码时才赋值；取消、信号或启动失败不能伪装成 0。

正常结束要求进程 wait 完成、管道 drain 完成或明确因 drain 上限停止。孙进程长期持有管道时不能无限等待；返回 incomplete 并按所有权策略处理受控进程组。

### 8.5 取消范围明确

本蓝图的必需取消入口仍为现有 `turn.cancel`：取消整个当前 AgentLoop及其 Bash。Session close/shutdown 同样取消并 join。已有 Bash input timeout 与 Tool/Turn deadline 继续取更早边界。

单独“停止某条 Bash 但让父 Loop 继续”不是本轮强制接口；以后可以在同一 ToolRef 上增加窄 `tool.cancel`，但不得偷偷把它当成 turn.cancel，必须单独定义模型收到何种失败结果。

取消立即返回的是请求已接受；输出记录先进入 cancelling，真实清理完成后再给出 terminal/termination_confirmed。

工具 future 可能被 Runtime 的外层 deadline/cancel 丢弃，不能只把进程 join 责任放在这个 future 的局部变量中。需要复用 Session 任务所有权原语保留命令 worker 句柄，Drop 请求取消，Session completion/close/shutdown 是 join 屏障。

每个 Bash 至多一个命令 owner worker，内部并行 drain 两个管道；不是三个独立服务。[S13][S18]

Unix 进程组和 Windows 受控子进程机制需分别实现并测试。`kill_on_drop` 只能作为补充，不能单独当作“所有后代已回收”的证明；主动脱离受控执行范围的进程不在无 sandbox 的强保证内。

### 8.6 主要修改位置

`src/tools/bash.rs`、`src/tools/mod.rs`、`src/sessions.rs`、工具数据模块、`src/event.rs` 与 RPC。

不改变 read/write/edit 的语义来迁就 Bash；也不将 stdout 内容写进 Runtime 的泛型 Progress message。

## 9. 文件改动与 Review：两种来源必须分开

### 9.1 workspace.status

```text
workspace.status(session_id)
  -> repo_available, head_oid, branch?, detached,
     staged/unstaged/untracked/conflicted 概要,
     observed_at, complete/warnings
```

这是查询时的工作区状态，可能包含用户预先存在的改动、其他工具/进程的改动，不声明作者。

采用显式查询和短期缓存/dirty 标记即可；Git 刷新不应在主 Turn 保存和完成通知之前成为必须等待的步骤，不增加常驻 watcher。

建议用固定 Git 参数的机器格式：porcelain v2、NUL 分隔；`--no-optional-locks` 避免后台 status 的可选索引写入。不用 shell 拼接，不解析彩色终端文本。[S19]

不存在 Git、不是仓库、没有 HEAD、detached、merge conflict 都是正常可表达状态，不统一报 Internal，也不伪装 clean。

Workspace 位于更大 Git 仓库子目录时，只返回允许范围内路径，不能泄漏父目录文件。

### 9.2 changes.list：显式 scope

```text
changes.list(session_id, scope, cursor?, limit?)
```

| scope | 来源与含义 |
|---|---|
| workspace | 当前 Git index/worktree 观察结果，归因 unknown |
| turn(loop_id) | 指定 Turn 文件工具记录的变更 |
| session | 该 Session 有保留记录的文件工具变更 |

一条记录至少含 change_ref、path、kind、origin、tool_ref（如有）、before/after revision、commit_state、details_available、coverage。

原有未提交改动只能出现在 workspace scope，不能因为现在 Agent 在这个目录工作就标记为 Agent 修改。

Bash、外部编辑器、其他 Agent 实例造成的变化，没有可靠工具级证据时只能作为 observed workspace changes，不能猜 ToolCall 归属。

### 9.3 changes.diff

```text
changes.diff(session_id, change_ref, context_lines?, cursor?, max_bytes?)
  -> origin, base_version, target_version,
     hunks/lines 或规范 patch 数据,
     complete/truncated, binary, stale/availability
```

推荐提供 structured hunks（old/new 范围和 context/add/remove 行），GUI/TUI自行选择显示方式；可附标准 unified patch，但不能只提供带 ANSI 的彩色字符串。

workspace scope 必须说明比较的是 index->worktree、HEAD->index 或 HEAD->worktree。普通 git diff 不包含 untracked 文件全文，不能用它声称覆盖全部变化；未跟踪文件可在上限内按新增文件比较或明确尚无 diff。[S20]

使用 `--no-ext-diff`、`--no-textconv`、`--no-color` 等只读参数，路径按 literal pathspec 处理；不触发任意外部 diff/filter 或运行模型。Git 查询还应禁用可触发外部执行的 fsmonitor 配置，隔离 inherited Git 路径环境，固定参数并限制 stdout/stderr 字节、执行时间和并发。

返回结果绑定所比较版本。文件在 list 与 diff 之间变化时重新读取并标明新版本，或返回 stale 供刷新；不能悄悄把新文件 diff 当作旧 change_ref 的结果。

### 9.4 工具变更记录从真实写入路径产生

read-only wrapper 只能看到工具入参和返回文本，不能凭 `old_text/new_text` 宣称整个文件实际改动。

在 write/edit/apply_patch 真实 mutation 路径中记录：

```text
读取/捕获原版本
    -> 校验并计算候选结果
    -> 原子提交文件
    -> 记录 actual outcome 与受限 before/after 数据
```

edit/apply_patch 尽量复用已经读到的 source 与计算结果，不为展示额外读第二遍。write 替换原文件时按预算捕获旧内容；无权限或超大时正常执行原操作，但 Review 标记资料不完整。

意图与已应用结果分开：`planned` 不是 `applied`；rename 后同步错误等不确定边界记为 unknown，不允许把失败统一解释为“没有改动”。[S14]

before 基于那次操作实际读取的文件，不基于 Git HEAD；因此不会把用户原有改动混进 Agent diff。

### 9.5 并发修改不夸大保证

工具记录描述“本次读到的版本 -> 本次写入的版本”，不是跨进程文件事务。检测到中间版本变化时标记 conflict/unknown，不能声称完整归因。

同文件多次修改聚合为 Turn diff，仅当上一 after_version 与下一 before_version 连续。否则返回多段变更，不自动拼成一个看似完整的 A->Z diff。

当前 Review 只读，不提供 restore/reset/stage/commit。之后要做恢复，必须单独设计版本检查和用户改动保护。

### 9.6 辅助资料存储

可在现有 Session 目录下增加很窄的：

```text
runs/<loop-id>/tools/<safe-tool-key>/
  record.json
  stdout.bin / stderr.bin       有保留数据的 Bash 才创建
  before.bin / after.bin        有可保留文件变更时才创建
```

身份、保留范围和版本写在 record.json。辅助数据与完整 History、summary 派生快照分开；不把大输出/整个 before 文件复制进 history.jsonl。

这些辅助资料保存失败，不改变真实 Tool outcome，不触发整轮自动重试。接口明确 recording/availability 状态；主 History 保存失败仍遵守既有 persistence=failed/Blocked 规则。

旧会话没有 runs 资料时，正常显示 details_unavailable，不能当 Store 损坏。设 per-tool、per-session、全局保留预算；不需要 ArtifactManager、引用计数数据库或全量快照系统。

## 10. 模块改动建议

| 文件/模块 | 修改重点 |
|---|---|
| agent.rs | 统一 Rust facade，read/query/compact/result 方法 |
| sessions.rs | admission、执行历史、Tool 记录所有权与清理屏障 |
| store.rs | 只读 session page、摘要锚点、有限辅助结果读取/保存 |
| history.rs | 有序、安全、字节有界的数据投影，不做 UI 排版 |
| compaction.rs / utility.rs | 完成现有手动实现，预算与恢复逻辑 |
| prompt.rs | 避免二次压缩切片，注入数据型摘要，request-time fitting |
| models/openai.rs | 现有预算/错误/continuation 的最小适配 |
| workspace.rs | 共享文件与路径底层，Git 查询 |
| workspace/query.rs（按需新增） | files/read/search，避免主文件失控 |
| tool_data.rs（按需新增） | 调用与结果 DTO、有界记录，替代 UI 型数据职责 |
| presentation.rs | 迁移兼容投影；逐步停止增加 UI 字段 |
| changes.rs（新增） | change 记录与两个 scope 的查询/diff |
| tools/read/write/edit/apply_patch.rs | 少量真实阶段与 mutation 事实 |
| tools/bash.rs | chunk capture、退出数据、受控取消与结果补读 |
| event.rs、rpc/* | 新数据事件、查询分页、deferred 响应、错误分类 |

新增文件是合理的领域代码容器，不是要求每个文件配一套 Trait/Factory/Manager。只有复用已形成时才抽 helper。

## 11. 开发阶段与依赖

| 阶段 | 交付 | 必须通过的关口 |
|---|---|---|
| P0 基线 | 确认手动压缩草稿状态；修 RPC 半帧取消安全；冻结身份/分页契约 | 分片输入与 waiter 交错不丢请求；新查询不阻塞 cancel |
| P1 会话只读 | session.read、turn.result | 无模型/Workspace 也能看历史；失败保存结果可补读 |
| P2 工具数据 | 开始即发参数；tool.read/output 骨架；旧 Presentation 兼容 | 工具完成前收到调用详情；无 UI 字段依赖；事件丢失可核对 |
| P3 完整压缩 | 手动验收 -> 启动前投影 -> request 自动 -> overflow 一次恢复 | 原始历史不变；大历史能启动；工具不重跑；取消可回收 |
| P4 Workspace | files/read/search/status | 路径安全、ignore 一致、范围/字节有界、大查询不堵 reader |
| P5 Bash | 复用 P2 完成双流、退出状态、取消、保留窗口 | 进程退出前有输出；gap 可补；高输出不堵管道 |
| P6 Review | changes.list/diff，两种 scope 和工具真实 before/after | 用户改动不被归因；真实写入与预览不同；无 Git 也可看工具记录 |
| P7 联调收口 | 同一数据契约的 fake GUI/TUI client 测试与文档 | 不直接读内部 Store、不解析展示字符串、所有权与边界回归 |

P3 与 P4 可分开实施；P5 依赖 P2；P6 依赖 P2 和 P4 的底层。不要让多名 Agent 同时重写 sessions.rs 和 event.rs 的共同协议。

每个阶段先做一个纵向最小闭环（底层->API->RPC->测试），而不是先搭一整套框架，再同时接六项能力。

不规定未经验证的代码减少百分比或完成工期。合理目标是减少重复状态和转换，不是为某个 LOC 数字牺牲正常功能。

## 12. 关键验收清单

### 会话与 RPC

1. Workspace 删除、模型移除、Profile 变化后，session.read 仍能读取支持格式的旧会话。
2. read 不创建目录、不截断 JSONL、不更新 metadata、不启动 Model。
3. 正在 append 的尾部不被误修复；跨页结果限定在已捕获完整前缀。
4. 流式事件丢失且 JSONL append 失败时，turn.result 仍能取得保留中的答案。
5. 半帧请求与 deferred waiter 交错不会丢前缀；大页面受编码后字节上限控制。

### 压缩

6. 手动压缩实际调用 no-tools raw Model；History 字节和公开历史顺序不变。
7. 摘要加载校验锚点；损坏/过期摘要不阻断 session.read。
8. 超过 Runtime 原始 History 上限的历史，在摘要后能构造合规 LoopRequest。
9. 启动前已裁剪历史不会在 PromptProvider 再按旧索引裁一次。
10. 每次请求都算预算；单个巨大 ToolResult、模型切小窗口、Steer 都覆盖。
11. 当前 User/Steer 原文保留，Tool exchange 不拆对；没有额外 User 或工具副作用。
12. ContextOverflow+NotStarted 仅恢复一次；Started/Unknown/第二次拒绝不重发。
13. 手动/自动/恢复均可取消，超时不泄漏 utility worker；摘要无进展明确失败。
14. 压缩 utility usage 与主请求 usage 分开，估计值不冒充精确计量。

### Workspace 与改动

15. files/search ignore 与路径范围一致；symlink、父路径、Git 父仓库边界不能越界。
16. read 返回原始代码数据而非渲染行号；非 UTF-8、超大、并发变化明确表达。
17. 搜索 scan_incomplete 时不谎报全局 total，仍可处理 ping/cancel。
18. workspace status 区分 index/worktree/untracked/conflict；无 Git 不是 clean。
19. 工作区预存用户改动不进入 turn scope；Bash 修改没有可靠证据不自动归因。
20. edit/write/patch preview、applied、unknown 分开；Review 附加资料失败不重做修改。
21. 同文件并发变化或版本链断裂返回 partial/conflict，不能拼造完整 diff。

### Bash 与工具数据

22. Bash 写 stdout 后等待、随后写 stderr：退出前就能看到两种流，不要求有换行。
23. 超过 pipe 容量、高输出、无人消费 Event：仍持续 drain，进程不因 UI 堵塞。
24. 多字节字符跨 chunk、无效 UTF-8、ANSI 和二进制输出不会破坏 RPC 或 offsets。
25. gap/淘汰窗口/辅助文件缺失均明确；重启可补读保留成功的结果，不恢复进程。
26. exit_code 非零、signal、spawn failure、timeout、cancel 不冒充成功 0。
27. 取消后先显示 cancelling，回收确认后终态；受控命令的子进程测试覆盖目标平台。
28. Runtime 外层丢弃 Tool future 后，命令 worker 仍被 owner 取消并 join。
29. read/write 的快速操作不制造假百分比、不改变原子写入；开始就可获得路径和操作数据。
30. 同一份数据同时供无渲染测试客户端与 TUI/GUI 使用，不解析 hidden-lines/label 等字符串。

## 13. 验证与交付要求

保持 Rust 1.85 与 stable、Linux/macOS/Windows 检查；对 Git、Bash、取消和文件路径采用真实本地进程/临时目录回归，而不是只有 DTO 单元测试。

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

Live Provider 测试继续独立、默认不运行；没有执行的验证明确写“未执行”。不把既有源码检查点或未验收测试结果当成该阶段验收。

每次交付记录实际 Agent/Runtime SHA、变更接口、数据兼容范围、关键测试结果与已知限制。只操作明确授权的代码与测试资料，不把私人配置和真实 Session 数据复制进测试/构建环境。

## 14. 最终目标

```text
前端可以不启动执行环境地查看旧会话；
长会话通过派生摘要持续执行，原始历史保持完整；
用户可以浏览/读取/搜索工作区而不启动模型；
工具开始前后都能取得真实参数、状态、过程与结果；
Bash 在运行中输出，取消可确认，丢失的保留输出可补读；
文件 Review 明确区分现有工作区变化和有证据的 Agent 变更；
任何 UI 都自行选择如何呈现这些数据。
```

不需要把 minicore-agent 变成 IDE 或工作流平台。需要的是完成上述数据闭环，并继续让 Runtime 保持普通的执行内核。

## 参考来源

以下源码均按文中基线读取。来源用于确认现状与约束；接口和阶段是本蓝图的设计建议。

- [S1] [Agent dev 基线](https://github.com/zqcli/minicore-agent/commit/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0)
- [S2] [最新压缩计划及实现状态](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/docs/compaction-plan.md)
- [S3] [CompactionState、SummarySnapshot 与 summary_data_message](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/compaction.rs)
- [S4] [当前 ProjectPromptProvider 投影](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/prompt.rs)
- [S5] [Session 启动、任务与压缩所有权](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/sessions.rs)
- [S6] [PresentationTool 和工具详情发布时间](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/presentation.rs)
- [S7] [ReadTool](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/tools/read.rs)
- [S8] [WriteTool](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/tools/write.rs)
- [S9] [Bash collector 与 process owner](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/tools/bash.rs)
- [S10] [RPC server](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/rpc/server.rs)
- [S11] [Runtime AgentLoop 启动与 History 预算检查](https://github.com/zqcli/minicore-runtime/blob/6cd2bdbc634437dea925495c61c7eb0be10ba171/src/agent_loop/mod.rs)
- [S12] [Runtime 默认 Summary 投影角色](https://github.com/zqcli/minicore-runtime/blob/6cd2bdbc634437dea925495c61c7eb0be10ba171/src/prompt.rs)
- [S13] [Runtime Tool 调用与取消边界](https://github.com/zqcli/minicore-runtime/blob/6cd2bdbc634437dea925495c61c7eb0be10ba171/src/agent_loop/runner/tools.rs)
- [S14] [Workspace 实际读写边界](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/workspace.rs)
- [S15] [现有事件 DTO](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/event.rs)
- [S16] [EditTool 真实 read/modify/write 路径](https://github.com/zqcli/minicore-agent/blob/8b8bbcb33dd692f023e89a0eefcbbb6f2c7a87c0/src/tools/edit.rs)
- [S17] [Tokio AsyncBufReadExt：取消安全](https://docs.rs/tokio/latest/tokio/io/trait.AsyncBufReadExt.html)
- [S18] [Tokio process：进程与管道](https://docs.rs/tokio/latest/tokio/process/index.html)
- [S19] [Git status：porcelain、NUL 与 optional locks](https://git-scm.com/docs/git-status)
- [S20] [Git diff：比较端点与 machine-readable 参数](https://git-scm.com/docs/git-diff)
