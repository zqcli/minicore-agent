# Configuration

The binary loads one TOML file with:

```bash
minicore-agent --config ./example.agent.toml --stdio
```

`AgentConfig` rejects unknown fields. The complete runnable shape is in
[`example.agent.toml`](../example.agent.toml).

## Agent Fields

- `data_dir` is required and must be non-empty. A relative path is interpreted
  from the process working directory; it is not rebased to the config file.
- `event_capacity` defaults to `256` and is bounded to `1..=4096`.
- `default_profile` is required and must name an entry in `profiles`.
- `profiles` and `models` are required non-empty tables.
- `[loop]` is optional. Its fields are Runtime option overrides:
  `event_capacity`, `max_pending_steers`, `prompt_timeout_seconds`,
  `model_timeout_seconds`, `policy_timeout_seconds`, `tool_timeout_seconds`,
  `model_retry_attempts`, and `model_retry_base_delay_millis`.
- `[compaction]` is optional. It defaults to enabled with an `80` percent
  trigger and `50` percent target. The valid relationship is
  `0 < target_percent < trigger_percent <= 100`.

## Profiles

A profile requires `model` and `system_prompt`; `reasoning`, `tools`,
`max_tool_rounds`, and `approval` are optional and have defaults described below.

- `system_prompt` can be an inline non-empty string or
  `{ file = "prompts/coding.md" }`.
- `tools` defaults to an empty list and otherwise accepts the five executable
  names: `read`, `write`, `edit`, `apply_patch`, and `bash`. Names must be
  unique. `subagent` is not supported in current profiles.
- `max_tool_rounds` defaults to `32` and accepts `1..=1024`.
- `approval` defaults to `ask`, or may be `auto`/`read_only`. `auto` executes
  enabled tools without an approval prompt; use it only in a trusted
  environment.
- `reasoning` defaults to `auto` and is checked against the selected model's
  explicit `supported_reasoning` set. Supported values currently include
  `auto`, `disabled`, `low`, `medium`, `high`, `xhigh`, `max`, and `ultra` when
  the model configuration allows them.

## Models And Secrets

Each `[models.<id>]` table requires `provider = "open_ai_responses"`. Its
provider-specific fields are `model`, `base_url`, `api_key_env`,
`physical_context_window`, `output_budget_tokens`, `safety_margin_tokens`,
`supported_reasoning`, `supports_tools`, and optional
`request_timeout_seconds`.

`api_key_env` is only the name of an environment variable. The secret value is
read when the Agent opens the model catalog and must be non-empty; it should
never appear in TOML, logs, errors, fixtures, or documentation. Model IDs,
provider URLs, and credential names are configuration inputs, not evidence that
a request was sent.

## Paths And Prompt Files

`AgentConfig::load` and `Agent::open_file` make the supplied config path
absolute lexically without canonicalizing it. A relative profile prompt file is
resolved against that path's parent, including the supplied alias directory of
a config symlink. Absolute prompt paths are accepted; `~` and environment
expansion are not performed.

A prompt file must resolve to a regular UTF-8 file, be at most 128 KiB, contain
no disallowed control characters, and is normalized from CRLF to LF. Symlinks
to regular files are allowed. The file is read at config load/reload time, and
each newly created Session stores its prompt snapshot in `session.json`; turns
and Session reopen do not reread it. `AgentConfig::from_toml` has no config
base, so relative file references require `AgentConfig::load` or `open_file`.

The workspace's optional `<workspace>/AGENTS.md` is separate: prompt preparation
reads it at request time and merges it with the Session's stored profile prompt.
The read is bounded to a 64 KiB prefix plus only the lookahead needed to keep a
boundary-safe UTF-8 code point or CRLF pair. Longer content is marked
`[truncated]`; invalid UTF-8 or disallowed control characters fail prompt
preparation rather than becoming an empty instruction.

## Reload And Update

`agent.reload` is available for the binary's `open_file` Agent. It rebuilds the
model/profile catalog and future-turn execution snapshots before swapping them.
An active Loop keeps the complete configuration snapshot captured when that Loop
started: reload does not change any request in that Loop. After it finishes,
new Loops use the reloaded future-turn snapshot; newly created Sessions use the
reloaded profile/default catalog. Existing Session records, history, and stored
prompt snapshots are not rewritten. `data_dir` and the Agent-level
`event_capacity` require restart. An embedded `Agent::open` has no reload source
and returns `reload_unavailable`. The Bash credential scrub set is cumulative:
credential environment names from both the previous and reloaded model catalogs
remain removed from future Bash children.

A Session copies its selected model/reasoning and the Profile's `tools`,
`max_tool_rounds`, `approval`, and system-prompt text into its persistent
Session record at creation. Reload does not retrofit those Profile defaults into
an existing Session; its future snapshot is rebuilt from the Session record plus
the reloaded model catalog and global policies.

`session.update` changes only a loaded Session's model and/or reasoning settings.
The persistent Session record is written first. If a Loop is active, its new
execution configuration is accepted at the next request boundary, while the
LoopOptions captured at Loop start remain unchanged; the new LoopOptions apply
to the next Loop. With no active Loop, both the execution configuration and
LoopOptions are ready for the next submission. Profile selection, tools,
`max_tool_rounds`, approval, and the Session's stored system-prompt snapshot are
not rewritten by this method.
