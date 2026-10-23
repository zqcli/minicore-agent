//! Bounded, read-only presentation data for local frontends (Rail UI parity
//! spec §12).
//!
//! The Agent owns content facts (tool command/path/input bodies, results,
//! user acceptance times, git branch, honest unknown context/cost); the TUI
//! owns layout and colors. Everything here is best-effort and display-only:
//! a presentation failure must never alter tool execution, model requests,
//! cancellation, deadlines, or the persisted outcome, and no lock is held
//! across an await.
//!
//! Command text and input bodies may contain secrets the user typed
//! themselves; they are intentionally allowed to reach the local TUI
//! (documented permission), but they must never appear in logs, error
//! payloads, or Debug dumps. `ToolDisplay` therefore carries a redacted
//! `Debug` (identity + lengths + counts only).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::Serialize;

use minicore_runtime::history::{HistoryItem, UserMessageKind};
use minicore_runtime::model::{Model, ModelCallContext, ModelDescriptor, ModelRequest, Usage};
use minicore_runtime::prompt::{PromptFuture, PromptProvider, PromptRequest};
use minicore_runtime::tools::{Tool, ToolContext, ToolExecutionOutcome, ToolInvocation, ToolSpec};
use minicore_runtime::{LoopId, ToolCallId};

use crate::event::{AgentEvent, AgentEventSink, EventMeta};
use crate::ids::SessionId;
use crate::read::{InterruptedRequest, PartialStopReason};
use crate::sessions::TurnRef;
use crate::tool_data::ToolRef;
use crate::tools::observe::{RequestKey, ToolObserver};

mod preview;
pub use preview::ToolArgumentsPreviewState;

mod display;
mod state;
mod wrappers;

pub(crate) use crate::tools::observe::{PresentationPolicy, ToolImpl};
pub use display::{AssistantDisplayPart, ToolDisplay};
pub(crate) use display::{
    MAX_DETAIL_BYTES, MAX_RESULT_DISPLAY_BYTES, assistant_display_parts, bounded_result_content,
    build_tool_card_display, build_tool_display, count_lines, result_line_count, tool_card_counts,
};
#[cfg(test)]
use display::{MAX_EXPANDED_INPUT_BYTES, edit_display_body, path_arg};
use display::{
    apply_result_to_display, bounded_display_copy, format_read_range, safe_tool_error_text,
    single_line,
};
pub use state::PresentationView;
#[cfg(test)]
use state::reported_context;
#[cfg(test)]
pub(crate) use state::{ContextKind, ContextUsageView};
pub(crate) use state::{Presentation, context_tokens};
pub(crate) use wrappers::{PresentationModel, PresentationTool, SteerReceiptPrompt};
#[cfg(test)]
mod tests;
