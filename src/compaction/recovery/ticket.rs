use super::*;
use crate::compaction::projection::AutoContextBinding;
use minicore_runtime::model::{ModelLimits, ReasoningPreference};
use minicore_runtime::tools::ToolSpec;
use sha2::{Digest, Sha256};

const MAX_RECOVERY_SOURCE_BYTES: usize = 512 * 1024;

/// Immutable ticket metadata prepared during prompt preparation and strictly
/// bound to one logical model request. `content_hash` is fixed when the prompt
/// is prepared; `ticket_hash` is bound once the Runtime-built request (with
/// its actual `ModelLimits`) reaches `Model::start`.
#[derive(Clone)]
pub(crate) struct ActiveRecoveryTicket {
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
    pub(crate) content_hash: [u8; 32],
    pub(crate) ticket_hash: Option<[u8; 32]>,
    pub(crate) original_body_bytes: usize,
    pub(crate) system: BoundedText,
    pub(crate) summary: Option<BoundedText>,
    pub(crate) base: Vec<HistoryItem>,
    pub(crate) appended: Vec<HistoryItem>,
    pub(crate) tools: Vec<ToolSpec>,
    pub(crate) limits: ModelLimits,
    pub(crate) reasoning: ReasoningPreference,
    /// Utility binding captured when the Runtime-built request was bound. The
    /// recovery uses this binding, not the current settings, so a later
    /// settings update cannot re-pair the ticket with a newer utility model.
    pub(crate) auto_binding: Option<AutoContextBinding>,
}

/// Bounds the retained recovery source before any `HistoryItem` is cloned. The
/// count is a capped serde write of exactly the fields the ticket retains
/// (system, durable summary, history items, tool schemas), so structural
/// framing, identifiers, model/usage/reasoning fields, and empty items all
/// count. Serialization stops as soon as the cap is crossed and never builds
/// an intermediate buffer.
pub(crate) fn recovery_source_is_safe(
    system: &BoundedText,
    summary: Option<&BoundedText>,
    base: &[HistoryItem],
    appended: &[HistoryItem],
    tools: &[ToolSpec],
) -> bool {
    let mut writer = super::super::utility::CappedWriter {
        bytes: 0,
        cap: MAX_RECOVERY_SOURCE_BYTES,
    };
    serde_json::to_writer(&mut writer, system).is_ok()
        && serde_json::to_writer(&mut writer, &summary).is_ok()
        && serde_json::to_writer(&mut writer, base).is_ok()
        && serde_json::to_writer(&mut writer, appended).is_ok()
        && serde_json::to_writer(&mut writer, tools).is_ok()
}

pub(crate) struct TicketFraming<'a> {
    pub(crate) loop_id: LoopId,
    pub(crate) request_index: u32,
    pub(crate) messages: &'a [ModelMessage],
    pub(crate) tools: &'a [ToolSpec],
    pub(crate) model_descriptor: &'a ModelDescriptor,
    pub(crate) reasoning: ReasoningPreference,
    pub(crate) config_generation: u64,
    pub(crate) summary_generation: u64,
}

/// Preparation binds content before Runtime supplies the actual model limits.
pub(crate) fn compute_content_hash(framing: &TicketFraming<'_>) -> Result<[u8; 32], ModelError> {
    hash_ticket_inputs(framing, None)
}

pub(crate) fn compute_ticket_hash(
    framing: &TicketFraming<'_>,
    limits: &ModelLimits,
) -> Result<[u8; 32], ModelError> {
    hash_ticket_inputs(framing, Some(limits))
}

fn hash_ticket_inputs(
    framing: &TicketFraming<'_>,
    limits: Option<&ModelLimits>,
) -> Result<[u8; 32], ModelError> {
    let TicketFraming {
        loop_id,
        request_index,
        messages,
        tools,
        model_descriptor,
        reasoning,
        config_generation,
        summary_generation,
    } = framing;
    let mut hasher = Sha256::new();
    hasher.update(loop_id.to_string().as_bytes());
    hasher.update(request_index.to_le_bytes());
    let messages_json = serde_json::to_vec(messages)
        .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&messages_json);
    let tools_json =
        serde_json::to_vec(tools).map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&tools_json);
    if let Some(limits) = limits {
        let limits_json = serde_json::to_vec(limits)
            .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
        hasher.update(&limits_json);
    }
    hasher.update(model_descriptor.model_ref.as_str().as_bytes());
    hasher.update(model_descriptor.context_window.to_le_bytes());
    hasher.update([model_descriptor.supports_tools as u8]);
    let reasoning_support = serde_json::to_vec(&model_descriptor.supported_reasoning)
        .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&reasoning_support);
    let reasoning_json = serde_json::to_vec(&reasoning)
        .map_err(|_| local_model_error(ModelErrorKind::InvalidRequest))?;
    hasher.update(&reasoning_json);
    hasher.update(config_generation.to_le_bytes());
    hasher.update(summary_generation.to_le_bytes());
    Ok(hasher.finalize().into())
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TicketClaimError {
    AlreadyConsumed,
    NoTicket,
    IndexMismatch,
    StaleTicket,
}
