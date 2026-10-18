use super::*;

/// Reasoning summary parts are separated by a single newline, never by
/// text/capitalization heuristics. Each provider `...summary_text.delta`
/// carries (when present) the item identity (`item_id`/`output_index`) and
/// the per-item `summary_index`; a boundary is any change in that identity or
/// an explicit reasoning item `output_item.added`/`done`.
///
/// Missing identity is NOT a boundary: a fragment with no fields continues the
/// current part (the known identity is retained across present->absent->
/// present gaps), so a provider that drops metadata mid-part and restores it
/// later still concatenates. Only a genuine KNOWN-field mismatch or a
/// lifecycle transition separates parts.
#[derive(Default)]
pub(super) struct ReasoningParts {
    /// Identity of the part the previous delta belonged to (fully-known so
    /// far: omitted fields inherit this value).
    last_key: Option<ReasoningPartKey>,
    /// Lifecycle bumped by reasoning `output_item.added`/`done` and
    /// `reasoning_summary_part.added`/`done`, so explicit boundaries separate
    /// parts even when deltas carry no index.
    lifecycle: u64,
    /// The `lifecycle` observed on the previous delta.
    last_lifecycle: u64,
    /// True once any nonempty reasoning text was handed to the stream.
    pub(super) emitted: bool,
    /// Whether the last emitted nonempty text normalizes to a trailing newline.
    last_ends_newline: bool,
    /// A boundary observed on an EMPTY delta (e.g. a new `summary_index` with
    /// no text): the next nonempty delta emits the separator without the
    /// empty delta itself producing a blank line.
    pending_boundary: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReasoningPartKey {
    output_index: Option<u32>,
    item_id: Option<String>,
    summary_index: Option<u32>,
}

#[derive(Deserialize)]
pub(super) struct ReasoningSummaryDeltaEvent {
    pub(super) delta: String,
    pub(super) output_index: Option<Value>,
    pub(super) item_id: Option<String>,
    pub(super) summary_index: Option<Value>,
}

impl ReasoningParts {
    pub(super) fn mark_item_boundary(&mut self) {
        self.lifecycle = self.lifecycle.wrapping_add(1);
    }
}

pub(super) fn queue_reasoning_delta(state: &mut StreamState, event: ReasoningSummaryDeltaEvent) {
    // The delta's explicit fields override the previous known part; omitted
    // fields inherit it, so a metadata gap never invents a boundary.
    let merged = merge_reasoning_key(state.reasoning_parts.last_key.clone(), &event);
    let delta = event.delta;
    let identity_changed = merged != state.reasoning_parts.last_key;
    let lifecycle_changed = state.reasoning_parts.last_lifecycle != state.reasoning_parts.lifecycle;

    if delta.is_empty() {
        // An empty delta can still announce the next part (a new summary index
        // before any text): remember the transition so the following
        // identity-less nonempty delta starts a new part, with no blank line.
        if identity_changed || lifecycle_changed {
            state.reasoning_parts.last_key = merged;
            state.reasoning_parts.last_lifecycle = state.reasoning_parts.lifecycle;
            state.reasoning_parts.pending_boundary = true;
        }
        return;
    }

    let boundary = identity_changed || lifecycle_changed || state.reasoning_parts.pending_boundary;
    // A single newline separates two actual nonempty parts; a raw provider
    // break at either side prevents doubling it.
    let needs_separator = boundary
        && state.reasoning_parts.emitted
        && !state.reasoning_parts.last_ends_newline
        && !delta.starts_with(['\n', '\r']);
    if needs_separator {
        queue_text(state, "\n".to_owned(), true);
    }
    let last_ends_newline = delta.ends_with(['\n', '\r']);
    queue_text(state, delta, true);
    if boundary {
        state.reasoning_parts.last_key = merged;
        state.reasoning_parts.last_lifecycle = state.reasoning_parts.lifecycle;
    }
    state.reasoning_parts.pending_boundary = false;
    state.reasoning_parts.emitted = true;
    state.reasoning_parts.last_ends_newline = last_ends_newline;
}

/// Overlays a delta's EXPLICIT identity fields over the last known part.
/// Missing identity (no fields at all) returns the previous key unchanged:
/// NOT a boundary. A partial delta retains the known fields and only overrides
/// the fields it actually carries, so a dropped-then-restored index on the
/// same part stays glued.
fn merge_reasoning_key(
    previous: Option<ReasoningPartKey>,
    event: &ReasoningSummaryDeltaEvent,
) -> Option<ReasoningPartKey> {
    let has_identity =
        event.output_index.is_some() || event.item_id.is_some() || event.summary_index.is_some();
    if !has_identity {
        return previous;
    }
    let previous = previous.unwrap_or(ReasoningPartKey {
        output_index: None,
        item_id: None,
        summary_index: None,
    });
    Some(ReasoningPartKey {
        output_index: event
            .output_index
            .as_ref()
            .and_then(value_to_u32)
            .or(previous.output_index),
        item_id: event.item_id.clone().or(previous.item_id),
        summary_index: event
            .summary_index
            .as_ref()
            .and_then(value_to_u32)
            .or(previous.summary_index),
    })
}

fn value_to_u32(value: &Value) -> Option<u32> {
    value.as_u64().and_then(|number| u32::try_from(number).ok())
}
