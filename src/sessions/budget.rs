use super::*;

/// Estimates history tokens at four bytes each, without provider replay.
fn history_tokens(history: &[HistoryItem], bytes: usize) -> u64 {
    // Replay is opaque provider state that the provider mostly drops from
    // context, so counting it would overstate the history.
    let replay: usize = history
        .iter()
        .filter_map(|item| match item {
            HistoryItem::Assistant(assistant) => assistant.provider_replay.as_ref(),
            _ => None,
        })
        .map(|replay| replay.byte_len())
        .sum();
    u64::try_from(bytes - replay)
        .unwrap_or(u64::MAX)
        .div_ceil(4)
}

impl Session {
    pub(crate) fn context(&self) -> SessionContext {
        let inner = self.lock_inner();
        let (covered_loop_count, covered_item_count) =
            self.shared.compaction.coverage().unwrap_or((0, 0));
        let retained_item_count = inner.history.len().saturating_sub(covered_item_count);
        let projection = self.shared.compaction.project(&inner.history);
        let projected_history = projection
            .as_ref()
            .map_or(&inner.history[..], |projection| projection.suffix);
        let estimated_history_items = projected_history.len();
        let estimated_history_bytes = estimate_history_bytes(projected_history);
        let automatic = self.shared.compaction.automatic_view();
        let estimated_request_context_tokens = self.shared.compaction.latest_request_tokens();
        let (input_budget_tokens, trigger_tokens, target_tokens) =
            match (&inner.auto, inner.config.descriptor().context_window) {
                (Some(auto), window) => {
                    let budget = auto.policy.budget(window);
                    (
                        Some(window),
                        Some(budget.trigger_tokens),
                        Some(budget.target_tokens),
                    )
                }
                _ => (None, None, None),
            };
        SessionContext {
            session_id: inner.record.session_id,
            current_operation: inner.compaction_progress.clone(),
            coverage: SummaryCoverage {
                covered_loop_count,
                covered_item_count,
                retained_item_count,
            },
            last_result: inner.last_compaction_result.clone(),
            last_prepare_failure: self.shared.compaction.prepare_failure(),
            budget: ContextBudget {
                estimated_history_items,
                estimated_history_tokens: history_tokens(
                    projected_history,
                    estimated_history_bytes,
                ),
                estimated_history_bytes,
                estimated_request_context_tokens,
                input_budget_tokens,
                trigger_tokens,
                target_tokens,
            },
            automatic,
            recovery: self.shared.compaction.recovery_observation(),
        }
    }
}

#[cfg(test)]
mod tests;
