use super::*;
use minicore_runtime::history::AssistantHistory;
use minicore_runtime::model::{
    AssistantPart, ModelFinishReason, ProviderReplay, ReasoningPreference, Usage,
};

#[test]
fn history_tokens_leave_out_provider_replay() {
    let replay = ProviderReplay::new("test-v1", serde_json::json!("r".repeat(4_000))).unwrap();
    let item = |provider_replay| {
        HistoryItem::Assistant(AssistantHistory {
            loop_id: LoopId::new().unwrap(),
            request_index: 0,
            model: "main".parse().unwrap(),
            reasoning: ReasoningPreference::Auto,
            content: vec![AssistantPart::Text("answer".repeat(100).into())],
            provider_replay,
            finish_reason: ModelFinishReason::Stop,
            usage: Usage::default(),
        })
    };
    let plain = [item(None)];
    let replayed = [item(Some(replay))];
    let plain_tokens = history_tokens(&plain, estimate_history_bytes(&plain));
    assert_eq!(
        history_tokens(&replayed, estimate_history_bytes(&replayed)),
        plain_tokens
    );
}
