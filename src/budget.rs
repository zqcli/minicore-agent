//! Shared limits for bounded queries: cancellation/deadline checks and the
//! encoded size of JSON results.

use std::time::Instant;

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;

/// Fails with `QueryLimit` once the query is cancelled or past its deadline.
pub(crate) fn check_budget(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), AgentError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(AgentError::QueryLimit)
    } else {
        Ok(())
    }
}

/// Returns a query result only if its JSON representation fits the page budget.
pub(crate) fn encoded_result<T: Serialize>(result: T, max_bytes: usize) -> Result<T, AgentError> {
    if encoded_len(&result) > max_bytes {
        Err(AgentError::InvalidArguments)
    } else {
        Ok(result)
    }
}

/// Encoded JSON size of a result value. Result types are plain data with
/// string keys, so serialization cannot fail.
pub(crate) fn encoded_len<T: Serialize + ?Sized>(value: &T) -> usize {
    json_len(value).expect("query results serialize to JSON")
}

pub(crate) fn json_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, serde_json::Error> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("JSON size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub(crate) fn encode_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = Vec::with_capacity(json_len(value)?);
    serde_json::to_writer(&mut bytes, value)?;
    Ok(bytes)
}

pub(crate) fn json_escaped_len(text: &str) -> usize {
    text.chars().map(escaped_char_len).sum()
}

fn escaped_char_len(character: char) -> usize {
    match character {
        '"' | '\\' | '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
        character if (character as u32) < 0x20 => 6,
        character => character.len_utf8(),
    }
}

/// Returns the longest UTF-8 prefix fitting a JSON string budget, including quotes.
pub(crate) fn json_string_prefix(text: &str, max_encoded_bytes: usize) -> &str {
    let mut remaining = max_encoded_bytes.saturating_sub(2);
    let mut end = 0;
    for (index, character) in text.char_indices() {
        let cost = escaped_char_len(character);
        if cost > remaining {
            break;
        }
        remaining -= cost;
        end = index + character.len_utf8();
    }
    &text[..end]
}
