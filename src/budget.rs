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

/// Encoded JSON size of a result value. Result types are plain data with
/// string keys, so serialization cannot fail.
pub(crate) fn encoded_len<T: Serialize>(value: &T) -> usize {
    serde_json::to_vec(value)
        .expect("query results serialize to JSON")
        .len()
}
