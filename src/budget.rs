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
    serde_json::to_vec(value)
        .expect("query results serialize to JSON")
        .len()
}
