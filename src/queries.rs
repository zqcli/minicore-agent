//! Shared orchestration for the explicit `workspace.status` query.
//!
//! Both the Rust `Agent` API and the RPC dispatcher need the same three steps:
//! register a Session-owned worker, wait for its result, and project a complete
//! observation onto the compatibility presentation cache. The registration is
//! synchronous and stays with the caller, so a `close`/`shutdown` that happens
//! after `prepare_workspace_status` returns can never race ahead of the worker
//! it just registered.
//!
//! The returned future only waits. It does not own the worker: joining that
//! worker remains the Session's authority, and dropping this future must not
//! detach or cancel it.

use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::error::AgentError;
use crate::sessions::Session;
use crate::workspace::status::WorkspaceStatusRequest;

/// A wait-only future for one already-registered query.
///
/// The box keeps the two callers on one concrete return type. This is one small
/// allocation per explicit status query, not a registry or lifecycle owner.
pub(crate) type QueryFuture<T> = BoxFuture<'static, Result<T, AgentError>>;

/// Registers an owned `workspace.status` worker and returns a future that waits
/// for its result and completes the presentation cache.
///
/// The caller must have validated the request and checked capacity first. The
/// future has no side effect until it is polled, and it performs no registration
/// itself.
pub(crate) fn prepare_workspace_status(
    session: Session,
    request: WorkspaceStatusRequest,
    cancellation: CancellationToken,
) -> Result<QueryFuture<crate::WorkspaceStatusResult>, AgentError> {
    let workspace = session.workspace();
    // Registration happens here, synchronously, so a later close observes the
    // worker. The worker captures only the workspace, request, and tokens.
    let query = session.spawn_status_query(workspace, request, cancellation.clone())?;
    Ok(Box::pin(async move {
        let result = query.wait().await?;
        // Only the explicit status entry updates the branch cache; a pure
        // changes query must never refresh it implicitly.
        session.complete_status_query(&result, &cancellation);
        Ok(result)
    }))
}
