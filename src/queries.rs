//! Shared orchestration for the explicit read queries.
//!
//! The Rust `Agent` API and the RPC dispatcher both need the same steps for
//! `workspace.status`, `changes.list`, and `changes.diff`: pick the data source,
//! register any Session-owned worker synchronously, then wait and post-process
//! under one absolute deadline. Registration stays with the caller, so a
//! `close`/`shutdown` that happens after `prepare_*` returns can never race
//! ahead of a worker it just registered.
//!
//! The returned futures only wait. They do not own a worker: joining remains the
//! Session's (or the Store's) authority, and dropping this future must not
//! detach or cancel it. Each caller keeps its own validation, capacity, and
//! error-mapping order.

use std::time::Instant;

use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::changes::{
    ChangeScope, ChangesListRequest, ChangesListResult, list_tool_changes, list_workspace_changes,
};
use crate::diff::{ChangesDiffRequest, DiffResult, changes_diff};
use crate::error::AgentError;
use crate::sessions::Session;
use crate::store::Store;
use crate::workspace::status::WorkspaceStatusRequest;

/// A wait-only future for one already-registered query.
///
/// The box keeps the callers on one concrete return type. This is one small
/// allocation per explicit query, not a registry or lifecycle owner.
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

/// Selects the `changes.list` source and returns a future that waits for the
/// result.
///
/// `Workspace` scope requires a loaded Session and registers the same owned
/// status worker as an explicit status query, using the passed absolute
/// `deadline`. `Session`/`Turn` scope allows an unloaded Session and is a pure
/// cold read of retained tool data; it must never load a Workspace or model.
///
/// The caller must have validated the request and checked capacity first.
pub(crate) fn prepare_changes_list(
    store: Store,
    loaded: Option<Session>,
    request: ChangesListRequest,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<QueryFuture<ChangesListResult>, AgentError> {
    match &request.scope {
        ChangeScope::Workspace => {
            let session = loaded.ok_or(AgentError::SessionNotLoaded)?;
            let status_request = WorkspaceStatusRequest {
                session_id: request.session_id,
                max_bytes: Some(request.max_bytes()),
            };
            let session_cancellation = session.query_cancellation();
            // Registration is synchronous; the Session owns the worker even if
            // this future is never polled. The future observes only the
            // cancellation token, so it does not retain the Session.
            let query = session.spawn_status_query_with_deadline(
                session.workspace(),
                status_request,
                cancellation.clone(),
                deadline,
            )?;
            drop(session);
            Ok(Box::pin(async move {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
                    _ = session_cancellation.cancelled() => Err(AgentError::QueryLimit),
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        Err(AgentError::QueryLimit)
                    }
                    result = query.wait() => {
                        let status = result?;
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
                            _ = session_cancellation.cancelled() => Err(AgentError::QueryLimit),
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                                Err(AgentError::QueryLimit)
                            }
                            result = list_workspace_changes(
                                request,
                                status,
                                cancellation.clone(),
                                deadline,
                            ) => result,
                        }
                    }
                }
            }))
        }
        ChangeScope::Session | ChangeScope::Turn { .. } => {
            let (tool_data, session_cancellation) = match &loaded {
                Some(session) => (Some(session.tool_data()), session.query_cancellation()),
                None => (None, CancellationToken::new()),
            };
            Ok(Box::pin(async move {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
                    _ = session_cancellation.cancelled() => Err(AgentError::QueryLimit),
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        Err(AgentError::QueryLimit)
                    }
                    result = list_tool_changes(
                        store,
                        tool_data,
                        request,
                        cancellation.clone(),
                        deadline,
                    ) => result,
                }
            }))
        }
    }
}

/// Selects the `changes.diff` source and returns a future that waits for the
/// result.
///
/// A `workspace:` reference requires a loaded Session and registers the owned
/// diff-source worker synchronously, using the passed absolute `deadline`; the
/// Store still owns the CPU comparison the future subsequently starts. A tool
/// reference allows an unloaded Session, a removed Workspace, or a missing
/// model and is a pure cold read. The caller must have validated the request
/// and checked capacity first.
pub(crate) fn prepare_changes_diff(
    store: Store,
    loaded: Option<Session>,
    request: ChangesDiffRequest,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<QueryFuture<DiffResult>, AgentError> {
    if request.change_ref.starts_with("workspace:") {
        let session = loaded.ok_or(AgentError::SessionNotLoaded)?;
        let session_cancel = session.query_cancellation();
        // Registration is synchronous; the Session owns the source worker even
        // if this future is never polled. The future observes only the
        // cancellation token, so it does not retain the Session.
        let query =
            session.spawn_workspace_diff(request.clone(), cancellation.clone(), deadline)?;
        drop(session);
        Ok(Box::pin(async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
                _ = session_cancel.cancelled() => Err(AgentError::QueryLimit),
                _ = tokio::time::sleep_until(deadline.into()) => Err(AgentError::QueryLimit),
                result = async {
                    let sources = query.wait().await?;
                    crate::diff::workspace_diff(store, request, sources, session_cancel.clone(), deadline).await
                } => result,
            }
        }))
    } else {
        let (tool_data, session_cancellation) = match &loaded {
            Some(session) => (Some(session.tool_data()), session.query_cancellation()),
            None => (None, CancellationToken::new()),
        };
        Ok(Box::pin(async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
                _ = session_cancellation.cancelled() => Err(AgentError::QueryLimit),
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    Err(AgentError::QueryLimit)
                }
                result = changes_diff(
                    store,
                    tool_data,
                    request,
                    session_cancellation.clone(),
                    deadline,
                ) => result,
            }
        }))
    }
}
