//! Shared orchestration for the explicit read queries.
//!
//! Rust and RPC callers share source selection, deadlines, and cancellation.
//! Status and diff queries register Session-owned workers synchronously, so
//! close/shutdown observes them even if the returned future is never polled.
//!
//! Files/search retain their own blocking-worker join and must not be wrapped
//! in an outer timeout. Status/diff awaiters leave joining to the Session or
//! Store. Callers retain their validation and capacity-admission order.

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

/// An owned query future, separate from the worker ownership described above.
pub(crate) type QueryFuture<T> = BoxFuture<'static, Result<T, AgentError>>;

/// Registers an owned `workspace.status` worker and returns a future that waits
/// for its result and completes the presentation cache.
///
/// The caller must have validated the request and checked capacity first. The
/// returned future waits and updates the presentation cache; registration is
/// already complete when this function returns.
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
        let result = closed_query_result(query.wait().await, &session.query_cancellation())?;
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
                    _ = session_cancellation.cancelled() => Err(AgentError::SessionNotLoaded),
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        Err(AgentError::QueryLimit)
                    }
                    result = query.wait() => {
                        let status = result?;
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => Err(AgentError::QueryLimit),
                            _ = session_cancellation.cancelled() => Err(AgentError::SessionNotLoaded),
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
                    _ = session_cancellation.cancelled() => Err(AgentError::SessionNotLoaded),
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
                _ = session_cancel.cancelled() => Err(AgentError::SessionNotLoaded),
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
                _ = session_cancellation.cancelled() => Err(AgentError::SessionNotLoaded),
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

async fn bounded_read<T>(
    future: impl std::future::Future<Output = Result<T, AgentError>>,
    close: CancellationToken,
    deadline: std::time::Duration,
) -> Result<T, AgentError> {
    tokio::select! {
        biased;
        _ = close.cancelled() => Err(AgentError::SessionNotLoaded),
        result = tokio::time::timeout(deadline, future) => result.unwrap_or(Err(AgentError::QueryLimit)),
    }
}

pub(crate) fn prepare_session_read(
    store: Store,
    loaded: Option<Session>,
    request: crate::ReadSession,
    cancellation: CancellationToken,
) -> QueryFuture<crate::ReadSessionResult> {
    let close = loaded
        .as_ref()
        .map_or_else(CancellationToken::new, Session::query_cancellation);
    Box::pin(bounded_read(
        crate::read::read_session(store, loaded, request, cancellation),
        close,
        crate::read::READ_DEADLINE,
    ))
}

pub(crate) fn prepare_turn_result(
    store: Store,
    loaded: Option<Session>,
    request: crate::TurnResultRequest,
    cancellation: CancellationToken,
) -> QueryFuture<crate::TurnResultPage> {
    let close = loaded
        .as_ref()
        .map_or_else(CancellationToken::new, Session::query_cancellation);
    Box::pin(bounded_read(
        crate::read::turn_result(store, loaded, request, cancellation),
        close,
        crate::read::READ_DEADLINE,
    ))
}

pub(crate) fn prepare_workspace_read(
    session: Session,
    request: crate::WorkspaceReadRequest,
    cancellation: CancellationToken,
) -> QueryFuture<crate::WorkspaceReadResult> {
    let close = session.query_cancellation();
    Box::pin(bounded_read(
        crate::workspace::query::read(session.workspace(), request, close.clone(), cancellation),
        close,
        crate::workspace::query::WORKSPACE_READ_DEADLINE,
    ))
}

// These scans own their blocking-worker join. Await it even on cancellation;
// an outer timeout/select would detach that ownership.
pub(crate) fn prepare_workspace_files(
    session: Session,
    request: crate::WorkspaceFilesRequest,
    cancellation: CancellationToken,
) -> QueryFuture<crate::WorkspaceFilesResult> {
    let close = session.query_cancellation();
    let workspace = session.workspace();
    Box::pin(async move {
        let result =
            crate::workspace::listing::files(workspace, request, close.clone(), cancellation).await;
        closed_query_result(result, &close)
    })
}

pub(crate) fn prepare_workspace_search(
    session: Session,
    request: crate::WorkspaceSearchRequest,
    cancellation: CancellationToken,
) -> QueryFuture<crate::WorkspaceSearchResult> {
    let close = session.query_cancellation();
    let workspace = session.workspace();
    Box::pin(async move {
        let result =
            crate::workspace::search::search(workspace, request, close.clone(), cancellation).await;
        closed_query_result(result, &close)
    })
}

fn closed_query_result<T>(
    result: Result<T, AgentError>,
    close: &CancellationToken,
) -> Result<T, AgentError> {
    if close.is_cancelled() {
        Err(AgentError::SessionNotLoaded)
    } else {
        result
    }
}

pub(crate) fn prepare_tool_read(
    store: Store,
    tool_data: Option<std::sync::Arc<crate::tool_data::ToolData>>,
    request: crate::tool_data::ToolReadRequest,
    cancellation: CancellationToken,
) -> QueryFuture<crate::tool_data::ToolReadResult> {
    Box::pin(crate::read::tool_read(
        store,
        tool_data,
        request,
        cancellation,
    ))
}

pub(crate) fn prepare_tool_output(
    store: Store,
    tool_data: Option<std::sync::Arc<crate::tool_data::ToolData>>,
    request: crate::tool_data::ToolOutputRequest,
    cancellation: CancellationToken,
) -> QueryFuture<crate::tool_data::ToolOutputPage> {
    Box::pin(crate::read::tool_output(
        store,
        tool_data,
        request,
        cancellation,
    ))
}
