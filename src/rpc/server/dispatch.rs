use super::*;

impl RpcServer {
    pub(super) fn dispatch(&mut self, request: RpcRequest) -> Dispatch {
        match self.dispatch_inner(request) {
            Ok(dispatch) => dispatch,
            Err(response) => Dispatch::Response(response),
        }
    }

    fn dispatch_inner(&mut self, request: RpcRequest) -> Result<Dispatch, RpcResponse> {
        let RpcRequest { id, method, params } = request;
        tracing::debug!(method = canonical_method(&method), "rpc dispatch");
        match method.as_str() {
            name if matches!(name, "session.read" | "turn.result")
                || name.starts_with("workspace.")
                || name.starts_with("changes.")
                || name.starts_with("tool.") =>
            {
                self.dispatch_query(id, name, params)
            }
            name if name.starts_with("agent.")
                || name.starts_with("profile.")
                || name.starts_with("model.") =>
            {
                self.dispatch_agent(id, name, params)
            }
            name if name.starts_with("session.") => self.dispatch_session(id, name, params),
            name if name.starts_with("turn.") || name.starts_with("interaction.") => {
                self.dispatch_turn(id, name, params)
            }
            _ => Err(method_not_found(id)),
        }
    }

    fn dispatch_agent(
        &mut self,
        id: RpcId,
        method: &str,
        params: Option<Value>,
    ) -> Result<Dispatch, RpcResponse> {
        Ok(match method {
            "agent.ping" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                let result = self.agent().ping();
                Dispatch::Response(success(&id, result))
            }
            "agent.reload" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                self.admit_operation(&id, Some(Reservation::Reload))?;
                let future = self
                    .agent()
                    .prepare_reload()
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(Some(Reservation::Reload), async move {
                    let result = future.await;
                    Box::new(move |agent: &mut Agent| {
                        agent_result(
                            &id,
                            result.and_then(|(config, models)| {
                                agent.apply_reload_settings(config, models)
                            }),
                        )
                        .into()
                    }) as ApplyOperation
                })
            }
            "agent.shutdown" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                Dispatch::Shutdown(id)
            }
            "profile.list" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                Dispatch::Response(success(
                    &id,
                    ProfilesResult {
                        profiles: self.agent().list_profiles(),
                    },
                ))
            }
            "model.list" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                Dispatch::Response(success(
                    &id,
                    ModelsResult {
                        models: self.agent().list_models(),
                    },
                ))
            }
            _ => return Err(method_not_found(id)),
        })
    }

    fn dispatch_session(
        &mut self,
        id: RpcId,
        method: &str,
        params: Option<Value>,
    ) -> Result<Dispatch, RpcResponse> {
        Ok(match method {
            "session.list" => {
                let _: EmptyParams = params_or_error(&id, params)?;
                self.admit_operation(&id, None)?;
                let store = self.agent().store_handle();
                self.defer_operation(None, async move {
                    let result = store.list_sessions().await.map_err(AgentError::from);
                    Box::new(move |agent: &mut Agent| {
                        agent_result(
                            &id,
                            result.map(|records| SessionsResult {
                                sessions: agent.session_infos(records),
                            }),
                        )
                        .into()
                    }) as ApplyOperation
                })
            }
            "session.create" => {
                let request: CreateSession = params_or_error(&id, params)?;
                self.admit_operation(&id, None)?;
                let session_id = crate::ids::SessionId::new()
                    .map_err(|_| agent_error(id.clone(), &AgentError::Internal))?;
                let future = self
                    .agent()
                    .prepare_create(request, session_id)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = future.await;
                    Box::new(move |agent: &mut Agent| {
                        agent_result(
                            &id,
                            result.map(|(stored, workspace, setup)| SessionResult {
                                session: agent.install_session(stored, workspace, setup),
                            }),
                        )
                        .into()
                    }) as ApplyOperation
                })
            }
            "session.open" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                if let Some(session) = self.agent().loaded_session(session_id) {
                    Dispatch::Response(success(
                        &id,
                        SessionResult {
                            session: session.info(true),
                        },
                    ))
                } else {
                    let store = self.agent().store_handle();
                    self.defer_operation(Some(Reservation::Session(session_id)), async move {
                        let result = Agent::prepare_open(store, session_id).await;
                        Box::new(move |agent: &mut Agent| {
                            agent_result(
                                &id,
                                result
                                    .and_then(|prepared| agent.install_open(prepared))
                                    .map(|session| SessionResult { session }),
                            )
                            .into()
                        }) as ApplyOperation
                    })
                }
            }
            "session.close" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let session = self
                    .agent()
                    .loaded_session(session_id)
                    .ok_or_else(|| agent_error(id.clone(), &AgentError::SessionNotLoaded))?;
                session.begin_shutdown();
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = session.join_shutdown().await;
                    Box::new(move |agent: &mut Agent| {
                        agent.finish_close(session_id);
                        agent_result(&id, result.map(|()| OkResult::TRUE)).into()
                    }) as ApplyOperation
                })
            }
            "session.delete" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                if self.agent().loaded_session(session_id).is_some() {
                    return Err(agent_error(id, &AgentError::SessionAlreadyLoaded));
                }
                let store = self.agent().store_handle();
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = store
                        .delete_session(session_id)
                        .await
                        .map_err(AgentError::from);
                    Box::new(move |_: &mut Agent| {
                        agent_result(&id, result.map(|()| OkResult::TRUE)).into()
                    }) as ApplyOperation
                })
            }
            "session.state" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .session_state(params.session_id)
                    .map(|state| SessionStateView::from(&state));
                Dispatch::Response(agent_result(&id, result))
            }
            "session.context" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self.agent().session_context(params.session_id);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.compact" => {
                let request: CompactSession = params_or_error(&id, params)?;
                let reservation = Some(Reservation::Session(request.session_id));
                self.admit_operation(&id, reservation)?;
                let future = self
                    .agent()
                    .prepare_compact(request)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(reservation, async move {
                    let result = future.await;
                    Box::new(move |_: &mut Agent| match result {
                        Ok(receiver) => OperationReply::Waiter(Box::pin(async move {
                            agent_result(&id, await_compaction_completion(receiver).await)
                        })),
                        Err(error) => agent_error(id, &error).into(),
                    }) as ApplyOperation
                })
            }
            "session.compact.cancel" => {
                let params: CompactSession = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .cancel_compaction(params)
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.update" => {
                let request: UpdateSession = params_or_error(&id, params)?;
                if !request.has_field() {
                    return Err(invalid_params(Some(id)));
                }
                self.admit_operation(&id, Some(Reservation::Session(request.session_id)))?;
                let session_id = request.session_id;
                let future = self
                    .agent()
                    .prepare_update(request)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    future.await.map(SessionUpdateResult::from)
                })
            }
            "session.rename" => {
                let request: RenameSession = params_or_error(&id, params)?;
                let session_id = request.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let future = self
                    .agent()
                    .prepare_rename(request)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    future.await.map(|session| SessionResult { session })
                })
            }
            "session.history" => {
                let params: SessionHistoryParams = params_or_error(&id, params)?;
                let result = self.agent().history(params.into());
                Dispatch::Response(agent_result(&id, result))
            }
            "session.presentation" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self.agent().session_presentation(params.session_id);
                Dispatch::Response(agent_result(&id, result))
            }
            _ => return Err(method_not_found(id)),
        })
    }

    fn dispatch_query(
        &mut self,
        id: RpcId,
        method: &str,
        params: Option<Value>,
    ) -> Result<Dispatch, RpcResponse> {
        Ok(match method {
            "session.read" => {
                let params: ReadSession = params_or_error(&id, params)?;
                let request = params;
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_session_read(store, loaded, request, cancellation),
                )
            }
            "workspace.read" => {
                let request: WorkspaceReadRequest = params_or_error(&id, params)?;
                // Lexical path/range validation happens before a query slot is
                // reserved; the offset itself is checked against the file.
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_workspace_read(session, request, cancellation),
                )
            }
            "workspace.files" => {
                let request: WorkspaceFilesRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_workspace_files(session, request, cancellation),
                )
            }
            "workspace.search" => {
                let request: WorkspaceSearchRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_workspace_search(session, request, cancellation),
                )
            }
            "workspace.status" => {
                let request: WorkspaceStatusRequest = params_or_error(&id, params)?;
                if let Err(error) = request.validate() {
                    return Err(query_error(id, &error));
                }
                let Some(session) = self.agent().loaded_session(request.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                if !self.query_capacity_available() {
                    return Err(resource_exhausted(id));
                }
                // Registration happens synchronously inside `prepare`, so the
                // Session owns the worker before this dispatcher can be
                // dropped. The returned future only waits and completes the
                // cache; `session` moves into it.
                let cancellation = self.query_cancellation.clone();
                let query = match crate::queries::prepare_workspace_status(
                    session,
                    request,
                    cancellation,
                ) {
                    Ok(query) => query,
                    Err(error) => return Err(query_error(id, &error)),
                };
                // The awaiter only observes the owned worker's result; the
                // worker owns its git child and enforces its own deadline.
                self.defer_query(id, query)
            }
            "changes.list" => {
                let request: ChangesListRequest = params_or_error(&id, params)?;
                self.admit_query(&id, request.validate())?;
                let deadline = change_deadline();
                let cancellation = self.query_cancellation.clone();
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                // Workspace scope registers its owned worker synchronously
                // inside `prepare`, before this dispatcher can be dropped. The
                // returned future only waits and post-processes.
                let query = match crate::queries::prepare_changes_list(
                    store,
                    loaded,
                    request,
                    cancellation,
                    deadline,
                ) {
                    Ok(query) => query,
                    Err(error) => return Err(query_error(id, &error)),
                };
                self.defer_query(id, query)
            }
            "changes.diff" => {
                let request: ChangesDiffRequest = params_or_error(&id, params)?;
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.session_id);
                let deadline = diff_deadline();
                let cancellation = self.query_cancellation.clone();
                // A `workspace:` ref registers its owned source worker here; a
                // tool ref stays a pure cold read that needs no Workspace or
                // model.
                let query = match crate::queries::prepare_changes_diff(
                    store,
                    loaded,
                    request,
                    cancellation,
                    deadline,
                ) {
                    Ok(query) => query,
                    Err(error) => return Err(query_error(id, &error)),
                };
                self.defer_query(id, query)
            }
            "tool.read" => {
                let params: ToolReadParams = params_or_error(&id, params)?;
                let request: ToolReadRequest = params.into();
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let tool_data = self.agent().tool_data(request.tool_ref.session_id).ok();
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_tool_read(store, tool_data, request, cancellation),
                )
            }
            "tool.output" => {
                let params: ToolOutputParams = params_or_error(&id, params)?;
                let request: ToolOutputRequest = params.into();
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let tool_data = self.agent().tool_data(request.tool_ref.session_id).ok();
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_tool_output(store, tool_data, request, cancellation),
                )
            }
            "turn.result" => {
                let params: TurnResultParams = params_or_error(&id, params)?;
                let request: TurnResultRequest = params.into();
                self.admit_query(&id, request.validate())?;
                let store = self.agent().store_handle();
                let loaded = self.agent().loaded_session(request.turn.session_id);
                let cancellation = self.query_cancellation.clone();
                self.defer_query(
                    id,
                    crate::queries::prepare_turn_result(store, loaded, request, cancellation),
                )
            }
            _ => return Err(method_not_found(id)),
        })
    }

    fn dispatch_turn(
        &mut self,
        id: RpcId,
        method: &str,
        params: Option<Value>,
    ) -> Result<Dispatch, RpcResponse> {
        Ok(match method {
            "turn.send" => {
                let request: SendMessage = params_or_error(&id, params)?;
                let session_id = request.session_id;
                let input = minicore_runtime::execution::UserInput::text(request.text)
                    .map_err(|_| invalid_params(Some(id.clone())))?;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let future = self
                    .agent()
                    .prepare_submit(session_id, input)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    let crate::sessions::LoopSubmission::Accepted(accepted) = future.await?;
                    Ok(TurnResult {
                        turn: accepted.turn,
                        accepted_at: accepted.accepted_at,
                    })
                })
            }
            "turn.steer" => {
                let params: TurnSteerParams = params_or_error(&id, params)?;
                let result =
                    self.agent()
                        .steer_accepted(params.into())
                        .map(|accepted| SteerResult {
                            ok: true,
                            accepted_at: accepted.accepted_at,
                            steer_index: accepted.steer_index,
                        });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.cancel" => {
                let params: TurnParams = params_or_error(&id, params)?;
                let result = self
                    .agent()
                    .cancel(params.into())
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.wait" => {
                let params: TurnParams = params_or_error(&id, params)?;
                match self.agent().wait_turn_receiver(params.into()) {
                    Ok(receiver) => {
                        if self.deferred_count() >= MAX_DEFERRED_WAITERS {
                            return Err(resource_exhausted(id));
                        }
                        self.defer_waiter(id, async move {
                            await_turn_completion(receiver)
                                .await
                                .map(|result| TurnResultView::from_turn_result(&result))
                        })
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "interaction.answer" => {
                let params: InteractionAnswerParams = params_or_error(&id, params)?;
                let answer = match params.answer.into_runtime() {
                    Ok(answer) => answer,
                    Err(_) => return Err(invalid_params(Some(id))),
                };
                let turn = TurnRef {
                    session_id: params.session_id,
                    loop_id: params.loop_id,
                };
                let result = self
                    .agent()
                    .answer_interaction(AnswerInteraction {
                        turn,
                        interaction_id: params.interaction_id,
                        answer,
                    })
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            _ => return Err(method_not_found(id)),
        })
    }
}

pub(super) const METHODS: &[&str] = &[
    "agent.ping",
    "agent.reload",
    "agent.shutdown",
    "profile.list",
    "model.list",
    "session.list",
    "session.create",
    "session.open",
    "session.close",
    "session.delete",
    "session.state",
    "session.context",
    "session.compact",
    "session.compact.cancel",
    "session.update",
    "session.rename",
    "session.history",
    "session.read",
    "session.presentation",
    "workspace.read",
    "workspace.files",
    "workspace.search",
    "workspace.status",
    "changes.list",
    "changes.diff",
    "tool.read",
    "tool.output",
    "turn.send",
    "turn.steer",
    "turn.cancel",
    "turn.wait",
    "turn.result",
    "interaction.answer",
];

pub(super) fn canonical_method(method: &str) -> &'static str {
    METHODS
        .iter()
        .copied()
        .find(|candidate| *candidate == method)
        .unwrap_or("unknown")
}
