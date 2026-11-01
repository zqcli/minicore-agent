use super::*;

impl RpcServer {
    pub(super) fn dispatch(&mut self, request: RpcRequest) -> Dispatch {
        let is_push = request.method == "input.push";
        match self.dispatch_inner(request) {
            Ok(dispatch) => dispatch,
            Err(response) => Dispatch::Response(if is_push {
                response.rejected_input()
            } else {
                response
            }),
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
            name if name.starts_with("input.") => self.dispatch_input(id, name, params),
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
                #[cfg(feature = "e2e-observe")]
                crate::e2e_observe::responsiveness_probe("ping");
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
                let charge = self
                    .agent()
                    .reserve_snapshot()
                    .ok_or_else(|| resource_exhausted(id.clone()))?;
                let session_id = crate::ids::SessionId::new()
                    .map_err(|_| agent_error(id.clone(), &AgentError::Internal))?;
                let future = self
                    .agent()
                    .prepare_create(request, session_id)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(Some(Reservation::Session(session_id)), async move {
                    let result = future.await;
                    Box::new(move |agent: &mut Agent| match result {
                        Ok((stored, workspace, setup)) => {
                            let info = agent.install_session(stored, workspace, setup);
                            let session = agent
                                .loaded_session(info.session_id)
                                .expect("installed session");
                            snapshot_reply(id, session, Some(info), charge)
                        }
                        Err(error) => agent_error(id, &error).into(),
                    }) as ApplyOperation
                })
            }
            "session.open" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let charge = self
                    .agent()
                    .reserve_snapshot()
                    .ok_or_else(|| resource_exhausted(id.clone()))?;
                if let Some(session) = self.agent().loaded_session(session_id) {
                    if self.queries.len() >= MAX_DEFERRED_QUERIES {
                        return Err(resource_exhausted(id));
                    }
                    let info = session.info(true);
                    self.spawn_snapshot_reply(snapshot_reply(id, session, Some(info), charge))
                } else {
                    let store = self.agent().store_handle();
                    self.defer_operation(Some(Reservation::Session(session_id)), async move {
                        let result = Agent::prepare_open(store, session_id).await;
                        Box::new(move |agent: &mut Agent| {
                            match result.and_then(|prepared| agent.install_open(prepared)) {
                                Ok(info) => {
                                    let session = agent
                                        .loaded_session(info.session_id)
                                        .expect("installed session");
                                    snapshot_reply(id, session, Some(info), charge)
                                }
                                Err(error) => agent_error(id, &error).into(),
                            }
                        }) as ApplyOperation
                    })
                }
            }
            "session.close" => {
                let (params, load): (SessionParams, _) = params_with_load(&id, params)?;
                let session_id = params.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let session = self.loaded_session_with_load(&id, session_id, load)?;
                session
                    .begin_close()
                    .map_err(|error| agent_error(id.clone(), &error))?;
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
                #[cfg(feature = "e2e-observe")]
                crate::e2e_observe::responsiveness_probe("state");
                let params: SessionParams = params_or_error(&id, params)?;
                let session = self
                    .agent()
                    .loaded_session(params.session_id)
                    .ok_or_else(|| agent_error(id.clone(), &AgentError::SessionNotLoaded))?;
                if self.queries.len() >= MAX_DEFERRED_QUERIES {
                    return Err(resource_exhausted(id));
                }
                let charge = self
                    .agent()
                    .reserve_snapshot()
                    .ok_or_else(|| resource_exhausted(id.clone()))?;
                self.spawn_snapshot_reply(snapshot_reply(id, session, None, charge))
            }
            "session.context" => {
                let params: SessionParams = params_or_error(&id, params)?;
                let result = self.agent().session_context(params.session_id);
                Dispatch::Response(agent_result(&id, result))
            }
            "session.compact" => {
                let (request, load): (CompactSession, _) = params_with_load(&id, params)?;
                let reservation = Some(Reservation::Session(request.session_id));
                self.admit_operation(&id, reservation)?;
                let session = self.loaded_session_with_load(&id, request.session_id, load)?;
                let future = self
                    .agent()
                    .prepare_compact_for(request, session)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_operation(reservation, async move {
                    let result = future.await;
                    Box::new(move |_: &mut Agent| match result {
                        Ok(receiver) => OperationReply::Waiter(Box::pin(async move {
                            agent_result(
                                &id,
                                await_compaction_completion(receiver)
                                    .await
                                    .map(|completion| CompactResult {
                                        result: completion.result,
                                        next: completion.next,
                                    }),
                            )
                        })),
                        Err(error) => agent_error(id, &error).into(),
                    }) as ApplyOperation
                })
            }
            "session.compact.cancel" => {
                let (params, load): (CompactSession, _) = params_with_load(&id, params)?;
                let session = self.loaded_session_with_load(&id, params.session_id, load)?;
                if !crate::compaction::valid_operation_id(&params.operation_id) {
                    return Err(invalid_params(Some(id)));
                }
                let result = session
                    .cancel_compaction(&params.operation_id)
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "session.update" => {
                let (request, load): (UpdateSession, _) = params_with_load(&id, params)?;
                if !request.has_field() {
                    return Err(invalid_params(Some(id)));
                }
                self.admit_operation(&id, Some(Reservation::Session(request.session_id)))?;
                let session_id = request.session_id;
                let session = self.loaded_session_with_load(&id, session_id, load)?;
                let future = self
                    .agent()
                    .prepare_update_for(request, session)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    future.await.map(SessionUpdateResult::from)
                })
            }
            "session.rename" => {
                let (request, load): (RenameSession, _) = params_with_load(&id, params)?;
                let session_id = request.session_id;
                self.admit_operation(&id, Some(Reservation::Session(session_id)))?;
                let session = self.agent().loaded_session(session_id);
                match &session {
                    Some(session) => validate_load(&id, session, load.as_deref())?,
                    None if load.is_some() => return Err(agent_error(id, &AgentError::StaleLoad)),
                    None => {}
                }
                let future = self
                    .agent()
                    .prepare_rename_for(request, session)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                self.defer_response(id, Some(Reservation::Session(session_id)), async move {
                    future.await.map(|session| SessionResult {
                        session,
                        load: None,
                        snapshot: None,
                    })
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
                #[cfg(feature = "e2e-observe")]
                crate::e2e_observe::responsiveness_probe("workspace");
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
                    crate::queries::prepare_turn_result(
                        self.agent().result_catalog(),
                        store,
                        loaded,
                        request,
                        cancellation,
                    ),
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
            "turn.cancel" => {
                let (params, load): (TurnParams, _) = params_with_load(&id, params)?;
                let session = self.loaded_session_with_load(&id, params.session_id, load)?;
                let result = session
                    .cancel(params.into())
                    .map(|cancelled| CancelledResult { cancelled });
                Dispatch::Response(agent_result(&id, result))
            }
            "turn.wait" => {
                let params: TurnParams = params_or_error(&id, params)?;
                let turn = TurnRef::from(params);
                let Some(session) = self.agent().loaded_session(turn.session_id) else {
                    return Err(agent_error(id, &AgentError::SessionNotLoaded));
                };
                match session.wait_receiver(turn) {
                    Ok(receiver) => {
                        if self.deferred_count() >= MAX_DEFERRED_WAITERS {
                            return Err(resource_exhausted(id));
                        }
                        self.defer_waiter(id, async move {
                            let result = await_turn_completion(receiver).await?;
                            Ok(TurnWaitResult {
                                turn: TurnResultView::from_turn_result(&result),
                                next: result.next.clone(),
                                queue: session.queue(),
                            })
                        })
                    }
                    Err(error) => Dispatch::Response(agent_error(id, &error)),
                }
            }
            "interaction.answer" => {
                let (params, load): (InteractionAnswerParams, _) = params_with_load(&id, params)?;
                let answer = match params.answer.into_runtime() {
                    Ok(answer) => answer,
                    Err(_) => return Err(invalid_params(Some(id))),
                };
                let turn = TurnRef {
                    session_id: params.session_id,
                    loop_id: params.loop_id,
                };
                let session = self.loaded_session_with_load(&id, params.session_id, load)?;
                let result = session
                    .answer(turn, params.interaction_id, answer)
                    .map(|()| OkResult::TRUE);
                Dispatch::Response(agent_result(&id, result))
            }
            _ => return Err(method_not_found(id)),
        })
    }
}

impl RpcServer {
    fn dispatch_input(
        &mut self,
        id: RpcId,
        method: &str,
        params: Option<Value>,
    ) -> Result<Dispatch, RpcResponse> {
        Ok(match method {
            "input.push" => {
                let (request, load): (PushInput, _) = params_with_load(&id, params)?;
                // A push that starts a turn is deferred and must not overlap a
                // reload, so admit it before the input is accepted.
                self.admit_operation(&id, None)?;
                let session = self.loaded_session_with_load(&id, request.session_id, load)?;
                let pending = session
                    .accept_input(request.mode, request.text)
                    .map_err(|error| agent_error(id.clone(), &error))?;
                if pending.needs_start() {
                    self.defer_operation(None, async move {
                        let result = pending.start().await;
                        Box::new(move |_: &mut Agent| accepted_input_response(&id, result).into())
                            as ApplyOperation
                    })
                } else {
                    Dispatch::Response(accepted_input_response(&id, Ok(pending.finish(None))))
                }
            }
            "input.remove" => {
                let (request, load): (RemoveInput, _) = params_with_load(&id, params)?;
                let session = self.loaded_session_with_load(&id, request.session_id, load)?;
                Dispatch::Response(agent_result(&id, session.remove_input(&request.id)))
            }
            "input.recall" => {
                let (params, load): (SessionParams, _) = params_with_load(&id, params)?;
                let session = self.loaded_session_with_load(&id, params.session_id, load)?;
                Dispatch::Response(success(&id, session.recall_inputs()))
            }
            "input.resume" => {
                let (params, load): (SessionParams, _) = params_with_load(&id, params)?;
                self.admit_operation(&id, None)?;
                let session = self.loaded_session_with_load(&id, params.session_id, load)?;
                let pending = session
                    .accept_resume()
                    .map_err(|error| agent_error(id.clone(), &error))?;
                if pending.needs_start() {
                    self.defer_response(id, None, pending.start())
                } else {
                    Dispatch::Response(success(&id, pending.finish(None)))
                }
            }
            "input.list" => {
                let params: SessionParams = params_or_error(&id, params)?;
                Dispatch::Response(agent_result(
                    &id,
                    self.agent().list_inputs(params.session_id),
                ))
            }
            _ => return Err(method_not_found(id)),
        })
    }

    fn loaded_session_with_load(
        &self,
        id: &RpcId,
        session_id: crate::ids::SessionId,
        load: Option<String>,
    ) -> Result<crate::sessions::Session, RpcResponse> {
        if load.is_none() {
            return Err(agent_error(id.clone(), &AgentError::MissingLoad));
        }
        let session = self
            .agent()
            .loaded_session(session_id)
            .ok_or_else(|| agent_error(id.clone(), &AgentError::StaleLoad))?;
        validate_load(id, &session, load.as_deref())?;
        Ok(session)
    }
}

fn params_with_load<T: serde::de::DeserializeOwned>(
    id: &RpcId,
    params: Option<Value>,
) -> Result<(T, Option<String>), RpcResponse> {
    let mut object = match params.unwrap_or_else(|| Value::Object(Default::default())) {
        Value::Object(object) => object,
        _ => return Err(invalid_params(Some(id.clone()))),
    };
    let load = match object.remove("load") {
        None | Some(Value::Null) => None,
        Some(Value::String(load)) => Some(load),
        _ => return Err(invalid_params(Some(id.clone()))),
    };
    Ok((params_or_error(id, Some(Value::Object(object)))?, load))
}

fn validate_load(
    id: &RpcId,
    session: &crate::sessions::Session,
    load: Option<&str>,
) -> Result<(), RpcResponse> {
    match load {
        None => Err(agent_error(id.clone(), &AgentError::MissingLoad)),
        // Comparing the wire token keeps a well-formed but unissued load a
        // stale-load error instead of a parameter-shape error.
        Some(load) if load != session.load().to_string() => {
            Err(agent_error(id.clone(), &AgentError::StaleLoad))
        }
        Some(_) => Ok(()),
    }
}

fn snapshot_reply(
    id: RpcId,
    session: crate::sessions::Session,
    info: Option<crate::agent::SessionInfo>,
    charge: crate::sessions::hub::SnapshotCharge,
) -> OperationReply {
    #[cfg(test)]
    let gate_key = session.session_id();
    // Capture the admitted observation before shutdown can unregister this load.
    let result = session.sample();
    #[cfg(feature = "e2e-observe")]
    let mut charge = charge;
    #[cfg(feature = "e2e-observe")]
    if let Ok((_, snapshot)) = &result {
        charge.observe(snapshot);
    }
    let load = session.load().to_string();
    drop(session);
    OperationReply::Waiter(Box::pin(async move {
        #[cfg(test)]
        if let Some(gate) = super::SNAPSHOT_REPLY_GATES.take(&gate_key) {
            gate.pass().await;
        }
        let error_id = id.clone();
        match tokio::task::spawn_blocking(move || {
            #[cfg(feature = "e2e-observe")]
            let observed_start = crate::e2e_observe::clock();
            let response = match result {
                Ok((state, snapshot)) => {
                    let response = if let Some(info) = info {
                        RpcResponse::snapshot_success(
                            id.clone(),
                            SessionResult {
                                session: info,
                                load: Some(load),
                                snapshot: Some(snapshot),
                            },
                        )
                    } else {
                        RpcResponse::snapshot_success(
                            id.clone(),
                            SessionStateResult {
                                state: SessionStateView::from(&state),
                                snapshot,
                            },
                        )
                    };
                    response.unwrap_or_else(|_| agent_error(id, &AgentError::RpcSerialization))
                }
                Err(error) => agent_error(id, &error),
            };
            #[cfg(feature = "e2e-observe")]
            if let Some(key) = charge.observed_key() {
                crate::e2e_observe::encoded(key, observed_start, response.encoded().len());
            }
            (response, charge)
        })
        .await
        {
            Ok((response, charge)) => {
                drop(error_id);
                response.charged(charge)
            }
            Err(_) => agent_error(error_id, &AgentError::Internal),
        }
    }))
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
    "input.push",
    "input.remove",
    "input.recall",
    "input.resume",
    "input.list",
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
