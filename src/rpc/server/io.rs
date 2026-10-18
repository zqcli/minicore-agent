use super::*;
use std::io;

pub(super) const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;

pub(super) enum Frame {
    Eof,
    Data(Vec<u8>),
    Oversized,
}

pub(super) async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

pub(super) async fn pump_events(mut events: AgentEventStream, outbound: mpsc::Sender<RpcOutbound>) {
    while let Some(event) = events.recv().await {
        if outbound
            .send(RpcOutbound::Event(Box::new(AgentEventNotification::new(
                event,
            ))))
            .await
            .is_err()
        {
            break;
        }
    }
}

pub(super) async fn run_writer<W>(
    writer: W,
    outbound: mpsc::Receiver<RpcOutbound>,
    status: oneshot::Sender<Result<(), AgentError>>,
) where
    W: AsyncWrite + Unpin,
{
    let result = write_outbound(writer, outbound).await;
    if result.is_err() {
        tracing::warn!(kind = "write_failure", "rpc writer failure");
    }
    let _ = status.send(result);
}

async fn write_outbound<W>(
    mut writer: W,
    mut outbound: mpsc::Receiver<RpcOutbound>,
) -> Result<(), AgentError>
where
    W: AsyncWrite + Unpin,
{
    let mut batch = Vec::with_capacity(OUTBOUND_CAPACITY);
    while outbound.recv_many(&mut batch, OUTBOUND_CAPACITY).await != 0 {
        for frame in batch.drain(..) {
            match frame {
                RpcOutbound::Response(response) => writer.write_all(response.encoded()).await?,
                RpcOutbound::Event(event) => {
                    let encoded =
                        serde_json::to_vec(&event).map_err(|_| AgentError::RpcSerialization)?;
                    writer.write_all(&encoded).await?;
                }
            }
            writer.write_all(b"\n").await?;
        }
        writer.flush().await?;
    }
    Ok(())
}

pub(super) fn parse_frame(frame: &[u8]) -> Result<RpcRequest, RpcResponse> {
    let value = serde_json::from_slice::<Value>(frame).map_err(|_| {
        tracing::warn!(kind = "parse_error", "rpc request rejected");
        RpcResponse::error(None, PARSE_ERROR, "parse error", "parse_error", false)
    })?;
    let candidate_id = request_id(&value);
    parse_request(value).map_err(|_| {
        tracing::warn!(kind = "invalid_request", "rpc request rejected");
        RpcResponse::error(
            candidate_id,
            INVALID_REQUEST,
            "invalid request",
            "invalid_request",
            false,
        )
    })
}

pub(super) async fn read_frame<R>(reader: &mut R, frame: &mut Vec<u8>) -> io::Result<Frame>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(Frame::Eof)
            } else {
                Ok(Frame::Data(std::mem::take(frame)))
            };
        }

        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |position| position + 1);
        // The limit covers the whole frame, including bytes accumulated before
        // an earlier cancellation.
        if frame
            .len()
            .checked_add(consumed)
            .is_none_or(|length| length > MAX_RPC_LINE_BYTES)
        {
            frame.clear();
            return Ok(Frame::Oversized);
        }
        frame.extend_from_slice(&buffer[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Frame::Data(std::mem::take(frame)));
        }
    }
}

#[cfg(test)]
mod tests;
