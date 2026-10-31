use super::*;
use std::io;

/// Largest legal inbound request frame, including the trailing newline.
///
/// A legal input push carries at most `BoundedText::MAX_BYTES` of text, which
/// stays far below this even when every byte is a JSON-escaped control
/// character. Responses are bounded separately by the control-frame ceiling.
pub(super) const MAX_RPC_LINE_BYTES: usize = 8 * 1024 * 1024;

/// Largest legal outbound frame, including the trailing newline.
///
/// The legal maximum is a control snapshot or an `input.push` receipt carrying
/// 32 queued inputs plus one claimed input at `BoundedText::MAX_BYTES` each.
/// JSON escaping of one-byte control characters costs six bytes per byte, so
/// the text alone needs 33 * 262144 * 6 = 51,904,512 bytes; the largest
/// actionable interaction, 256 completion descriptors, identities, keys and
/// the envelope stay below the remainder. Nothing is truncated to fit.
pub(crate) const MAX_RPC_FRAME_BYTES: usize = 64 * 1024 * 1024;

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
    while let Some((event, charge)) = events.recv_owned().await {
        let notification = AgentEventNotification::new(event);
        let frame = if let Some(charge) = charge {
            match tokio::task::spawn_blocking(move || {
                RpcResponse::notification(notification, charge)
            })
            .await
            {
                Ok(Ok(response)) => RpcOutbound::Response(response),
                _ => RpcOutbound::Failed,
            }
        } else {
            RpcOutbound::Event(Box::new(notification))
        };
        if outbound.send(frame).await.is_err() {
            break;
        }
    }
    if events.control_failed() {
        let _ = outbound.send(RpcOutbound::Failed).await;
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
            let event_bytes;
            let encoded = match &frame {
                RpcOutbound::Response(response) => response.encoded(),
                RpcOutbound::Event(event) => {
                    event_bytes = crate::budget::encode_json(event)
                        .map_err(|_| AgentError::RpcSerialization)?;
                    &event_bytes
                }
                RpcOutbound::Failed => return Err(AgentError::Internal),
            };
            if encoded.len() + 1 > MAX_RPC_FRAME_BYTES {
                // A frame beyond the legal maximum is a producer bug; emitting
                // a truncated or substitute message would corrupt the contract.
                return Err(AgentError::Internal);
            }
            #[cfg(feature = "e2e-observe")]
            let write_started = crate::e2e_observe::clock();
            writer.write_all(encoded).await?;
            writer.write_all(b"\n").await?;
            if matches!(&frame, RpcOutbound::Response(response) if response.is_charged()) {
                // A buffered writer can retain a tail after write_all completes.
                writer.flush().await?;
                #[cfg(feature = "e2e-observe")]
                if let RpcOutbound::Response(response) = &frame
                    && let Some(key) = response.observed_snapshot()
                {
                    crate::e2e_observe::sent(key, write_started, encoded.len() + 1);
                }
            }
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
