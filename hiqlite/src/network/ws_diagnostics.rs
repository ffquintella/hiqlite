//! Diagnostics shared by every WebSocket reader / writer loop.
//!
//! Each hiqlite WebSocket (Raft replication, the client API stream, `/listen`, the proxy) is read
//! by a loop that ends the connection on the first `read_frame` error. Such an exit fails every
//! request that is in flight on that socket, so it must never be silent: an oversized message in
//! particular (`FrameTooLarge`) looks like an unexplained connection reset on the sending side
//! and like a request timeout to the application unless the reader says why it stopped.

use fastwebsockets::WebSocketError;
use std::fmt::Display;
use tracing::error;

/// Largest WebSocket message, in bytes, that a hiqlite peer accepts.
///
/// hiqlite never calls `set_max_message_size`, so this is the fastwebsockets 0.10 default
/// (`64 << 20`, 64 MiB). The *receiving* side rejects any frame whose payload is at least this
/// large with [`WebSocketError::FrameTooLarge`] and closes the connection. The tests below pin
/// this value against the library, so a dependency bump that changes the default fails loudly
/// instead of making these log lines wrong.
pub(crate) const WS_MAX_MESSAGE_SIZE: usize = 64 << 20;

/// Logs why a WebSocket reader loop stops after `read_frame` returned `err`.
///
/// `stream` names the loop (e.g. "Client API stream"), `peer` the remote end as precisely as the
/// caller knows it (node id and / or address).
pub(crate) fn log_ws_read_error(stream: &str, peer: impl Display, err: &WebSocketError) {
    match err {
        WebSocketError::FrameTooLarge => error!(
            "{stream}: WebSocket read from {peer} failed with FrameTooLarge: the peer sent a \
             message of {WS_MAX_MESSAGE_SIZE} bytes (64 MiB) or more, the WebSocket max message \
             size - closing the connection, every request in flight on it fails"
        ),
        err => error!(
            "{stream}: WebSocket read from {peer} failed - closing the connection, every \
             request in flight on it fails: {err:?}"
        ),
    }
}

/// Logs, on the sending side, a message of `len` bytes that the peer is going to reject with
/// `FrameTooLarge`. The message is still sent: this only makes the cause visible in the sender's
/// log, where the failure otherwise shows up as nothing more than a connection reset.
pub(crate) fn log_if_exceeds_ws_limit(stream: &str, peer: impl Display, len: usize) {
    if len >= WS_MAX_MESSAGE_SIZE {
        error!(
            "{stream}: sending a {len} byte message to {peer}, at or above the \
             {WS_MAX_MESSAGE_SIZE} byte (64 MiB) WebSocket max message size - the peer will \
             reject it with FrameTooLarge and drop the connection"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::WS_MAX_MESSAGE_SIZE;
    use fastwebsockets::{FragmentCollectorRead, Role, WebSocketError, after_handshake_split};
    use tokio::io::AsyncWriteExt;

    /// Feeds the header of an unmasked, final binary frame announcing `len` payload bytes (and no
    /// payload) to a client-side reader, exactly like `client::stream::stream_reader` reads the
    /// leader's responses, and returns the error `read_frame` fails with.
    async fn read_error_for_announced_len(len: usize) -> WebSocketError {
        let (client_io, mut server_io) = tokio::io::duplex(64);
        let (ws_read, _ws_write) =
            after_handshake_split(client_io, tokio::io::sink(), Role::Client);
        let mut read = FragmentCollectorRead::new(ws_read);

        let mut header = vec![0x82, 127];
        header.extend_from_slice(&(len as u64).to_be_bytes());
        server_io.write_all(&header).await.unwrap();
        // EOF right after the header: the payload never arrives.
        drop(server_io);

        match read
            .read_frame(&mut |_| async { Ok::<(), std::io::Error>(()) })
            .await
        {
            Ok(frame) => panic!(
                "a frame without its payload can never be read successfully, got {:?}",
                frame.opcode
            ),
            Err(err) => err,
        }
    }

    #[tokio::test]
    async fn ws_max_message_size_rejects_frames_at_the_limit() {
        let err = read_error_for_announced_len(WS_MAX_MESSAGE_SIZE).await;
        assert!(
            matches!(err, WebSocketError::FrameTooLarge),
            "expected FrameTooLarge at exactly WS_MAX_MESSAGE_SIZE, got {err:?}"
        );
    }

    #[tokio::test]
    async fn ws_max_message_size_accepts_frames_below_the_limit() {
        // One byte below the limit passes the size check, so the reader waits for the payload
        // and only fails because the stream ends.
        let err = read_error_for_announced_len(WS_MAX_MESSAGE_SIZE - 1).await;
        assert!(
            matches!(err, WebSocketError::UnexpectedEOF),
            "expected UnexpectedEOF just below WS_MAX_MESSAGE_SIZE, got {err:?}"
        );
    }
}
