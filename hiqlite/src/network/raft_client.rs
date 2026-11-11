use crate::Node;
use crate::NodeId;
use crate::app_state::RaftType;
use crate::helpers::{deserialize_serde, serialize_serde};
use crate::network::raft_server::{
    RaftStreamRequest, RaftStreamResponse, RaftStreamResponsePayload,
};
use crate::network::web_socket_connect;
use crate::network::ws_diagnostics::{log_if_exceeds_ws_limit, log_ws_read_error};
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, Payload, WebSocketWrite};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use openraft::error::RPCError;
use openraft::error::Unreachable;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::{select, task, time};
use tracing::{debug, error, info};

#[cfg(feature = "cache")]
use crate::store::state_machine::memory::TypeConfigKV;

#[cfg(feature = "sqlite")]
use crate::store::state_machine::sqlite::TypeConfigSqlite;

#[cfg(any(feature = "cache", feature = "sqlite"))]
use crate::Error;
#[cfg(any(feature = "cache", feature = "sqlite"))]
use openraft::{
    error::{InstallSnapshotError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

pub struct NetworkStreaming {
    pub node_id: NodeId,
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    pub secret_raft: Vec<u8>,
    pub raft_type: RaftType,
    pub heartbeat_interval: u64,
    pub is_raft_stopped: Arc<AtomicBool>,
    pub is_startup_finished: Arc<AtomicBool>,
    /// Idle peer connections, so a short-lived client does not pay a TCP + TLS + WebSocket
    /// handshake of its own.
    pub pool: ConnPool,
}

/// Most idle connections kept per peer. A connection above this is shut down on return.
const POOL_MAX_IDLE_PER_PEER: usize = 4;
/// An idle connection older than this is shut down instead of reused.
const POOL_IDLE_TTL: Duration = Duration::from_secs(60);

/// One WebSocket stream to a peer and the task that owns it.
///
/// Dropping it shuts the stream down, so a connection that is evicted or never returned to the
/// pool cannot leak its socket.
struct PooledConn {
    node: Node,
    sender: flume::Sender<RaftRequest>,
    task: JoinHandle<()>,
    idle_since: Instant,
}

impl PooledConn {
    fn is_alive(&self) -> bool {
        !self.task.is_finished() && !self.sender.is_disconnected()
    }

    fn is_reusable(&self, node: &Node) -> bool {
        self.node.addr_raft == node.addr_raft
            && self.idle_since.elapsed() < POOL_IDLE_TTL
            && self.is_alive()
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        let _ = self.sender.try_send(RaftRequest::Shutdown);
        self.task.abort();
    }
}

/// Idle [`PooledConn`]s per peer node id.
///
/// openraft builds a client for every linearizable read it confirms against a peer, and drops it
/// when the read ends. Opening a stream per read costs a TCP, TLS and WebSocket handshake inside
/// the read's heartbeat-sized timeout, and one lost SYN (a 1 s retransmit) is enough to fail it.
/// A checked-out connection is used by one client at a time, exactly like a fresh one, so
/// requests on a stream are still handled in order. Clients that are long-lived, such as a
/// replication stream, keep their connection to themselves until they are dropped.
#[derive(Clone, Default)]
pub struct ConnPool(Arc<Mutex<HashMap<NodeId, Vec<PooledConn>>>>);

impl ConnPool {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<NodeId, Vec<PooledConn>>> {
        // A panic while holding the lock cannot leave the map inconsistent: entries are only
        // pushed and popped.
        self.0.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Most recently used reusable connection to `node`. Unusable ones met on the way are shut
    /// down.
    fn take(&self, node: &Node) -> Option<PooledConn> {
        let mut map = self.lock();
        let idle = map.get_mut(&node.id)?;
        while let Some(conn) = idle.pop() {
            if conn.is_reusable(node) {
                return Some(conn);
            }
        }
        None
    }

    fn give_back(&self, mut conn: PooledConn) {
        let mut map = self.lock();
        let idle = map.entry(conn.node.id).or_default();
        idle.retain(|c| c.is_reusable(&conn.node));
        if idle.len() < POOL_MAX_IDLE_PER_PEER && conn.is_alive() {
            conn.idle_since = Instant::now();
            idle.push(conn);
        }
    }
}

#[cfg(feature = "cache")]
impl RaftNetworkFactory<TypeConfigKV> for NetworkStreaming {
    type Network = NetworkConnectionStreaming;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        debug!("Building new Raft Cache client with target {}", node);
        self.checkout(node)
    }
}

#[cfg(feature = "sqlite")]
impl RaftNetworkFactory<TypeConfigSqlite> for NetworkStreaming {
    type Network = NetworkConnectionStreaming;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        debug!("Building new Raft DB client with target {}", node);
        self.checkout(node)
    }
}

#[derive(Debug)]
enum RaftRequest {
    #[cfg(feature = "sqlite")]
    AppendDB(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            AppendEntriesRequest<TypeConfigSqlite>,
        ),
    ),
    #[cfg(feature = "sqlite")]
    VoteDB(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            VoteRequest<u64>,
        ),
    ),
    #[cfg(feature = "sqlite")]
    SnapshotDB(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            InstallSnapshotRequest<TypeConfigSqlite>,
        ),
    ),

    #[cfg(feature = "cache")]
    AppendCache(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            AppendEntriesRequest<TypeConfigKV>,
        ),
    ),
    #[cfg(feature = "cache")]
    VoteCache(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            VoteRequest<u64>,
        ),
    ),
    #[cfg(feature = "cache")]
    SnapshotCache(
        (
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
            InstallSnapshotRequest<TypeConfigKV>,
        ),
    ),

    StreamResponse(RaftStreamResponse),

    ReaderExit,
    Shutdown,
}

#[derive(Debug)]
enum WritePayload {
    Payload(Vec<u8>),
    Close,
}

/// A [`JoinHandle`] that aborts its task when dropped.
///
/// `ws_handler` owns the handles of the reader and writer tasks that hold the
/// two halves of the WebSocket, and aborts them on its own reconnect path. That
/// path never runs when `ws_handler` is itself aborted, and dropping a bare
/// `JoinHandle` only detaches its task instead of cancelling it, so the two
/// tasks would keep the socket open for good. Wrapping the handles propagates
/// the cancellation: aborting `ws_handler` drops its locals, and dropping this
/// guard aborts the task it holds.
struct AbortOnDrop(JoinHandle<()>);

impl AbortOnDrop {
    /// Abort eagerly. Idempotent, so the explicit aborts on the reconnect path
    /// and the later `Drop` do not conflict.
    #[inline]
    fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[allow(clippy::type_complexity)]
impl NetworkStreaming {
    /// A client for `node`, on an idle pooled stream when there is one and on a new one when not.
    fn checkout(&self, node: &Node) -> NetworkConnectionStreaming {
        let conn = match self.pool.take(node) {
            Some(conn) => {
                debug!("Reusing a pooled Raft stream to {}", node);
                conn
            }
            None => self.open(node),
        };

        NetworkConnectionStreaming {
            node: node.clone(),
            sender: conn.sender.clone(),
            conn: Some(conn),
            pool: self.pool.clone(),
            in_rpc: false,
            healthy: true,
        }
    }

    fn open(&self, node: &Node) -> PooledConn {
        let (sender, rx) = flume::bounded(1);

        let task = tokio::task::spawn(Box::pin(Self::ws_handler(
            self.node_id,
            self.raft_type.clone(),
            node.clone(),
            self.tls_config.clone(),
            self.secret_raft.clone(),
            rx,
            self.heartbeat_interval,
            self.is_raft_stopped.clone(),
            self.is_startup_finished.clone(),
        )));

        PooledConn {
            node: node.clone(),
            sender,
            task,
            idle_since: Instant::now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn ws_handler(
        this_node: NodeId,
        raft_type: RaftType,
        node: Node,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        secret: Vec<u8>,
        rx: flume::Receiver<RaftRequest>,
        heartbeat_interval: u64,
        is_raft_stopped: Arc<AtomicBool>,
        is_startup_finished: Arc<AtomicBool>,
    ) {
        let mut request_id = 0usize;
        // TODO probably, a Vec<_> is faster here since we would never have too many in flight reqs
        // for raft internal replication and voting? -> check
        // maybe feature-gate an alternative impl, even though it might not make the biggest difference
        let mut in_flight: HashMap<
            usize,
            oneshot::Sender<Result<RaftStreamResponsePayload, Error>>,
        > = HashMap::with_capacity(4);
        let mut shutdown = false;

        'outer: loop {
            if is_raft_stopped.load(Ordering::Relaxed) {
                if !is_startup_finished.load(Ordering::Relaxed) {
                    debug!("Raft is still starting up - skipping initial connection");
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                debug!("Raft is stopped - exiting NetworkStreaming::ws_handler()");
                break;
            }

            info!("Trying to open WebSocket stream");
            let socket = {
                match web_socket_connect::try_connect(
                    this_node,
                    &node.addr_raft,
                    &format!("/stream/{}", raft_type.as_str()),
                    tls_config.clone(),
                    &secret,
                )
                .await
                {
                    Ok(socket) => {
                        info!("WebSocket connected successfully");
                        socket
                    }
                    Err(err) => {
                        error!("Socket connect error to node {}: {:?}", node.id, err);

                        for _ in 0..3 {
                            // if there is a network error, no reason to try too hard to connect
                            time::sleep(Duration::from_millis(heartbeat_interval)).await;

                            // make sure channel is always free
                            if let Ok(req) = rx.try_recv() {
                                let ack = match req {
                                    #[cfg(feature = "sqlite")]
                                    RaftRequest::AppendDB((ack, _)) => Some(ack),
                                    #[cfg(feature = "sqlite")]
                                    RaftRequest::VoteDB((ack, _)) => Some(ack),
                                    #[cfg(feature = "sqlite")]
                                    RaftRequest::SnapshotDB((ack, _)) => Some(ack),
                                    #[cfg(feature = "cache")]
                                    RaftRequest::AppendCache((ack, _)) => Some(ack),
                                    #[cfg(feature = "cache")]
                                    RaftRequest::VoteCache((ack, _)) => Some(ack),
                                    #[cfg(feature = "cache")]
                                    RaftRequest::SnapshotCache((ack, _)) => Some(ack),
                                    RaftRequest::StreamResponse(_) => None,
                                    RaftRequest::ReaderExit => {
                                        continue;
                                    }
                                    RaftRequest::Shutdown => {
                                        break 'outer;
                                    }
                                };

                                if let Some(ack) = ack {
                                    let _ = ack.send(Err(Error::Connect(err.to_string())));
                                }
                            }
                        }

                        continue;
                    }
                }
            };
            assert!(
                in_flight.is_empty(),
                "raft in flight buffer should always be empty when restoring a connection"
            );

            let (tx_write, rx_write) = flume::bounded(1);
            let (tx_read, rx_read) = flume::bounded(1);

            // TODO splitting needs `unstable-split` feature right now but is about to be stabilized soon
            let (read, write) = socket.split(tokio::io::split);
            // IMPORTANT: the reader is NOT CANCEL SAFE in v0.8!
            let read = FragmentCollectorRead::new(read);

            let peer = format!(
                "{:?} Raft member node {} ({})",
                raft_type, node.id, node.addr_raft
            );

            // Guarded: `ws_handler` may be aborted before it reaches the
            // aborts below, and these must then cancel and not detach.
            let handle_read = AbortOnDrop(task::spawn(Box::pin(Self::stream_reader(
                read,
                tx_read.clone(),
                peer.clone(),
            ))));
            let handle_write = AbortOnDrop(task::spawn(Box::pin(Self::stream_writer(
                write, rx_write, peer,
            ))));

            loop {
                let res = select! {
                    res = rx_read.recv_async() => res,
                    res = rx.recv_async() => res,
                };

                let req = match res {
                    Ok(r) => r,
                    Err(err) => {
                        error!("Client stream reader error: {}", err,);

                        if rx.is_disconnected() {
                            debug!("Raft tx dropped - exiting Stream Reader");
                            shutdown = true;
                        }
                        if rx_read.is_disconnected() {
                            debug!("Client Stream reader exited - initiating shutdown + reconnect");
                        }

                        break;
                    }
                };

                let stream_req = match req {
                    #[cfg(feature = "sqlite")]
                    RaftRequest::AppendDB((ack, req)) => {
                        Some((ack, RaftStreamRequest::AppendDB((request_id, req))))
                    }
                    #[cfg(feature = "sqlite")]
                    RaftRequest::VoteDB((ack, req)) => {
                        Some((ack, RaftStreamRequest::VoteDB((request_id, req))))
                    }
                    #[cfg(feature = "sqlite")]
                    RaftRequest::SnapshotDB((ack, req)) => {
                        Some((ack, RaftStreamRequest::SnapshotDB((request_id, req))))
                    }

                    #[cfg(feature = "cache")]
                    RaftRequest::AppendCache((ack, req)) => {
                        Some((ack, RaftStreamRequest::AppendCache((request_id, req))))
                    }
                    #[cfg(feature = "cache")]
                    RaftRequest::VoteCache((ack, req)) => {
                        Some((ack, RaftStreamRequest::VoteCache((request_id, req))))
                    }
                    #[cfg(feature = "cache")]
                    RaftRequest::SnapshotCache((ack, req)) => {
                        Some((ack, RaftStreamRequest::SnapshotCache((request_id, req))))
                    }

                    RaftRequest::StreamResponse(resp) => {
                        match in_flight.remove(&resp.request_id) {
                            None => {
                                error!("client ack for RaftStreamResponse missing");
                            }
                            Some(ack) => {
                                if ack.send(Ok(resp.payload)).is_err() {
                                    error!("sending back stream response from raft server");
                                }
                            }
                        }
                        None
                    }

                    RaftRequest::ReaderExit => {
                        debug!(
                            "ReaderExit - Client Stream reader exited - initiating shutdown + reconnect"
                        );
                        break;
                    }
                    RaftRequest::Shutdown => {
                        debug!("RaftRequest::Shutdown");
                        shutdown = true;
                        break;
                    }
                };

                if let Some((ack, payload)) = stream_req {
                    let bytes = serialize_serde(&payload).unwrap();

                    if let Err(err) = tx_write.send_async(WritePayload::Payload(bytes)).await {
                        let _ = ack.send(Err(Error::Connect(format!(
                            "Error sending Write Request to WebSocket writer: {err}"
                        ))));
                        break;
                    }

                    in_flight.insert(request_id, ack);
                    request_id += 1;
                }
            }

            let _ = tx_write.send_async(WritePayload::Close).await;

            for (_, ack) in in_flight.drain() {
                let _ = ack.send(Err(Error::Connect("Raft WebSocket stream ended".into())));
            }
            // reset to a reasonable size for the next start to keep memory usage under control
            in_flight = HashMap::with_capacity(4);

            // Give the writer enough time to possibly send out the close request.
            // Since we need to re-connect, there is no need to rush anyway.
            time::sleep(Duration::from_millis(250)).await;

            handle_write.abort();
            handle_read.abort();

            if shutdown {
                break;
            }
        }

        debug!("Raft Client shut down, tx closed, exiting WsHandler");
    }

    /// `peer` names the cluster member this connection was opened to, for the logs.
    async fn stream_reader(
        mut read: FragmentCollectorRead<ReadHalf<TokioIo<Upgraded>>>,
        tx: flume::Sender<RaftRequest>,
        peer: String,
    ) {
        loop {
            let frame = match read
                .read_frame(&mut |frame| async move {
                    // TODO obligated sends should be auto ping / pong / close ? -> verify!
                    debug!(
                        "Received obligated send in stream client: OpCode: {:?}: {:?}",
                        frame.opcode.clone(),
                        frame.payload
                    );
                    Ok::<(), Error>(())
                })
                .await
            {
                Ok(frame) => frame,
                Err(err) => {
                    // Never silent: the reconnect that follows fails every in-flight Raft RPC.
                    log_ws_read_error("Raft client stream", &peer, &err);
                    break;
                }
            };

            match frame.opcode {
                OpCode::Continuation => {}
                OpCode::Text => {}
                OpCode::Binary => {
                    let bytes = frame.payload.deref();
                    let payload = match deserialize_serde::<RaftStreamResponse>(bytes) {
                        Ok(payload) => payload,
                        Err(err) => {
                            // corrupt frame from a cluster member: drop the connection instead
                            // of panicking the raft client task
                            error!(
                                "Raft client stream: error deserializing a {} byte response from \
                                 {peer} - closing the connection: {err:?}",
                                bytes.len()
                            );
                            break;
                        }
                    };
                    if let Err(err) = tx.send_async(RaftRequest::StreamResponse(payload)).await {
                        error!(
                            "Error sending Response to Raft client stream manager: {:?}",
                            err
                        );
                    }
                }
                OpCode::Close => break,
                OpCode::Ping => {}
                OpCode::Pong => {}
            }
        }

        let _ = tx.send_async(RaftRequest::ReaderExit).await;
        debug!("Exiting Client Stream Reader");
    }

    async fn stream_writer(
        mut write: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        rx: flume::Receiver<WritePayload>,
        peer: String,
    ) {
        while let Ok(payload) = rx.recv_async().await {
            match payload {
                WritePayload::Payload(bytes) => {
                    // A Raft request this large (e.g. a big AppendEntries batch) is rejected by
                    // the member, and openraft retries it as-is, so say why replication stalls.
                    log_if_exceeds_ws_limit("Raft client stream", &peer, bytes.len());
                    let frame = Frame::binary(Payload::from(bytes));
                    if let Err(err) = write.write_frame(frame).await {
                        error!("Client Stream error: {:?}", err);
                        break;
                    }
                }
                WritePayload::Close => {
                    debug!("Received Close request in Client Stream Writer");
                    let _ = write.write_frame(Frame::close(1000, b"go away")).await;
                    break;
                }
            }
        }

        debug!("Exiting Client Stream Writer");
    }
}

/// Whether the peer answered that its Raft instance is dead (stopped, or failed for good).
///
/// A stream can outlive the instance it was opened to: the node restarts, a new instance listens
/// on the same address, and the old stream keeps answering `Stopped` forever, because the server
/// only closes it for a stopped `append_entries`.
fn payload_is_fatal(payload: &RaftStreamResponsePayload) -> bool {
    match payload {
        #[cfg(feature = "sqlite")]
        RaftStreamResponsePayload::AppendDB(res) => matches!(res, Err(RaftError::Fatal(_))),
        #[cfg(feature = "sqlite")]
        RaftStreamResponsePayload::VoteDB(res) => matches!(res, Err(RaftError::Fatal(_))),
        #[cfg(feature = "sqlite")]
        RaftStreamResponsePayload::SnapshotDB(res) => matches!(res, Err(RaftError::Fatal(_))),
        #[cfg(feature = "cache")]
        RaftStreamResponsePayload::AppendCache(res) => matches!(res, Err(RaftError::Fatal(_))),
        #[cfg(feature = "cache")]
        RaftStreamResponsePayload::VoteCache(res) => matches!(res, Err(RaftError::Fatal(_))),
        #[cfg(feature = "cache")]
        RaftStreamResponsePayload::SnapshotCache(res) => matches!(res, Err(RaftError::Fatal(_))),
    }
}

#[allow(clippy::type_complexity)]
pub struct NetworkConnectionStreaming {
    node: Node,
    sender: flume::Sender<RaftRequest>,
    /// The checked-out stream. Taken on drop, to go back to `pool` or be shut down.
    conn: Option<PooledConn>,
    pool: ConnPool,
    /// A request is on the wire and has no answer yet. Still set on drop when openraft gave up on
    /// the call (its heartbeat timeout drops the future), and the stream is then not reused: the
    /// peer would first answer a request nobody waits for.
    in_rpc: bool,
    /// Cleared when a request on the stream failed, so a broken stream is not handed out again.
    healthy: bool,
}

impl Drop for NetworkConnectionStreaming {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.healthy && !self.in_rpc {
                self.pool.give_back(conn);
            }
            // otherwise `conn` is dropped here and shuts the stream down
        }
    }
}

impl NetworkConnectionStreaming {
    // The error type is huge, but defined by the openraft trait.
    #[allow(clippy::result_large_err)]
    #[inline(always)]
    async fn send<Err>(
        &mut self,
        req: RaftRequest,
        rx: oneshot::Receiver<Result<RaftStreamResponsePayload, Error>>,
    ) -> Result<RaftStreamResponsePayload, RPCError<NodeId, Node, Err>>
    where
        Err: std::error::Error + 'static + Clone,
    {
        tracing::debug!(
            req = debug(&req),
            "sending rpc request to {}",
            self.node.addr_raft
        );

        self.in_rpc = true;
        if let Err(err) = self.sender.send_async(req).await {
            self.healthy = false;
            error!(
                "NetworkConnectionStreaming::send to node {}: {}",
                self.node.id,
                err.to_string()
            );
            return Err(RPCError::Unreachable(Unreachable::new(&err)));
        }

        let res = rx.await;
        self.in_rpc = false;
        match res {
            Ok(Ok(payload)) => {
                // The stream leads to a dead Raft instance. Do not pool it, and make its
                // task reconnect: a long-lived client (a replication stream) keeps this stream
                // otherwise, and a fresh one reaches the instance that is running now.
                if payload_is_fatal(&payload) {
                    self.healthy = false;
                    let _ = self.sender.try_send(RaftRequest::ReaderExit);
                }
                Ok(payload)
            }
            Ok(Err(err)) => {
                // the stream broke under the request
                self.healthy = false;
                Err(RPCError::Unreachable(Unreachable::new(&err)))
            }
            Err(err) => {
                self.healthy = false;
                Err(RPCError::Unreachable(Unreachable::new(&err)))
            }
        }
    }
}

#[cfg(feature = "sqlite")]
impl RaftNetwork<TypeConfigSqlite> for NetworkConnectionStreaming {
    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfigSqlite>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self.send(RaftRequest::AppendDB((ack, req)), rx).await? {
            RaftStreamResponsePayload::AppendDB(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfigSqlite>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let (ack, rx) = oneshot::channel();
        match self.send(RaftRequest::SnapshotDB((ack, req)), rx).await? {
            RaftStreamResponsePayload::SnapshotDB(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self.send(RaftRequest::VoteDB((ack, req)), rx).await? {
            RaftStreamResponsePayload::VoteDB(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "cache")]
impl RaftNetwork<TypeConfigKV> for NetworkConnectionStreaming {
    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfigKV>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self.send(RaftRequest::AppendCache((ack, req)), rx).await? {
            RaftStreamResponsePayload::AppendCache(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfigKV>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let (ack, rx) = oneshot::channel();
        match self
            .send(RaftRequest::SnapshotCache((ack, req)), rx)
            .await?
        {
            RaftStreamResponsePayload::SnapshotCache(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug))]
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let (ack, rx) = oneshot::channel();
        match self.send(RaftRequest::VoteCache((ack, req)), rx).await? {
            RaftStreamResponsePayload::VoteCache(resp) => {
                resp.map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId, addr: &str) -> Node {
        Node {
            id,
            addr_raft: addr.to_string(),
            addr_api: format!("{addr}-api"),
        }
    }

    /// A live connection whose task just parks, like an idle `ws_handler`.
    fn conn(node: &Node) -> PooledConn {
        let (sender, rx) = flume::bounded::<RaftRequest>(1);
        let task = tokio::spawn(async move {
            while rx.recv_async().await.is_ok() {}
        });
        PooledConn {
            node: node.clone(),
            sender,
            task,
            idle_since: Instant::now(),
        }
    }

    fn idle_count(pool: &ConnPool, id: NodeId) -> usize {
        pool.lock().get(&id).map_or(0, |v| v.len())
    }

    fn client(pool: &ConnPool, conn: PooledConn) -> NetworkConnectionStreaming {
        NetworkConnectionStreaming {
            node: conn.node.clone(),
            sender: conn.sender.clone(),
            conn: Some(conn),
            pool: pool.clone(),
            in_rpc: false,
            healthy: true,
        }
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn a_fatal_answer_marks_the_stream_unusable() {
        use openraft::error::Fatal;

        let stopped = RaftStreamResponsePayload::VoteDB(Err(RaftError::Fatal(Fatal::Stopped)));
        assert!(payload_is_fatal(&stopped));

        let snapshot_stopped =
            RaftStreamResponsePayload::SnapshotDB(Err(RaftError::Fatal(Fatal::Stopped)));
        assert!(payload_is_fatal(&snapshot_stopped));

        let granted = RaftStreamResponsePayload::VoteDB(Ok(VoteResponse {
            vote: openraft::Vote::new(1, 1),
            vote_granted: true,
            last_log_id: None,
        }));
        assert!(!payload_is_fatal(&granted));
    }

    #[tokio::test]
    async fn empty_pool_hands_out_nothing() {
        assert!(ConnPool::default().take(&node(2, "b:8210")).is_none());
    }

    #[tokio::test]
    async fn returned_connection_is_handed_out_again() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        let c = conn(&n);
        let sender = c.sender.clone();
        pool.give_back(c);

        let again = pool.take(&n).expect("the idle connection is reused");
        assert!(again.sender.same_channel(&sender));
        assert!(pool.take(&n).is_none(), "a connection is handed out once");
    }

    #[tokio::test]
    async fn connection_to_a_moved_peer_is_not_reused() {
        let pool = ConnPool::default();
        pool.give_back(conn(&node(2, "old:8210")));
        assert!(pool.take(&node(2, "new:8210")).is_none());
    }

    #[tokio::test]
    async fn connections_of_other_peers_are_kept_apart() {
        let pool = ConnPool::default();
        pool.give_back(conn(&node(2, "b:8210")));
        assert!(pool.take(&node(3, "c:8210")).is_none());
        assert!(pool.take(&node(2, "b:8210")).is_some());
    }

    #[tokio::test]
    async fn idle_connections_are_capped_per_peer() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        for _ in 0..POOL_MAX_IDLE_PER_PEER + 3 {
            pool.give_back(conn(&n));
        }
        assert_eq!(idle_count(&pool, 2), POOL_MAX_IDLE_PER_PEER);
    }

    #[tokio::test]
    async fn expired_connection_is_not_reused() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        let mut c = conn(&n);
        c.idle_since = Instant::now()
            .checked_sub(POOL_IDLE_TTL + Duration::from_secs(1))
            .expect("monotonic clock far enough from its origin");
        pool.lock().entry(2).or_default().push(c);
        assert!(pool.take(&n).is_none());
    }

    #[tokio::test]
    async fn connection_whose_task_ended_is_not_reused() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        let c = conn(&n);
        c.task.abort();
        while !c.task.is_finished() {
            tokio::task::yield_now().await;
        }
        pool.lock().entry(2).or_default().push(c);
        assert!(pool.take(&n).is_none());
    }

    #[tokio::test]
    async fn healthy_idle_client_returns_its_connection() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        drop(client(&pool, conn(&n)));
        assert_eq!(idle_count(&pool, 2), 1);
    }

    #[tokio::test]
    async fn client_with_an_unanswered_request_does_not_return_its_connection() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        let mut c = client(&pool, conn(&n));
        c.in_rpc = true;
        drop(c);
        assert_eq!(idle_count(&pool, 2), 0);
    }

    #[tokio::test]
    async fn client_after_a_failed_request_does_not_return_its_connection() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        let mut c = client(&pool, conn(&n));
        c.healthy = false;
        drop(c);
        assert_eq!(idle_count(&pool, 2), 0);
    }

    #[tokio::test]
    async fn connection_that_is_not_returned_is_shut_down() {
        let pool = ConnPool::default();
        let n = node(2, "b:8210");
        let c = conn(&n);
        let sender = c.sender.clone();
        let mut cl = client(&pool, c);
        cl.healthy = false;
        drop(cl);
        // the task owns the receiver, so the channel disconnects once it is aborted
        while !sender.is_disconnected() {
            tokio::task::yield_now().await;
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn checkout_prefers_an_idle_connection_over_a_new_one() {
        let net = NetworkStreaming {
            node_id: 1,
            tls_config: None,
            secret_raft: b"test".to_vec(),
            raft_type: RaftType::Sqlite,
            heartbeat_interval: 500,
            // `ws_handler` parks without connecting while Raft is stopped and not yet started
            is_raft_stopped: Arc::new(AtomicBool::new(true)),
            is_startup_finished: Arc::new(AtomicBool::new(false)),
            pool: ConnPool::default(),
        };
        let n = node(2, "b:8210");

        let first = net.checkout(&n);
        let sender = first.sender.clone();
        drop(first);
        assert_eq!(idle_count(&net.pool, 2), 1);

        let second = net.checkout(&n);
        assert!(second.sender.same_channel(&sender), "the stream is reused");
        assert_eq!(idle_count(&net.pool, 2), 0);

        let third = net.checkout(&n);
        assert!(
            !third.sender.same_channel(&sender),
            "a stream in use is never shared"
        );
    }
}
