//! Leader side of replication: accept follower connections on the cluster
//! port and answer their fetches.
//!
//! A fetch carries the follower's position in every stream it has. For each
//! stream the leader checks the position against its epoch history: a
//! follower beyond the end of its last epoch in the leader's log is told to
//! truncate; otherwise it gets the records it is missing (bounded by
//! `max_bytes`, streams served round-robin). The full stream list and
//! configs are included whenever the follower's metadata version is stale or
//! it disagrees with the leader about which streams exist. With nothing to
//! send, the fetch waits (long poll) for an append or a metadata change.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use exspeed_common::auth::Action;
use exspeed_common::Offset;
use exspeed_streams::ReadLimits;

use super::tracker::ReplicaTracker;
use super::wire::{
    code, read_msg, write_msg, FetchRequest, FetchResponse, Msg, StreamAction, StreamData,
    StreamMeta, WireRecord, PROTOCOL_VERSION,
};
use super::Cluster;

/// Longest a fetch may wait for new data.
const MAX_WAIT: Duration = Duration::from_secs(10);

pub(crate) async fn serve(cluster: Arc<Cluster>, listener: TcpListener, cancel: CancellationToken) {
    loop {
        let (socket, peer) = tokio::select! {
            _ = cancel.cancelled() => return,
            r = listener.accept() => match r {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "cluster listener accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let _ = socket.set_nodelay(true);
        let cluster = cluster.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let res = match cluster.cfg.tls.as_ref() {
                Some(tls) => {
                    let acceptor = tokio_rustls::TlsAcceptor::from(tls.server.clone());
                    match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(socket))
                        .await
                    {
                        Ok(Ok(stream)) => connection(&cluster, stream, peer, &cancel).await,
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "TLS handshake timed out",
                        )),
                    }
                }
                None => connection(&cluster, socket, peer, &cancel).await,
            };
            if let Err(e) = res {
                debug!(%peer, error = %e, "replication connection closed");
            }
        });
    }
}

async fn connection<S: AsyncRead + AsyncWrite + Unpin>(
    cluster: &Arc<Cluster>,
    socket: S,
    peer: SocketAddr,
    cancel: &CancellationToken,
) -> std::io::Result<()> {
    let (rd, mut wr) = tokio::io::split(socket);
    let mut rd = BufReader::new(rd);
    let hello = tokio::time::timeout(Duration::from_secs(10), read_msg(&mut rd))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no hello"))??;
    let follower = match hello {
        Msg::Hello {
            protocol,
            node_id,
            token,
        } => {
            if protocol != PROTOCOL_VERSION {
                let msg = format!("unsupported replication protocol {protocol}");
                write_msg(&mut wr, &error(code::BAD_REQUEST, &msg)).await?;
                return Ok(());
            }
            if !authorized(cluster, token.as_deref()) {
                cluster
                    .metrics
                    .auth_denied("unauthorized", "cluster", "replicate");
                write_msg(&mut wr, &error(code::UNAUTHORIZED, "unauthorized")).await?;
                return Ok(());
            }
            node_id
        }
        _ => {
            write_msg(&mut wr, &error(code::BAD_REQUEST, "expected hello")).await?;
            return Ok(());
        }
    };
    let epoch = cluster.tracker().map_or(0, |t| t.epoch());
    write_msg(
        &mut wr,
        &Msg::HelloOk {
            node_id: cluster.node_id().to_string(),
            epoch,
        },
    )
    .await?;
    info!(%peer, follower = %follower, "follower connected");

    let mut round = 0usize;
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            m = read_msg(&mut rd) => m?,
        };
        let reply = match msg {
            Msg::Fetch(req) => {
                round = round.wrapping_add(1);
                match fetch(cluster, &follower, req, round, cancel).await {
                    Ok(resp) => Msg::FetchOk(resp),
                    Err(e) => e,
                }
            }
            _ => error(code::BAD_REQUEST, "expected fetch"),
        };
        let fatal = matches!(reply, Msg::Error { .. });
        write_msg(&mut wr, &reply).await?;
        if fatal {
            return Ok(());
        }
    }
}

fn error(code: u16, message: &str) -> Msg {
    Msg::Error {
        code,
        message: message.to_string(),
    }
}

fn authorized(cluster: &Cluster, token: Option<&str>) -> bool {
    let Some(store) = cluster.credentials.as_ref() else {
        return true; // auth disabled
    };
    let Some(token) = token else {
        return false;
    };
    let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    store.lookup(&digest).is_some_and(|id| {
        id.permissions
            .iter()
            .any(|p| p.actions.contains(Action::Replicate))
    })
}

/// `stream -> next offset` for every stream on this node.
pub(crate) async fn high_watermarks(cluster: &Cluster) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    for s in cluster.storage.list_streams().await.unwrap_or_default() {
        if let Ok((_, next)) = cluster.storage.stream_bounds(&s).await {
            out.insert(s.as_str().to_string(), next.0);
        }
    }
    out
}

async fn fetch(
    cluster: &Arc<Cluster>,
    follower: &str,
    req: FetchRequest,
    round: usize,
    cancel: &CancellationToken,
) -> Result<FetchResponse, Msg> {
    let Some(tracker) = cluster.tracker() else {
        return Err(error(code::NOT_LEADER, "not the leader"));
    };
    if req.epoch > tracker.epoch() {
        return Err(error(
            code::FENCED,
            &format!(
                "this node's epoch {} is older than {}: it is no longer the leader",
                tracker.epoch(),
                req.epoch
            ),
        ));
    }
    let deadline = Instant::now() + Duration::from_millis(req.max_wait_ms as u64).min(MAX_WAIT);
    let mut appends = cluster.log.watch_appends();
    let mut meta = cluster.log.watch_metadata();

    // Record progress once per fetch (capped where the follower diverged).
    let progress = progress(cluster, &req).await;
    tracker.on_fetch(follower, &progress);

    loop {
        appends.borrow_and_update();
        meta.borrow_and_update();
        if tracker.is_closed() {
            return Err(error(code::NOT_LEADER, "not the leader"));
        }
        let (resp, hws, has_data) = build(cluster, &tracker, &req, round).await;
        if has_data || Instant::now() >= deadline {
            tracker.on_response(follower, hws, Instant::now());
            let bytes: usize = resp
                .streams
                .iter()
                .map(|s| match &s.action {
                    StreamAction::Records(rs) => rs.iter().map(WireRecord::size).sum(),
                    _ => 0,
                })
                .sum();
            cluster
                .metrics
                .replication_bytes_total
                .add(bytes as u64, &[KeyValue::new("direction", "out")]);
            return Ok(resp);
        }
        tokio::select! {
            _ = appends.changed() => {}
            _ = meta.changed() => {}
            _ = tokio::time::sleep_until(deadline) => {}
            _ = tokio::time::sleep(Duration::from_millis(250)) => {} // notice closure
            _ = cancel.cancelled() => return Err(error(code::NOT_LEADER, "shutting down")),
        }
    }
}

async fn progress(cluster: &Cluster, req: &FetchRequest) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    for p in &req.positions {
        let Some(h) = cluster.epochs.get(&p.stream) else {
            continue;
        };
        if h.uid != p.uid {
            continue;
        }
        let Ok(name) = exspeed_common::StreamName::try_from(p.stream.as_str()) else {
            continue;
        };
        let Ok((_, next)) = cluster.storage.stream_bounds(&name).await else {
            continue;
        };
        out.insert(
            p.stream.clone(),
            p.next.min(h.end_offset(p.last_epoch, next.0)),
        );
    }
    out
}

/// Build a response. Returns it, the high watermarks it reflects, and
/// whether it carries anything worth returning early for.
async fn build(
    cluster: &Cluster,
    tracker: &ReplicaTracker,
    req: &FetchRequest,
    round: usize,
) -> (FetchResponse, HashMap<String, u64>, bool) {
    let version = (tracker.epoch() << 32) | (cluster.log.metadata_counter() & 0xffff_ffff);
    let mut streams = cluster.storage.list_streams().await.unwrap_or_default();
    streams.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let positions: HashMap<&str, &super::wire::FetchPos> = req
        .positions
        .iter()
        .map(|p| (p.stream.as_str(), p))
        .collect();
    let leader_names: HashSet<&str> = streams.iter().map(|s| s.as_str()).collect();
    let mut need_meta = req.metadata_version != version
        || req
            .positions
            .iter()
            .any(|p| !leader_names.contains(p.stream.as_str()));

    let mut hws = HashMap::new();
    let mut data = Vec::new();
    let mut budget = req.max_bytes.max(1) as usize;
    let mut has_data = false;
    let n = streams.len();
    for i in 0..n {
        let s = &streams[(i + round) % n.max(1)];
        let Ok((earliest, next)) = cluster.storage.stream_bounds(s).await else {
            continue;
        };
        hws.insert(s.as_str().to_string(), next.0);
        let Ok(hist) = cluster.epochs.get_or_create(s.as_str()) else {
            continue;
        };
        let Some(p) = positions.get(s.as_str()) else {
            need_meta = true;
            continue;
        };
        if p.uid != hist.uid {
            need_meta = true;
            continue;
        }
        let end = hist.end_offset(p.last_epoch, next.0);
        let action = if p.next > end {
            Some(StreamAction::Truncate(end))
        } else if p.next < next.0 && budget > 0 {
            match cluster
                .storage
                .read_batch(
                    s,
                    Offset(p.next),
                    ReadLimits {
                        max_records: 10_000,
                        max_bytes: budget,
                    },
                )
                .await
            {
                Ok(batch) if !batch.records.is_empty() => {
                    let records: Vec<WireRecord> =
                        batch.records.into_iter().map(WireRecord::from).collect();
                    let size: usize = records.iter().map(WireRecord::size).sum();
                    budget = budget.saturating_sub(size);
                    Some(StreamAction::Records(records))
                }
                Ok(_) => None,
                Err(e) => {
                    warn!(stream = %s, error = %e, "replication read failed");
                    None
                }
            }
        } else if p.earliest < earliest.0 {
            Some(StreamAction::Trim)
        } else {
            None
        };
        if let Some(action) = action {
            has_data |= !matches!(action, StreamAction::Trim);
            data.push(StreamData {
                stream: s.as_str().to_string(),
                action,
                earliest: earliest.0,
                high_watermark: next.0,
                epochs: hist.epochs.clone(),
            });
        }
    }

    let metadata = if need_meta {
        let mut metas = Vec::with_capacity(n);
        for s in &streams {
            let (Ok(cfg), Ok(hist)) = (
                cluster.storage.stream_config(s).await,
                cluster.epochs.get_or_create(s.as_str()),
            ) else {
                continue;
            };
            metas.push(StreamMeta {
                name: s.as_str().to_string(),
                uid: hist.uid,
                config_json: serde_json::to_string(&cfg).unwrap_or_else(|_| "{}".into()),
            });
        }
        has_data = true;
        Some(metas)
    } else {
        None
    };

    (
        FetchResponse {
            epoch: tracker.epoch(),
            metadata_version: version,
            metadata,
            streams: data,
        },
        hws,
        has_data,
    )
}
