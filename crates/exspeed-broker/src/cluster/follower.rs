//! Follower side of replication: find the leader through the lease, connect
//! to its cluster port and keep fetching.
//!
//! Applying a response, in order:
//! 1. **Metadata** (when present): create missing streams, recreate streams
//!    whose uid changed (deleted and recreated on the leader), apply config
//!    changes, delete streams the leader no longer has.
//! 2. **Per stream**: truncate a divergent suffix, or append the records
//!    with their original offsets, timestamps, keys and headers
//!    (`StorageEngine::append_at`); trim up to the leader's earliest offset;
//!    adopt the leader's epoch history for the part of the log we now have.
//!
//! The follower writes to storage directly: it is the one writer that must
//! not go through `Log` (whose leader gate rejects it by design).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use exspeed_common::{Offset, StreamName};
use exspeed_streams::{StorageError, StoredRecord, StreamConfig};

use super::epochs::StreamEpochs;
use super::wire::{
    read_msg, write_msg, FetchPos, FetchRequest, FetchResponse, Msg, StreamAction, StreamMeta,
    PROTOCOL_VERSION,
};
use super::Cluster;
use crate::lease::{LeaseRecord, CLUSTER_LEASE};

pub(crate) async fn run(cluster: Arc<Cluster>, cancel: CancellationToken) {
    let mut backoff = Duration::from_millis(100);
    let mut metadata_version = 0u64;
    info!("following the cluster leader");
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let leader =
            match tokio::time::timeout(Duration::from_secs(5), cluster.lease.get(CLUSTER_LEASE))
                .await
            {
                Ok(Ok(Some(r)))
                    if r.is_live()
                        && r.holder != cluster.cfg.node_id
                        && r.replication_endpoint.is_some() =>
                {
                    Some(r)
                }
                _ => None,
            };
        let Some(leader) = leader else {
            cluster.follower_info.lock().connected = false;
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
            }
        };
        match session(&cluster, &leader, &mut metadata_version, &cancel).await {
            Ok(()) => backoff = Duration::from_millis(100),
            Err(e) => {
                warn!(leader = %leader.holder, error = %e, "replication session failed");
                let mut info = cluster.follower_info.lock();
                info.connected = false;
                info.last_error = Some(e);
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}

async fn session(
    cluster: &Arc<Cluster>,
    leader: &LeaseRecord,
    metadata_version: &mut u64,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let endpoint = leader.replication_endpoint.as_deref().unwrap_or_default();
    let socket = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("connect to {endpoint} timed out"))
        .and_then(|r| r.map_err(|e| format!("connect to {endpoint}: {e}")));
    cluster
        .metrics
        .record_replication_connect_attempt(socket.is_ok());
    let socket = socket?;
    let _ = socket.set_nodelay(true);
    match cluster.cfg.tls.as_ref() {
        Some(tls) => {
            let host = endpoint
                .rsplit_once(':')
                .map_or(endpoint, |(h, _)| h)
                .trim_start_matches('[')
                .trim_end_matches(']');
            let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| format!("bad TLS server name {host:?}: {e}"))?;
            let stream = tokio::time::timeout(
                Duration::from_secs(10),
                tokio_rustls::TlsConnector::from(tls.client.clone()).connect(name, socket),
            )
            .await
            .map_err(|_| format!("TLS handshake with {endpoint} timed out"))?
            .map_err(|e| format!("TLS handshake with {endpoint}: {e}"))?;
            replicate(cluster, leader, endpoint, stream, metadata_version, cancel).await
        }
        None => replicate(cluster, leader, endpoint, socket, metadata_version, cancel).await,
    }
}

async fn replicate<S: AsyncRead + AsyncWrite + Unpin>(
    cluster: &Arc<Cluster>,
    leader: &LeaseRecord,
    endpoint: &str,
    stream: S,
    metadata_version: &mut u64,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let (rd, mut wr) = tokio::io::split(stream);
    let mut rd = BufReader::new(rd);
    write_msg(
        &mut wr,
        &Msg::Hello {
            protocol: PROTOCOL_VERSION,
            node_id: cluster.cfg.node_id.clone(),
            token: cluster.cfg.replicator_token.clone(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    match tokio::time::timeout(Duration::from_secs(10), read_msg(&mut rd)).await {
        Ok(Ok(Msg::HelloOk { node_id, epoch })) => {
            if node_id != leader.holder {
                return Err(format!(
                    "{endpoint} is node {node_id}, not the leader {}",
                    leader.holder
                ));
            }
            if epoch < leader.epoch {
                return Err(format!(
                    "{endpoint} is not leading epoch {} yet (at {epoch})",
                    leader.epoch
                ));
            }
        }
        Ok(Ok(Msg::Error { code, message })) => return Err(format!("{code}: {message}")),
        Ok(Ok(other)) => return Err(format!("unexpected reply {other:?}")),
        Ok(Err(e)) => return Err(e.to_string()),
        Err(_) => return Err("hello timed out".into()),
    }
    {
        let mut info = cluster.follower_info.lock();
        info.leader = Some(leader.holder.clone());
        info.leader_epoch = leader.epoch;
        info.connected = true;
        info.last_error = None;
    }
    info!(leader = %leader.holder, epoch = leader.epoch, %endpoint, "replicating from the leader");

    let max_wait = cluster.cfg.fetch_max_wait;
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        // A newer leader was elected: reconnect.
        if let Some(l) = cluster.leadership() {
            if l.lease_record().is_some_and(|r| r.epoch > leader.epoch) {
                return Ok(());
            }
        }
        let positions = positions(cluster).await?;
        let req = FetchRequest {
            epoch: leader.epoch,
            metadata_version: *metadata_version,
            positions,
            max_wait_ms: max_wait.as_millis() as u32,
            max_bytes: cluster.cfg.fetch_max_bytes,
        };
        write_msg(&mut wr, &Msg::Fetch(req))
            .await
            .map_err(|e| e.to_string())?;
        let reply = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            r = tokio::time::timeout(max_wait + Duration::from_secs(10), read_msg(&mut rd)) => r,
        };
        match reply {
            Ok(Ok(Msg::FetchOk(resp))) => {
                if resp.epoch < leader.epoch {
                    return Err(format!("stale leader response (epoch {})", resp.epoch));
                }
                // Applying is never interrupted half-way.
                apply(cluster, resp, metadata_version).await?;
            }
            Ok(Ok(Msg::Error { code, message })) => return Err(format!("{code}: {message}")),
            Ok(Ok(other)) => return Err(format!("unexpected reply {other:?}")),
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => return Err("fetch timed out".into()),
        }
    }
}

async fn positions(cluster: &Cluster) -> Result<Vec<FetchPos>, String> {
    let streams = cluster
        .storage
        .list_streams()
        .await
        .map_err(|e| format!("list streams: {e}"))?;
    let mut out = Vec::with_capacity(streams.len());
    for s in streams {
        let Ok((earliest, next)) = cluster.storage.stream_bounds(&s).await else {
            continue;
        };
        let h = cluster.epochs.get(s.as_str());
        out.push(FetchPos {
            stream: s.as_str().to_string(),
            uid: h.as_ref().map_or(0, |h| h.uid),
            next: next.0,
            last_epoch: h.as_ref().map_or(0, |h| h.last_epoch(next.0)),
            earliest: earliest.0,
        });
    }
    Ok(out)
}

fn name(s: &str) -> Result<StreamName, String> {
    StreamName::try_from(s).map_err(|e| format!("bad stream name {s:?}: {e}"))
}

async fn apply(
    cluster: &Cluster,
    resp: FetchResponse,
    metadata_version: &mut u64,
) -> Result<(), String> {
    if let Some(metas) = &resp.metadata {
        reconcile(cluster, metas).await?;
    }
    *metadata_version = resp.metadata_version;
    let storage = &cluster.storage;
    let mut lag = 0u64;
    for sd in resp.streams {
        let stream = name(&sd.stream)?;
        let (_, next) = match storage.stream_bounds(&stream).await {
            Ok(b) => b,
            Err(StorageError::StreamNotFound(_)) => continue, // metadata comes next round
            Err(e) => return Err(format!("bounds of {stream}: {e}")),
        };
        match sd.action {
            StreamAction::Truncate(to) => {
                warn!(%stream, from = to, to = next.0, "truncating records that diverged from the leader");
                storage
                    .truncate_from(&stream, Offset(to))
                    .await
                    .map_err(|e| format!("truncate {stream}: {e}"))?;
                cluster
                    .metrics
                    .inc_replication_truncated_records(stream.as_str(), next.0.saturating_sub(to));
            }
            StreamAction::Records(records) => {
                let records: Vec<StoredRecord> = records
                    .into_iter()
                    .filter(|r| r.offset >= next.0)
                    .map(StoredRecord::from)
                    .collect();
                let n = records.len() as u64;
                if n > 0 {
                    if let Err(e) = storage.append_at(&stream, records).await {
                        cluster.metrics.replication_apply_errors_total.add(1, &[]);
                        return Err(format!("append to {stream}: {e}"));
                    }
                    cluster
                        .metrics
                        .inc_replication_records_applied(stream.as_str(), n);
                }
            }
            StreamAction::Trim => {}
        }
        let (earliest, next) = storage
            .stream_bounds(&stream)
            .await
            .map_err(|e| format!("bounds of {stream}: {e}"))?;
        if earliest.0 < sd.earliest && sd.earliest <= next.0 {
            storage
                .trim_up_to(&stream, Offset(sd.earliest))
                .await
                .map_err(|e| format!("trim {stream}: {e}"))?;
        }
        cluster
            .epochs
            .update(stream.as_str(), |h| h.adopt(&sd.epochs, next.0))
            .map_err(|e| format!("epoch history of {stream}: {e}"))?;
        lag += sd.high_watermark.saturating_sub(next.0);
    }
    cluster.follower_info.lock().lag_records = lag;
    cluster.metrics.replication_lag_records.record(
        lag as i64,
        &[KeyValue::new("follower_id", cluster.cfg.node_id.clone())],
    );
    Ok(())
}

async fn reconcile(cluster: &Cluster, metas: &[StreamMeta]) -> Result<(), String> {
    let storage = &cluster.storage;
    let local: HashSet<String> = storage
        .list_streams()
        .await
        .map_err(|e| format!("list streams: {e}"))?
        .into_iter()
        .map(|s| s.as_str().to_string())
        .collect();
    let mut wanted = HashSet::new();
    for m in metas {
        wanted.insert(m.name.clone());
        let stream = name(&m.name)?;
        let cfg: StreamConfig =
            serde_json::from_str(&m.config_json).map_err(|e| format!("config of {stream}: {e}"))?;
        let mut exists = local.contains(&m.name);
        let known = cluster.epochs.get(&m.name);
        if exists && known.as_ref().is_some_and(|h| h.uid != m.uid) {
            info!(%stream, "stream was recreated on the leader; dropping the local copy");
            delete(cluster, &stream).await?;
            exists = false;
        }
        if !exists {
            debug!(%stream, "creating replicated stream");
            match storage.create_stream_with(&stream, &cfg).await {
                Ok(()) | Err(StorageError::StreamAlreadyExists(_)) => {}
                Err(e) => return Err(format!("create {stream}: {e}")),
            }
            cluster
                .epochs
                .put(&m.name, StreamEpochs::new(m.uid))
                .map_err(|e| e.to_string())?;
            continue;
        }
        if known.is_none() {
            cluster
                .epochs
                .put(&m.name, StreamEpochs::new(m.uid))
                .map_err(|e| e.to_string())?;
        }
        if storage.stream_config(&stream).await.ok().as_ref() != Some(&cfg) {
            storage
                .update_stream_config(&stream, &cfg)
                .await
                .map_err(|e| format!("configure {stream}: {e}"))?;
        }
    }
    for s in local.difference(&wanted) {
        let stream = name(s)?;
        info!(%stream, "stream was deleted on the leader");
        delete(cluster, &stream).await?;
    }
    Ok(())
}

async fn delete(cluster: &Cluster, stream: &StreamName) -> Result<(), String> {
    cluster
        .storage
        .delete_stream(stream)
        .await
        .map_err(|e| format!("delete {stream}: {e}"))?;
    cluster.epochs.remove(stream.as_str());
    cluster.log.dedup().forget_stream(stream).await;
    Ok(())
}
