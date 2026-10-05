//! Core (non-persistent) publish/subscribe: messages on subjects, fanned out
//! in memory to the subscriptions live at publish time, at most once. Nothing
//! touches disk. Queue groups share a subject's messages (each goes to one
//! member). Request-reply is built on top: a request carries a `reply_to`
//! subject (an `_INBOX.…` the requester subscribed to) and a responder
//! publishes its answer there.
//!
//! Core messaging runs on the leader only, like writes: the bus is open
//! during a leadership tenure ([`CoreBus::serve`]) and ends every
//! subscription when the tenure ends, so clients reconnect to the new
//! leader. Core messages and stream records are separate: a stream publish
//! never reaches core subscribers.
//!
//! A subscriber that can't keep up (its connection's queue is full) loses
//! messages; they are counted (`exspeed_core_messages_dropped_total`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use exspeed_common::{Metrics, SubjectFilter};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Core subscription ids have the high bit set, so a connection can tell
/// them from consumer subscription ids.
pub const CORE_SUB_ID_BIT: u32 = 0x8000_0000;

/// A core message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMessage {
    pub subject: String,
    pub reply_to: Option<String>,
    pub headers: Vec<(String, String)>,
    pub value: Bytes,
    /// The connection that published it (0 = unknown), so a NATS client
    /// that connected with `echo: false` can skip its own messages.
    pub origin: u64,
}

/// What a connection receives for its core subscriptions.
#[derive(Debug, Clone)]
pub enum CoreEvent {
    Message {
        sub_id: u32,
        msg: Arc<CoreMessage>,
    },
    /// The subscription ended (leadership moved, server shutting down).
    Ended {
        sub_id: u32,
        code: u16,
        message: String,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BusError {
    #[error("not the leader; core messaging runs on the leader")]
    NotLeader,
    #[error("no subscribers for '{0}'")]
    NoResponders(String),
}

struct Sub {
    id: u32,
    filter: SubjectFilter,
    queue: Option<String>,
    tx: mpsc::Sender<CoreEvent>,
}

/// The in-memory subscription table.
pub struct CoreBus {
    subs: RwLock<Vec<Sub>>,
    next_id: AtomicU32,
    /// Picks the member of a queue group that gets a message.
    rr: AtomicU64,
    open: AtomicBool,
    metrics: Arc<Metrics>,
}

impl CoreBus {
    pub fn new(metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            subs: RwLock::new(Vec::new()),
            next_id: AtomicU32::new(1),
            rr: AtomicU64::new(0),
            open: AtomicBool::new(false),
            metrics,
        })
    }

    /// Serve core messaging for one leadership tenure: open now, and when
    /// `token` is cancelled close and end every subscription.
    pub fn serve(self: &Arc<Self>, token: CancellationToken) {
        self.open.store(true, Ordering::Release);
        let this = self.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            this.close("leadership moved; reconnect to the leader");
        });
    }

    /// Stop serving and end every subscription.
    pub fn close(&self, why: &str) {
        self.open.store(false, Ordering::Release);
        let subs = std::mem::take(&mut *self.subs.write().unwrap());
        for s in subs {
            let _ = s.tx.try_send(CoreEvent::Ended {
                sub_id: s.id,
                code: exspeed_protocol::client::code::UNAVAILABLE,
                message: why.to_string(),
            });
        }
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Subscribe `tx` to subjects matching `filter`; with a `queue` group,
    /// each message goes to one member of the group.
    pub fn subscribe(
        &self,
        filter: SubjectFilter,
        queue: Option<String>,
        tx: mpsc::Sender<CoreEvent>,
    ) -> Result<u32, BusError> {
        if !self.is_open() {
            return Err(BusError::NotLeader);
        }
        let id =
            CORE_SUB_ID_BIT | (self.next_id.fetch_add(1, Ordering::Relaxed) & !CORE_SUB_ID_BIT);
        self.subs.write().unwrap().push(Sub {
            id,
            filter,
            queue: queue.filter(|q| !q.is_empty()),
            tx,
        });
        Ok(id)
    }

    /// Remove a subscription. Returns whether it existed.
    pub fn unsubscribe(&self, id: u32) -> bool {
        let mut subs = self.subs.write().unwrap();
        let before = subs.len();
        subs.retain(|s| s.id != id);
        subs.len() != before
    }

    /// Publish to every matching subscription (one member per queue group).
    /// Returns how many subscriptions received it. A request (`reply_to`
    /// set) that nobody receives fails with [`BusError::NoResponders`].
    pub fn publish(&self, msg: CoreMessage) -> Result<usize, BusError> {
        if !self.is_open() {
            return Err(BusError::NotLeader);
        }
        let msg = Arc::new(msg);
        let subs = self.subs.read().unwrap();
        let mut groups: HashMap<&str, Vec<&Sub>> = HashMap::new();
        let mut targets: Vec<&Sub> = Vec::new();
        for s in subs.iter().filter(|s| s.filter.matches(&msg.subject)) {
            match &s.queue {
                Some(q) => groups.entry(q.as_str()).or_default().push(s),
                None => targets.push(s),
            }
        }
        for members in groups.values() {
            let i = self.rr.fetch_add(1, Ordering::Relaxed) as usize % members.len();
            targets.push(members[i]);
        }
        let mut delivered = 0;
        for s in targets {
            let ev = CoreEvent::Message {
                sub_id: s.id,
                msg: msg.clone(),
            };
            match s.tx.try_send(ev) {
                Ok(()) => delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.metrics.record_core_dropped();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        self.metrics.record_core_published(delivered as u64);
        if delivered == 0 && msg.reply_to.is_some() {
            return Err(BusError::NoResponders(msg.subject.clone()));
        }
        Ok(delivered)
    }

    /// The subject filter of subscription `id`, if it exists.
    pub fn filter_of(&self, id: u32) -> Option<SubjectFilter> {
        self.subs
            .read()
            .unwrap()
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.filter.clone())
    }

    /// Number of live subscriptions.
    pub fn len(&self) -> usize {
        self.subs.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(subject: &str) -> CoreMessage {
        CoreMessage {
            subject: subject.into(),
            reply_to: None,
            headers: vec![],
            value: Bytes::from_static(b"x"),
            origin: 0,
        }
    }

    fn f(s: &str) -> SubjectFilter {
        SubjectFilter::parse(s).unwrap()
    }

    fn open_bus() -> Arc<CoreBus> {
        let bus = CoreBus::new(Arc::new(Metrics::new().0));
        bus.open.store(true, Ordering::Release);
        bus
    }

    fn drain(rx: &mut mpsc::Receiver<CoreEvent>) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(CoreEvent::Message { msg, .. }) = rx.try_recv() {
            out.push(msg.subject.clone());
        }
        out
    }

    #[test]
    fn fans_out_to_matching_subscriptions() {
        let bus = open_bus();
        let (t1, mut r1) = mpsc::channel(16);
        let (t2, mut r2) = mpsc::channel(16);
        let a = bus.subscribe(f("orders.*"), None, t1).unwrap();
        assert!(a & CORE_SUB_ID_BIT != 0);
        bus.subscribe(f("orders.eu"), None, t2).unwrap();
        assert_eq!(bus.publish(msg("orders.eu")).unwrap(), 2);
        assert_eq!(bus.publish(msg("orders.us")).unwrap(), 1);
        assert_eq!(bus.publish(msg("billing.x")).unwrap(), 0);
        assert_eq!(drain(&mut r1), ["orders.eu", "orders.us"]);
        assert_eq!(drain(&mut r2), ["orders.eu"]);
        assert!(bus.unsubscribe(a));
        assert_eq!(bus.publish(msg("orders.us")).unwrap(), 0);
    }

    #[test]
    fn a_queue_group_shares_messages() {
        let bus = open_bus();
        let (t1, mut r1) = mpsc::channel(64);
        let (t2, mut r2) = mpsc::channel(64);
        let (t3, mut r3) = mpsc::channel(64);
        bus.subscribe(f("jobs"), Some("workers".into()), t1)
            .unwrap();
        bus.subscribe(f("jobs"), Some("workers".into()), t2)
            .unwrap();
        bus.subscribe(f("jobs"), None, t3).unwrap();
        for _ in 0..10 {
            assert_eq!(bus.publish(msg("jobs")).unwrap(), 2);
        }
        let (n1, n2) = (drain(&mut r1).len(), drain(&mut r2).len());
        assert_eq!(n1 + n2, 10, "each message reaches one group member");
        assert!(n1 > 0 && n2 > 0, "round-robin across members");
        assert_eq!(drain(&mut r3).len(), 10, "a plain subscriber gets all");
    }

    #[test]
    fn requests_without_responders_fail() {
        let bus = open_bus();
        let mut m = msg("svc.echo");
        m.reply_to = Some("_INBOX.a.1".into());
        assert_eq!(
            bus.publish(m),
            Err(BusError::NoResponders("svc.echo".into()))
        );
    }

    #[test]
    fn a_slow_subscriber_loses_messages_but_others_dont() {
        let bus = open_bus();
        let (slow, _keep) = mpsc::channel(1);
        let (fast, mut rf) = mpsc::channel(16);
        bus.subscribe(f(">"), None, slow).unwrap();
        bus.subscribe(f(">"), None, fast).unwrap();
        for _ in 0..3 {
            bus.publish(msg("a")).unwrap();
        }
        assert_eq!(drain(&mut rf).len(), 3);
    }

    #[tokio::test]
    async fn closing_ends_subscriptions_and_refuses_work() {
        let bus = CoreBus::new(Arc::new(Metrics::new().0));
        assert_eq!(bus.publish(msg("a")), Err(BusError::NotLeader));
        let token = CancellationToken::new();
        bus.serve(token.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let id = bus.subscribe(f("a"), None, tx).unwrap();
        token.cancel();
        match rx.recv().await {
            Some(CoreEvent::Ended { sub_id, code, .. }) => {
                assert_eq!(sub_id, id);
                assert_eq!(code, 503);
            }
            other => panic!("{other:?}"),
        }
        assert!(!bus.is_open());
        assert!(bus.is_empty());
    }
}
