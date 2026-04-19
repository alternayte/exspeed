use std::sync::Arc;
use std::time::Duration;

use exspeed_common::Metrics;
use tokio::time;

use crate::broker::Broker;

/// The hardcoded per-subscription channel size in [`Broker::subscribe`].
/// Keep this in sync with the `mpsc::channel(N)` literal there.
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 1000;

/// Spawn a background task that samples each active subscription's
/// delivery-channel fill ratio every 5 seconds and reports it to
/// `metrics.subscription_queue_fill_ratio`.
pub fn spawn_queue_depth_sampler(broker: Arc<Broker>, metrics: Arc<Metrics>) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            sample_once(&broker, &metrics);
        }
    });
}

fn sample_once(broker: &Broker, metrics: &Metrics) {
    let consumers = match broker.consumers.read() {
        Ok(c) => c,
        Err(_) => return, // poisoned lock — skip this tick
    };

    for (consumer_name, state) in consumers.iter() {
        for (subscriber_id, sub) in state.subscribers.iter() {
            let remaining = sub.delivery_tx.capacity();
            let used = SUBSCRIPTION_CHANNEL_CAPACITY.saturating_sub(remaining);
            let ratio = used as f64 / SUBSCRIPTION_CHANNEL_CAPACITY as f64;
            metrics.set_subscription_queue_fill(consumer_name, subscriber_id, ratio);
        }
    }
}
