//! Feature-gated cumulative metrics for physical memcached access.

use std::sync::OnceLock;
use std::time::Instant;

use brz_metrics::{MetricType, ResourceMetric, TimingMetric};

#[derive(Clone, Copy)]
pub(crate) enum Direction {
    Up,
    Down,
}

/// Metrics shared by every physical node in one cache-service topology.
///
/// CacheService guarantees that all nodes for one business group use the
/// same port, so names are derived once from the first configured endpoint.
pub(crate) struct ProfileMetrics {
    port_name: Box<str>,
    up_name: Box<str>,
    down_name: Box<str>,
    aggregate: OnceLock<TimingMetric>,
    up: OnceLock<TimingMetric>,
    down: OnceLock<TimingMetric>,
}

impl ProfileMetrics {
    pub(crate) fn new(port: u16) -> Self {
        Self {
            port_name: port.to_string().into_boxed_str(),
            up_name: format!("{port}_up").into_boxed_str(),
            down_name: format!("{port}_down").into_boxed_str(),
            aggregate: OnceLock::new(),
            up: OnceLock::new(),
            down: OnceLock::new(),
        }
    }

    /// Starts one physical access without allocation or address lookup.
    #[inline]
    pub(crate) fn attempt(&self, direction: Direction) -> ProfileAttempt {
        let aggregate = *self.aggregate.get_or_init(|| {
            TimingMetric::new(MetricType::Memcache(ResourceMetric::Req), &self.port_name)
        });
        let detail = match direction {
            Direction::Up => *self
                .up
                .get_or_init(|| TimingMetric::new(MetricType::McDetail, &self.up_name)),
            Direction::Down => *self
                .down
                .get_or_init(|| TimingMetric::new(MetricType::McDetail, &self.down_name)),
        };
        ProfileAttempt {
            metrics: [aggregate, detail],
            started: Instant::now(),
            finished: false,
        }
    }
}

/// Completion state retained by the protocol until the physical response is
/// decoded. This also covers fire-and-forget fanout whose response future is
/// intentionally dropped by the caller.
#[derive(Debug)]
pub(crate) struct ProfileAttempt {
    metrics: [TimingMetric; 2],
    started: Instant,
    finished: bool,
}

impl ProfileAttempt {
    #[inline]
    pub(crate) fn finish(&mut self, success: bool) {
        let elapsed = self.started.elapsed();
        for metric in self.metrics {
            metric.record(elapsed, success);
        }
        self.finished = true;
    }
}

impl Drop for ProfileAttempt {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(false);
        }
    }
}

#[cfg(feature = "metrics")]
pub(crate) struct ConnectionObserver {
    metric: brz_metrics::ConnMetric,
    active: std::sync::Mutex<Option<brz_metrics::ConnectionGuard>>,
}

#[cfg(feature = "metrics")]
impl ConnectionObserver {
    pub(crate) fn new(port: u16) -> Self {
        Self {
            metric: brz_metrics::ConnMetric::new(
                brz_metrics::MetricType::Memcache(brz_metrics::ResourceMetric::Conn),
                &port.to_string(),
            ),
            active: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(feature = "metrics")]
impl brz_net::NodeConnectionObserver for ConnectionObserver {
    fn on_connection_state_change(&self, connected: bool) {
        let mut active = self
            .active
            .lock()
            .expect("connection metrics mutex poisoned");
        if connected {
            if active.is_none() {
                *active = Some(self.metric.connection());
            }
        } else {
            active.take();
        }
    }
}
