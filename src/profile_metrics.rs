//! Feature-gated cumulative metrics for physical memcached access.

use std::time::Instant;

use brz_metrics::{MetricAccess, MetricType, ResourceLabels, ResourceMetric, TimingMetric};

#[derive(Clone, Copy)]
pub(crate) enum Direction {
    Up,
    Down,
}

/// Eagerly registered handles for one business resource and physical endpoint.
#[derive(Clone, Copy)]
pub(crate) struct ProfileMetrics {
    read: TimingMetric,
    write: TimingMetric,
}

impl ProfileMetrics {
    pub(crate) fn new(labels: &ResourceLabels) -> Self {
        let kind = MetricType::Memcache(ResourceMetric::Req);
        Self {
            read: TimingMetric::new_resource(kind, &labels.clone().with_access(MetricAccess::Read)),
            write: TimingMetric::new_resource(
                kind,
                &labels.clone().with_access(MetricAccess::Write),
            ),
        }
    }

    #[inline]
    pub(crate) fn attempt(&self, direction: Direction) -> ProfileAttempt {
        ProfileAttempt {
            metric: match direction {
                Direction::Up => self.write,
                Direction::Down => self.read,
            },
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
    metric: TimingMetric,
    started: Instant,
    finished: bool,
}

impl ProfileAttempt {
    #[inline]
    pub(crate) fn finish(&mut self, success: bool) {
        let elapsed = self.started.elapsed();
        self.metric.record(elapsed, success);
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
    pub(crate) fn new(labels: &ResourceLabels) -> Self {
        Self {
            metric: brz_metrics::ConnMetric::new_resource(
                brz_metrics::MetricType::Memcache(brz_metrics::ResourceMetric::Conn),
                labels,
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
