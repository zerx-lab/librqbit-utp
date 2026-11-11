pub mod cubic;
pub mod ledbat;
pub mod tracing;

#[cfg(test)]
mod simulation;

use std::time::{Duration, Instant};

use crate::rtte::RttEstimator;

#[allow(unused_variables)]
pub trait CongestionController: Send + Sync + core::fmt::Debug {
    /// Returns the number of bytes that can be sent.
    fn window(&self) -> usize;

    fn sshthresh(&self) -> usize;

    fn set_mss(&mut self, mss: usize);

    fn smss(&self) -> usize;

    fn on_recovered(&mut self, new_cwnd_bytes: usize, new_sshthresh: usize);

    /// Increase the window on ACK
    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator);

    // NOT fast retransmit
    // flight_size per rfc5681
    fn on_retransmission_timeout(&mut self, now: Instant);

    fn on_enter_recovery(&mut self, now: Instant);

    fn set_remote_window(&mut self, win: usize);

    /// Delay information of an accepted packet (BEP 29), called for every accepted packet
    /// before `on_ack`, also for packets that acknowledge nothing new.
    fn on_delay_sample(&mut self, now: Instant, sample: DelaySample) {}

    /// Called after every round of sending new data outside of loss recovery with the bytes in
    /// flight after the round, and whether the sender had more data than it sent (`true`: the
    /// window, the transport or MTU probing stopped it; `false`: the application ran out of
    /// data). Lets a controller avoid growing a window that is not used.
    fn on_send_round(&mut self, flight_size: usize, window_limited: bool) {}
}

/// The one-way delay information one received uTP packet carries (BEP 29).
///
/// Both values are raw: each is one peer's clock minus the other's, so only differences between
/// samples of the same kind are meaningful, and they wrap around at u32::MAX. 0 means unknown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DelaySample {
    /// The `timestamp_difference_microseconds` field: the peer's measurement of the delay from
    /// us to it.
    pub ours: u32,
    /// Our measurement of the delay from the peer to us: our clock when the packet arrived minus
    /// its `timestamp_microseconds` (the value we echo back).
    pub theirs: u32,
    /// The smallest round-trip time of the segments the packet newly acknowledged (segments
    /// that were retransmitted do not count), `None` if it acknowledged none. A one-way delay
    /// can never be larger than this.
    pub acked_rtt: Option<Duration>,
}
