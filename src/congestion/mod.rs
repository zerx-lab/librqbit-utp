pub mod cubic;
pub mod ledbat;
pub mod tracing;

#[cfg(test)]
mod simulation;

use std::time::Instant;

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

    /// A one-way delay sample from us to the peer: the `timestamp_difference_microseconds` field
    /// of a received packet, raw. It is the peer's clock minus ours, so only differences between
    /// samples are meaningful, and it wraps around at u32::MAX. Called for every accepted
    /// packet before `on_ack`, also for packets that acknowledge nothing new.
    fn on_delay_sample(&mut self, now: Instant, delay_micros: u32) {}

    /// Called after every round of sending new data outside of loss recovery with the bytes in
    /// flight after the round, and whether the sender had more data than it sent (`true`: the
    /// window, the transport or MTU probing stopped it; `false`: the application ran out of
    /// data). Lets a controller avoid growing a window that is not used.
    fn on_send_round(&mut self, flight_size: usize, window_limited: bool) {}
}
