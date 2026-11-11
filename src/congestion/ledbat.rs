//! LEDBAT, the delay-based "scavenger" congestion controller of uTP.
//!
//! Implements RFC 6817 (Low Extra Delay Background Transport) with the parameters BEP 29 and
//! libutp use. The controller keeps the queuing delay it adds to the path near [`TARGET`]: it
//! grows the window while the measured queuing delay is below the target and shrinks it above.
//! Competing traffic that fills the bottleneck queue raises the delay, so LEDBAT yields to it.
//!
//! The one-way delay samples come from the `timestamp_difference_microseconds` field of every
//! received uTP packet: the peer's clock at the moment it received our last packet minus our clock
//! when we sent it. The two clocks are unrelated, so a single sample means nothing; only the
//! difference between a sample and the smallest recent sample (the "base delay") is the queuing
//! delay. Samples are u32 microseconds that wrap around every ~71 minutes, so every comparison
//! between them is done with wrapping arithmetic.
//!
//! Two corrections from libutp keep a bad base delay from throttling the flow for the length of
//! the base history: a queuing delay larger than the round-trip time of the acknowledged
//! segments is impossible, so the base is raised by the excess; and clock drift between the two
//! peers is cancelled using the samples of the opposite direction.

use std::time::{Duration, Instant};

use tracing::{debug, trace};

use crate::rtte::RttEstimator;

use super::{CongestionController, DelaySample};

/// The queuing delay LEDBAT aims for. RFC 6817 §2.5: "TARGET MUST be 100 milliseconds or less";
/// BEP 29 and libutp use 100 ms.
const TARGET: Duration = Duration::from_millis(100);

/// RFC 6817 §2.5: "GAIN MUST be set to 1 or less". With 1 the fastest growth (no queuing delay)
/// equals TCP Reno's congestion avoidance: one MSS per RTT.
const GAIN: f64 = 1.;

/// Smallest window, in MSS. RFC 6817 §2.5: "INIT_CWND and MIN_CWND SHOULD both be 2".
const MIN_CWND: usize = 2;

/// Initial window, in MSS.
const INIT_CWND: usize = 2;

/// How far, in MSS, the window may grow beyond what is actually in flight (RFC 6817 §2.5).
const ALLOWED_INCREASE: usize = 1;

/// Number of base delay buckets. RFC 6817 §2.5: "BASE_HISTORY SHOULD be 10" one-minute buckets.
/// BEP 29 describes a two-minute history; we keep the RFC value. A longer history is a more
/// reliable minimum on a stable path, and an increase of the real base delay (route change) is
/// forgotten after at most 10 minutes.
const BASE_HISTORY: usize = 10;

/// Length of one base delay bucket.
const BASE_HISTORY_INTERVAL: Duration = Duration::from_secs(60);

/// Number of recent samples the current delay is the minimum of. RFC 6817 §2.5 asks for at least
/// 4 samples per RTT, and a MIN filter over a few samples removes the spikes delayed ACKs and
/// scheduling cause. Samples older than one RTT are not used (RFC 6817 §2.5).
const CURRENT_FILTER: usize = 4;

/// Slow start ends the first time the queuing delay reaches this. libutp leaves slow start at 90%
/// of the target, but its "slow start" adds one packet per RTT; ours doubles the window every RTT
/// like TCP, so it stops at half the target to keep the overshoot of the queue (the delay signal
/// lags one RTT behind the window) small.
const SLOW_START_EXIT_QUEUING_DELAY: Duration = Duration::from_millis(50);

/// A sample further than this from the base delay is not a delay measurement: no queue holds
/// 10 seconds of data, so it is a broken or hostile peer, or a timestamp computed without
/// wrap-around. Such a sample must neither become the new base nor collapse the window.
const MAX_DELAY_DEVIATION_MICROS: u32 = 10_000_000;

/// If this many implausible samples arrive in a row, the peer's clock really jumped (or our
/// history was wrong to begin with): forget the history and start measuring from scratch rather
/// than ignoring the peer forever.
const RELEARN_AFTER_IMPLAUSIBLE_SAMPLES: u32 = 8;

/// The largest single correction for clock drift (libutp's limit). A larger drop of the peer's
/// base delay towards us is a change of the path or of the peer's clock, not drift.
const MAX_CLOCK_DRIFT_SHIFT_MICROS: u32 = 10_000;

/// `a < b` for u32 timestamps that wrap around. Correct as long as the two values are less than
/// 2^31 microseconds (~35 minutes) apart, which the [`MAX_DELAY_DEVIATION_MICROS`] check
/// guarantees for the samples we keep.
fn wrapping_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

fn wrapping_min(a: u32, b: u32) -> u32 {
    if wrapping_lt(b, a) { b } else { a }
}

/// Minima of the delay samples over the last [`BASE_HISTORY`] intervals of
/// [`BASE_HISTORY_INTERVAL`] each (RFC 6817 `update_base_delay`).
#[derive(Clone, Copy, Debug)]
struct BaseDelayHistory {
    /// Ring buffer of per-interval minima. `None` is the RFC's +INFINITY: an interval without
    /// samples.
    minima: [Option<u32>; BASE_HISTORY],
    /// Index of the interval that is being filled now.
    newest: usize,
    /// When the newest interval started. `None` before the first sample.
    newest_started: Option<Instant>,
}

impl BaseDelayHistory {
    const fn new() -> Self {
        Self {
            minima: [None; BASE_HISTORY],
            newest: 0,
            newest_started: None,
        }
    }

    fn add(&mut self, now: Instant, sample: u32) {
        let Some(started) = self.newest_started else {
            self.newest_started = Some(now);
            self.minima[self.newest] = Some(sample);
            return;
        };

        let intervals =
            now.saturating_duration_since(started).as_secs() / BASE_HISTORY_INTERVAL.as_secs();
        if intervals > 0 {
            // Every interval that passed without samples is stored as +INFINITY (RFC 6817
            // §2.4.2), so after BASE_HISTORY idle intervals the whole history is gone.
            let rolls = intervals.min(BASE_HISTORY as u64) as usize;
            for _ in 0..rolls {
                self.newest = (self.newest + 1) % BASE_HISTORY;
                self.minima[self.newest] = None;
            }
            // Keep the interval boundaries aligned so that they do not drift with sample arrival.
            self.newest_started =
                Some(started + Duration::from_secs(intervals * BASE_HISTORY_INTERVAL.as_secs()));
        }

        let slot = &mut self.minima[self.newest];
        *slot = Some(slot.map_or(sample, |m| wrapping_min(m, sample)));
    }

    fn min(&self) -> Option<u32> {
        self.minima.iter().flatten().copied().reduce(wrapping_min)
    }

    /// Raise every interval's minimum by `offset` microseconds.
    fn shift(&mut self, offset: u32) {
        for m in self.minima.iter_mut().flatten() {
            *m = m.wrapping_add(offset);
        }
    }
}

/// The last [`CURRENT_FILTER`] delay samples with their arrival time (RFC 6817
/// `update_current_delay`); the current delay is the minimum of those not older than one RTT.
#[derive(Clone, Copy, Debug)]
struct CurrentDelayFilter {
    samples: [Option<(Instant, u32)>; CURRENT_FILTER],
    /// Where the next sample goes; the newest sample is just before it.
    next: usize,
}

impl CurrentDelayFilter {
    const fn new() -> Self {
        Self {
            samples: [None; CURRENT_FILTER],
            next: 0,
        }
    }

    fn add(&mut self, now: Instant, sample: u32) {
        self.samples[self.next] = Some((now, sample));
        self.next = (self.next + 1) % CURRENT_FILTER;
    }

    fn newest(&self) -> Option<u32> {
        self.samples[(self.next + CURRENT_FILTER - 1) % CURRENT_FILTER].map(|(_, s)| s)
    }

    fn get(&self, now: Instant, max_age: Duration) -> Option<u32> {
        self.samples
            .iter()
            .flatten()
            .filter(|(at, _)| now.saturating_duration_since(*at) <= max_age)
            .map(|(_, s)| *s)
            .reduce(wrapping_min)
            // All samples are older than an RTT (the peer has been quiet): the newest one is still
            // the best estimate there is.
            .or_else(|| self.newest())
    }
}

/// LEDBAT congestion controller (RFC 6817, BEP 29).
///
/// Window arithmetic is in bytes, as in the RFC.
#[derive(Clone, Copy)]
pub struct Ledbat {
    cwnd: f64,
    /// Slow start runs until the queuing delay first approaches the target or a loss happens,
    /// and again after a retransmission timeout until the window is back at `ssthresh`. A
    /// delay-based decrease does not restart it (that would ramp up faster than TCP).
    slow_start: bool,
    ssthresh: f64,
    mss: usize,
    /// The remote receive window. Like CUBIC, the window never grows beyond it.
    rwnd: usize,

    /// Our delay to the peer.
    base_delays: BaseDelayHistory,
    current_delays: CurrentDelayFilter,
    implausible_samples: u32,
    /// The peer's delay to us. Only used to detect clock drift.
    their_base_delays: BaseDelayHistory,

    /// Bytes in flight after the last round of sending, and whether that round had more data
    /// than it could send. Together they tell how much of the window is being used.
    flight_size: usize,
    window_limited: bool,

    srtt: Duration,
    /// When the window was last reduced because of loss, to reduce at most once per RTT.
    last_decrease: Option<Instant>,
}

impl core::fmt::Debug for Ledbat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cwnd={},slow_start={},sshthresh={},mss={},base_delay_us={:?},last_delay_us={:?},flight={},window_limited={}",
            self.window(),
            self.slow_start,
            self.sshthresh(),
            self.mss,
            self.base_delays.min(),
            self.current_delays.newest(),
            self.flight_size,
            self.window_limited,
        )
    }
}

impl PartialEq for Ledbat {
    fn eq(&self, other: &Self) -> bool {
        self.cwnd == other.cwnd
            && self.ssthresh == other.ssthresh
            && self.slow_start == other.slow_start
    }
}

impl Ledbat {
    /// A controller in slow start with an [`INIT_CWND`] window and no delay history. The
    /// window stays 0 until the first [`CongestionController::set_remote_window`].
    pub fn new(mss: usize) -> Ledbat {
        Ledbat {
            cwnd: (INIT_CWND * mss) as f64,
            slow_start: true,
            ssthresh: f64::INFINITY,
            mss,
            rwnd: 0,
            base_delays: BaseDelayHistory::new(),
            current_delays: CurrentDelayFilter::new(),
            implausible_samples: 0,
            their_base_delays: BaseDelayHistory::new(),
            flight_size: 0,
            window_limited: false,
            srtt: Duration::ZERO,
            last_decrease: None,
        }
    }

    fn min_cwnd(&self) -> f64 {
        (MIN_CWND * self.mss) as f64
    }

    /// Current delay minus base delay. Zero without samples: a peer that does not report delays
    /// gets standard loss-based behaviour (RFC 6817 §4.2).
    fn queuing_delay(&self, now: Instant) -> Duration {
        let (Some(base), Some(current)) = (
            self.base_delays.min(),
            self.current_delays.get(now, self.srtt),
        ) else {
            return Duration::ZERO;
        };
        let queuing = current.wrapping_sub(base) as i32;
        Duration::from_micros(u64::try_from(queuing).unwrap_or(0))
    }

    /// The part of the window the sender actually uses: all of it (up to the remote window) if
    /// the last send round stopped on a full window, otherwise what was in flight after it.
    fn used_window(&self) -> f64 {
        if self.window_limited {
            self.cwnd.min(self.rwnd as f64)
        } else {
            (self.flight_size as f64).min(self.cwnd)
        }
    }

    fn add_our_sample(&mut self, now: Instant, delay_micros: u32) {
        if let Some(base) = self.base_delays.min() {
            let deviation = (delay_micros.wrapping_sub(base) as i32).unsigned_abs();
            if deviation > MAX_DELAY_DEVIATION_MICROS {
                self.implausible_samples += 1;
                if self.implausible_samples < RELEARN_AFTER_IMPLAUSIBLE_SAMPLES {
                    trace!(
                        delay_micros,
                        base, "LEDBAT: ignoring implausible delay sample"
                    );
                    return;
                }
                debug!(
                    delay_micros,
                    base, "LEDBAT: the peer's clock jumped, measuring delays from scratch"
                );
                self.base_delays = BaseDelayHistory::new();
                self.current_delays = CurrentDelayFilter::new();
            }
        }
        self.implausible_samples = 0;

        self.base_delays.add(now, delay_micros);
        self.current_delays.add(now, delay_micros);
    }

    /// Clock drift: if the peer's clock runs faster than ours, our delay samples creep up
    /// (phantom queuing delay) while its samples of the opposite direction creep down by the
    /// same amount. So when the peer's base delay drops a little, raise ours by as much, as
    /// libutp does. A peer that lies about this can only make us less cautious, and the next
    /// real sample below the raised base undoes it.
    fn add_their_sample(&mut self, now: Instant, delay_micros: u32) {
        let previous = self.their_base_delays.min();
        self.their_base_delays.add(now, delay_micros);
        let (Some(previous), Some(new)) = (previous, self.their_base_delays.min()) else {
            return;
        };
        if wrapping_lt(new, previous) {
            let drift = previous.wrapping_sub(new);
            if drift <= MAX_CLOCK_DRIFT_SHIFT_MICROS {
                self.base_delays.shift(drift);
            }
        }
    }

    /// A one-way delay can never exceed the round-trip time. If the queuing delay does, the
    /// base is too low: an outlier sample from the peer, a jump of its clock or ours (e.g. our
    /// clock stopping while the host was suspended), or drift. Raise the base by the excess, as
    /// libutp does, so the error is bounded by the RTT instead of lasting until the bucket with
    /// the bad minimum expires.
    fn bound_by_rtt(&mut self, now: Instant, rtt: Duration) {
        let excess = self.queuing_delay(now).saturating_sub(rtt);
        if excess.is_zero() {
            return;
        }
        // queuing_delay() comes from an i32, so the excess fits.
        let Ok(excess_micros) = u32::try_from(excess.as_micros()) else {
            return;
        };
        trace!(
            ?excess,
            ?rtt,
            "LEDBAT: queuing delay above the RTT, raising the base delay"
        );
        self.base_delays.shift(excess_micros);
    }
}

impl CongestionController for Ledbat {
    fn window(&self) -> usize {
        self.cwnd.max(self.min_cwnd()).min(self.rwnd as f64) as usize
    }

    fn sshthresh(&self) -> usize {
        // f64 -> usize casts saturate, so infinity becomes usize::MAX like CUBIC's.
        self.ssthresh as usize
    }

    fn set_mss(&mut self, mss: usize) {
        if self.mss != mss {
            // The window is in bytes, so it keeps its value; only the floor moves.
            self.mss = mss;
            self.cwnd = self.cwnd.max(self.min_cwnd());
        }
    }

    fn smss(&self) -> usize {
        self.mss
    }

    fn on_recovered(&mut self, new_cwnd_bytes: usize, new_sshthresh: usize) {
        self.cwnd = (new_cwnd_bytes as f64)
            .min(self.rwnd as f64)
            .max(self.min_cwnd());
        self.ssthresh = new_sshthresh as f64;
    }

    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator) {
        self.srtt = rtt.roundtrip_time();
        if len == 0 {
            return;
        }

        let queuing_delay = self.queuing_delay(now);
        if self.slow_start && queuing_delay >= SLOW_START_EXIT_QUEUING_DELAY {
            trace!(
                ?queuing_delay,
                cwnd = self.cwnd,
                "LEDBAT: leaving slow start"
            );
            self.slow_start = false;
            self.ssthresh = self.cwnd;
        }

        let mss = self.mss as f64;
        let used = self.used_window();
        let max_allowed = used + (ALLOWED_INCREASE * self.mss) as f64;
        let old = self.cwnd;
        let mut cwnd = if self.slow_start {
            let cwnd = (old + len as f64).min(self.ssthresh);
            if cwnd >= self.ssthresh {
                self.slow_start = false;
            }
            cwnd
        } else {
            let target = TARGET.as_secs_f64();
            let off_target = (target - queuing_delay.as_secs_f64()) / target;
            if off_target < 0. {
                // Above target: yield. Cut the unused part first (RFC 6817 max_allowed_cwnd),
                // otherwise the decrease, which is inversely proportional to the window, would
                // be diluted by a window that is much larger than what is sent (e.g. after the
                // remote window shrank) and the flow would hardly back off.
                let from = old.min(max_allowed).max(self.min_cwnd());
                from + GAIN * off_target * len as f64 * mss / from
            } else {
                old + GAIN * off_target * len as f64 * mss / old
            }
        };

        if cwnd > old {
            // RFC 6817: max_allowed_cwnd = flightsize + ALLOWED_INCREASE * MSS, so that a window
            // the sender does not fill does not keep growing. If the last send round stopped on a
            // full window, the window is in use and may grow by ALLOWED_INCREASE MSS per ACK;
            // otherwise growth stops at what was actually in flight. Unlike the RFC we do not cut
            // an existing window down to the flight size while below target: ACKs are processed
            // in batches here, and an application-limited pause (a slow disk read) would leave a
            // window that takes hundreds of RTTs to regrow linearly. Above target the unused
            // part is cut, see above.
            let limit = max_allowed.min(self.rwnd as f64);
            cwnd = cwnd.min(old.max(limit));
        }

        self.cwnd = cwnd.max(self.min_cwnd());
    }

    fn on_retransmission_timeout(&mut self, now: Instant) {
        // RFC 6817 §2.4.2: no ACK within a congestion timeout means extreme congestion. The RFC
        // sets cwnd to 1 MSS and clamps it back to MIN_CWND on the next ACK; until that ACK only
        // the retransmitted segment is sent anyway, so go to MIN_CWND directly. Slow start back
        // up to half the previous window as TCP does (RFC 5681 §3.1).
        self.ssthresh = (self.cwnd / 2.).max(self.min_cwnd());
        self.cwnd = self.min_cwnd();
        self.slow_start = true;
        self.last_decrease = Some(now);
    }

    fn on_enter_recovery(&mut self, now: Instant) {
        // RFC 6817 §2.4.2 "on data loss": halve, at most once per RTT.
        let may_decrease = self
            .last_decrease
            .is_none_or(|t| now.saturating_duration_since(t) >= self.srtt);
        if may_decrease {
            self.cwnd = self.cwnd.min((self.cwnd / 2.).max(self.min_cwnd()));
            self.last_decrease = Some(now);
        }
        // The recovery module uses ssthresh as the window during recovery and sets the window
        // to it afterwards. A loss also ends slow start.
        self.ssthresh = self.cwnd;
        self.slow_start = false;
    }

    fn set_remote_window(&mut self, win: usize) {
        self.rwnd = win;
    }

    fn on_delay_sample(&mut self, now: Instant, sample: DelaySample) {
        // 0 means the peer has not received anything from us yet, or the packet carries no
        // timestamp (BEP 29, libutp).
        if sample.theirs != 0 {
            self.add_their_sample(now, sample.theirs);
        }
        if sample.ours != 0 {
            self.add_our_sample(now, sample.ours);
        }
        if let Some(rtt) = sample.acked_rtt {
            self.bound_by_rtt(now, rtt);
        }
    }

    fn on_send_round(&mut self, flight_size: usize, window_limited: bool) {
        self.flight_size = flight_size;
        self.window_limited = window_limited;
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{
        congestion::{CongestionController, DelaySample},
        rtte::RttEstimator,
    };

    use super::{BASE_HISTORY, BASE_HISTORY_INTERVAL, Ledbat, RELEARN_AFTER_IMPLAUSIBLE_SAMPLES};

    const MSS: usize = 1000;
    const RTT: Duration = Duration::from_millis(50);
    /// Arbitrary: the peer's clock has nothing to do with ours.
    const BASE: u32 = 123_456_789;

    struct Harness {
        ledbat: Ledbat,
        now: Instant,
        rtte: RttEstimator,
    }

    impl Harness {
        fn new() -> Self {
            let mut ledbat = Ledbat::new(MSS);
            ledbat.set_remote_window(100 * 1024 * 1024);
            let mut rtte = RttEstimator::default();
            rtte.sample(RTT);
            Self {
                ledbat,
                now: Instant::now(),
                rtte,
            }
        }

        /// Leave slow start with the given window, as if the delay had approached the target
        /// there: the state slow start exit leaves behind.
        fn congestion_avoidance(cwnd_mss: usize) -> Self {
            let mut h = Self::new();
            h.ledbat.cwnd = (cwnd_mss * MSS) as f64;
            h.ledbat.slow_start = false;
            h.ledbat.ssthresh = h.ledbat.cwnd;
            h
        }

        /// Report a raw delay sample from a packet that acknowledges nothing.
        fn sample(&mut self, raw: u32) {
            self.ledbat.on_delay_sample(
                self.now,
                DelaySample {
                    ours: raw,
                    ..Default::default()
                },
            );
        }

        /// Report a raw delay sample from a packet that acknowledges segments sent `rtt` ago.
        fn sample_acked(&mut self, raw: u32, rtt: Duration) {
            self.ledbat.on_delay_sample(
                self.now,
                DelaySample {
                    ours: raw,
                    theirs: 0,
                    acked_rtt: Some(rtt),
                },
            );
        }

        /// Report enough samples that the current delay filter holds only this one.
        fn current(&mut self, raw: u32) {
            for _ in 0..super::CURRENT_FILTER {
                self.sample(raw);
            }
        }

        /// Establish a base delay and then a steady queuing delay.
        fn queuing(&mut self, queuing: Duration) {
            self.sample(BASE);
            self.current(BASE + queuing.as_micros() as u32);
        }

        fn queuing_delay(&self) -> Duration {
            self.ledbat.queuing_delay(self.now)
        }

        /// ACK `count` segments of one MSS each, the sender keeping the window full.
        fn ack_full_window(&mut self, count: usize) {
            for _ in 0..count {
                self.ledbat.on_send_round(self.ledbat.window(), true);
                self.ledbat.on_ack(self.now, MSS, &self.rtte);
            }
        }

        fn cwnd(&self) -> f64 {
            self.ledbat.cwnd
        }
    }

    #[test]
    fn test_grows_one_mss_per_window_without_queuing_delay() {
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::ZERO);
        h.ack_full_window(20);
        let grown = h.cwnd() - (20 * MSS) as f64;
        assert!(
            (0.9 * MSS as f64..=1.0 * MSS as f64).contains(&grown),
            "GAIN = 1: one MSS per window of ACKs, grown by {grown}"
        );
    }

    #[test]
    fn test_grows_proportionally_below_target() {
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::from_millis(50));
        h.ack_full_window(20);
        let grown = h.cwnd() - (20 * MSS) as f64;
        assert!(
            (0.45 * MSS as f64..=0.5 * MSS as f64).contains(&grown),
            "off_target = 0.5, grown by {grown}"
        );
    }

    #[test]
    fn test_stops_growing_at_target() {
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::from_millis(100));
        h.ack_full_window(100);
        assert_eq!(h.cwnd(), (20 * MSS) as f64);
    }

    #[test]
    fn test_shrinks_above_target() {
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::from_millis(150));
        h.ack_full_window(20);
        let shrunk = (20 * MSS) as f64 - h.cwnd();
        assert!(
            (0.5 * MSS as f64..=0.55 * MSS as f64).contains(&shrunk),
            "off_target = -0.5, shrunk by {shrunk}"
        );
    }

    #[test]
    fn test_never_below_min_cwnd() {
        let mut h = Harness::congestion_avoidance(4);
        h.queuing(Duration::from_secs(1));
        h.ack_full_window(1000);
        assert_eq!(h.cwnd(), (2 * MSS) as f64);
        assert_eq!(h.ledbat.window(), 2 * MSS);
    }

    #[test]
    fn test_application_limited_flow_does_not_grow() {
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::ZERO);
        for _ in 0..100 {
            // The application only ever has 5 segments in flight.
            h.ledbat.on_send_round(5 * MSS, false);
            h.ledbat.on_ack(h.now, MSS, &h.rtte);
        }
        assert_eq!(h.cwnd(), (20 * MSS) as f64);

        // In slow start too: an unused window does not double.
        let mut h = Harness::new();
        h.queuing(Duration::ZERO);
        for _ in 0..100 {
            h.ledbat.on_send_round(MSS, false);
            h.ledbat.on_ack(h.now, MSS, &h.rtte);
        }
        assert_eq!(h.cwnd(), (2 * MSS) as f64);

        // Growth resumes up to ALLOWED_INCREASE beyond the flight size when the flow uses more.
        for _ in 0..100 {
            h.ledbat.on_send_round(10 * MSS, false);
            h.ledbat.on_ack(h.now, MSS, &h.rtte);
        }
        assert_eq!(h.cwnd(), (11 * MSS) as f64);
    }

    #[test]
    fn test_slow_start_until_queuing_delay_approaches_target() {
        let mut h = Harness::new();
        h.queuing(Duration::ZERO);
        h.ack_full_window(2);
        assert_eq!(h.cwnd(), (4 * MSS) as f64, "slow start doubles per window");
        h.ack_full_window(4);
        assert_eq!(h.cwnd(), (8 * MSS) as f64);

        h.current(BASE + 60_000);
        h.ack_full_window(1);
        assert_eq!(h.ledbat.sshthresh(), 8 * MSS);
        let cwnd = h.cwnd();
        assert!(cwnd > (8 * MSS) as f64 && cwnd < (8 * MSS + MSS / 10) as f64);

        // Congestion avoidance for good, even when the queue empties again.
        h.current(BASE);
        h.ack_full_window(8);
        assert!(h.cwnd() < (10 * MSS) as f64);
    }

    #[test]
    fn test_base_delay_history_rolls() {
        let mut h = Harness::congestion_avoidance(20);
        h.sample(BASE);
        // The route changes: the real base delay is 150 ms higher from now on.
        let new_base = BASE + 150_000;
        let start = h.now;
        let step = Duration::from_secs(30);
        while h.now + step < start + BASE_HISTORY_INTERVAL * BASE_HISTORY as u32 {
            h.now += step;
            h.current(new_base);
            assert_eq!(h.queuing_delay(), Duration::from_millis(150));
        }
        h.ack_full_window(20);
        assert!(h.cwnd() < (20 * MSS) as f64, "above target: shrinking");

        // Once the bucket holding the old minimum is ten minutes old it is gone.
        h.now = start + BASE_HISTORY_INTERVAL * BASE_HISTORY as u32;
        h.current(new_base);
        assert_eq!(h.queuing_delay(), Duration::ZERO);
        let before = h.cwnd();
        h.ack_full_window(20);
        assert!(h.cwnd() > before, "the window grows again");
    }

    #[test]
    fn test_idle_intervals_expire_history() {
        let mut h = Harness::congestion_avoidance(20);
        h.sample(BASE);
        // No samples for longer than the whole history.
        h.now += BASE_HISTORY_INTERVAL * (BASE_HISTORY as u32 + 3);
        h.current(BASE + 80_000);
        assert_eq!(h.queuing_delay(), Duration::ZERO);
    }

    #[test]
    fn test_samples_wrap_around() {
        // The base is just below u32::MAX, the current sample wrapped past zero.
        let mut h = Harness::congestion_avoidance(20);
        let base = u32::MAX - 20_000;
        h.sample(base);
        h.current(base.wrapping_add(50_000));
        assert_eq!(h.queuing_delay(), Duration::from_millis(50));
        h.ack_full_window(20);
        assert!(h.cwnd() > (20 * MSS) as f64);

        // A sample just below a small base wraps to a huge u32: it is a new, lower base, not
        // a huge queuing delay.
        let mut h = Harness::congestion_avoidance(20);
        let base = 5_000;
        h.current(base);
        let lower = base.wrapping_sub(10_000);
        h.sample(lower);
        assert_eq!(h.ledbat.base_delays.min(), Some(lower));
        assert_eq!(h.queuing_delay(), Duration::ZERO);
        h.current(base);
        assert_eq!(h.queuing_delay(), Duration::from_millis(10));
    }

    #[test]
    fn test_ignores_zero_and_implausible_samples() {
        let mut h = Harness::congestion_avoidance(20);
        h.sample(0);
        assert_eq!(h.ledbat.base_delays.min(), None, "0 = no sample yet");

        h.queuing(Duration::from_millis(30));
        // 20 seconds off, either way: neither a new base nor a delay that collapses the window.
        h.sample(BASE + 20_000_000);
        h.sample(BASE.wrapping_sub(20_000_000));
        assert_eq!(h.ledbat.base_delays.min(), Some(BASE));
        assert_eq!(h.queuing_delay(), Duration::from_millis(30));
        // A plausible sample in between: the implausible ones were not "in a row".
        h.sample(BASE + 30_000);

        // A peer whose clock really jumped gets measured from scratch eventually.
        let jumped = BASE + 1_000_000_000;
        for _ in 0..RELEARN_AFTER_IMPLAUSIBLE_SAMPLES {
            h.sample(jumped);
        }
        assert_eq!(h.ledbat.base_delays.min(), Some(jumped));
        h.current(jumped + 40_000);
        assert_eq!(h.queuing_delay(), Duration::from_millis(40));
    }

    #[test]
    fn test_loss_halves_once_per_rtt() {
        let mut h = Harness::congestion_avoidance(40);
        h.ack_full_window(1);
        let cwnd = h.cwnd();
        h.ledbat.on_enter_recovery(h.now);
        assert_eq!(h.cwnd(), cwnd / 2.);
        assert_eq!(h.ledbat.sshthresh(), (cwnd / 2.) as usize);

        // More losses from the same window of data.
        h.now += RTT / 2;
        h.ledbat.on_enter_recovery(h.now);
        assert_eq!(h.cwnd(), cwnd / 2.);

        h.now += RTT / 2;
        h.ledbat.on_enter_recovery(h.now);
        assert_eq!(h.cwnd(), cwnd / 4.);

        // What the recovery module does when it is done.
        let ssthresh = h.ledbat.sshthresh();
        h.ledbat.on_recovered(ssthresh, ssthresh);
        assert_eq!(h.ledbat.window(), (cwnd / 4.) as usize);

        for _ in 0..10 {
            h.now += RTT;
            h.ledbat.on_enter_recovery(h.now);
        }
        assert_eq!(h.ledbat.window(), 2 * MSS, "never below MIN_CWND");
    }

    #[test]
    fn test_retransmission_timeout_resets_window() {
        let mut h = Harness::congestion_avoidance(40);
        h.queuing(Duration::ZERO);
        h.ledbat.on_retransmission_timeout(h.now);
        assert_eq!(h.ledbat.window(), 2 * MSS);
        assert_eq!(h.ledbat.sshthresh(), 20 * MSS);

        // Slow start back to half the old window, then congestion avoidance.
        h.ack_full_window(18);
        assert_eq!(h.cwnd(), (20 * MSS) as f64);
        h.ack_full_window(20);
        assert!(h.cwnd() < (22 * MSS) as f64);
    }

    #[test]
    fn test_mss_change() {
        let mut h = Harness::new();
        assert_eq!(h.ledbat.window(), 2 * MSS);
        h.ledbat.set_mss(1400);
        assert_eq!(h.ledbat.smss(), 1400);
        assert_eq!(h.ledbat.window(), 2800, "MIN_CWND is in MSS");

        let mut h = Harness::congestion_avoidance(20);
        h.ledbat.set_mss(500);
        assert_eq!(h.ledbat.window(), 20 * MSS, "the window is in bytes");
        h.queuing(Duration::ZERO);
        // One 500 byte MSS per window of ACKs.
        h.ack_full_window(20);
        let grown = h.cwnd() - (20 * MSS) as f64;
        assert!((450. ..=500.).contains(&grown), "grown by {grown}");
    }

    #[test]
    fn test_remote_window_caps_window() {
        let mut h = Harness::new();
        h.ledbat.set_remote_window(5 * MSS);
        h.queuing(Duration::ZERO);
        h.ack_full_window(100);
        assert_eq!(h.ledbat.window(), 5 * MSS);
        assert!(h.cwnd() <= (5 * MSS) as f64, "does not grow beyond rwnd");

        h.ledbat.set_remote_window(MSS);
        assert_eq!(
            h.ledbat.window(),
            MSS,
            "rwnd caps even below MIN_CWND, like CUBIC"
        );
        h.ledbat.set_remote_window(0);
        assert_eq!(h.ledbat.window(), 0);
    }

    #[test]
    fn test_delay_based_decrease_does_not_restart_slow_start() {
        // Reach congestion avoidance the way production does.
        let mut h = Harness::new();
        h.queuing(Duration::ZERO);
        h.ack_full_window(18);
        h.current(BASE + 60_000);
        h.ack_full_window(1);
        assert!(!h.ledbat.slow_start);

        h.current(BASE + 150_000);
        h.ack_full_window(20);
        let shrunk = h.cwnd();
        assert!(shrunk < (20 * MSS) as f64);

        // The queue drains: congestion avoidance growth, not a slow-start MSS per ACK.
        h.current(BASE + 10_000);
        h.ack_full_window(1);
        let grown = h.cwnd() - shrunk;
        assert!(
            grown > 0. && grown < 0.1 * MSS as f64,
            "grown by {grown} in one ACK"
        );
        assert!(!h.ledbat.slow_start);
    }

    #[test]
    fn test_outlier_below_base_is_bounded_by_rtt() {
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::ZERO);
        // A single sample 5 seconds below the real base (a broken or hostile peer).
        h.sample(BASE.wrapping_sub(5_000_000));
        h.current(BASE);
        assert_eq!(h.queuing_delay(), Duration::from_secs(5));

        // The next ACK proves the delay cannot be more than its RTT, and the base is raised.
        h.sample_acked(BASE, RTT);
        assert_eq!(h.queuing_delay(), RTT);
        // 50 ms is below the target: the window keeps growing instead of being pinned at
        // MIN_CWND until the bucket with the outlier expires ten minutes later.
        h.ack_full_window(20);
        assert!(h.cwnd() > (20 * MSS) as f64, "{:?}", h.ledbat);

        // What remains is an over-estimate bounded by the RTT, until the bucket expires.
        h.current(BASE);
        assert_eq!(h.queuing_delay(), RTT);
    }

    #[test]
    fn test_upward_step_is_bounded_by_rtt() {
        // All samples jump up by 3 seconds, e.g. because our clock stopped while the host was
        // suspended and the peer's did not.
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::ZERO);
        let stepped = BASE + 3_000_000;
        h.current(stepped);
        assert_eq!(h.queuing_delay(), Duration::from_secs(3));
        h.sample_acked(stepped, RTT);
        assert_eq!(h.queuing_delay(), RTT);

        // Within the RTT the excess is not provably wrong and stays.
        let mut h = Harness::congestion_avoidance(20);
        h.queuing(Duration::from_millis(40));
        h.sample_acked(BASE + 40_000, RTT);
        assert_eq!(h.queuing_delay(), Duration::from_millis(40));
    }

    #[test]
    fn test_clock_drift_is_compensated() {
        // The peer's clock runs 200 ppm fast: over nine minutes our samples creep up by 108 ms
        // and the opposite direction's creep down by as much, with no real queuing at all.
        const THEIR_BASE: u32 = 987_654_321;
        let run = |with_reverse_samples: bool| {
            let mut h = Harness::congestion_avoidance(20);
            let start = h.now;
            let step = Duration::from_millis(100);
            while h.now - start < Duration::from_secs(9 * 60) {
                let drift = ((h.now - start).as_micros() / 5_000) as u32;
                h.ledbat.on_delay_sample(
                    h.now,
                    DelaySample {
                        ours: BASE.wrapping_add(drift),
                        theirs: if with_reverse_samples {
                            THEIR_BASE.wrapping_sub(drift)
                        } else {
                            0
                        },
                        acked_rtt: None,
                    },
                );
                h.now += step;
            }
            h.now -= step;
            h.queuing_delay()
        };

        let uncompensated = run(false);
        assert!(
            uncompensated > Duration::from_millis(100),
            "{uncompensated:?}"
        );
        let compensated = run(true);
        assert!(compensated < Duration::from_millis(1), "{compensated:?}");
    }

    #[test]
    fn test_unused_window_does_not_dilute_the_decrease() {
        // A large window, then the peer's receive window drops to 64 segments.
        let mut h = Harness::congestion_avoidance(1000);
        h.ledbat.set_remote_window(64 * MSS);
        h.queuing(Duration::from_millis(200));
        h.ack_full_window(64);
        // The unused part is cut at once, and one RTT of ACKs at twice the target then takes
        // about one MSS off, as with a window of that size. Dividing by the unused 1000 segments
        // would have left the window at 999.9 segments, far above anything sent.
        let cwnd_mss = h.cwnd() / MSS as f64;
        assert!((63.5..64.5).contains(&cwnd_mss), "{:?}", h.ledbat);

        // Application-limited: only 10 segments in flight.
        let mut h = Harness::congestion_avoidance(100);
        h.queuing(Duration::from_millis(200));
        h.ledbat.on_send_round(10 * MSS, false);
        h.ledbat.on_ack(h.now, MSS, &h.rtte);
        assert!(h.cwnd() < (11 * MSS) as f64, "{:?}", h.ledbat);

        // Below target an unused window is kept (see on_ack).
        let mut h = Harness::congestion_avoidance(100);
        h.queuing(Duration::from_millis(50));
        h.ledbat.on_send_round(10 * MSS, false);
        h.ledbat.on_ack(h.now, MSS, &h.rtte);
        assert_eq!(h.cwnd(), (100 * MSS) as f64);
    }
}
