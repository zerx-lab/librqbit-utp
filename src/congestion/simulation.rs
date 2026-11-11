//! Closed-loop tests: a congestion controller sending through a simulated bottleneck.
//!
//! The bottleneck is a fluid FIFO queue drained at a fixed rate with a drop-tail buffer; an
//! optional second flow pours a constant rate into the same queue. Every packet's one-way delay is
//! the propagation delay plus the time the backlog in front of it takes to drain, and the receiver
//! reports it back the way uTP does: its own clock (with an arbitrary offset, chosen so that the
//! samples wrap around u32) minus our timestamp. Everything runs on simulated time.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, Instant},
};

use crate::{
    congestion::{CongestionController, cubic::Cubic, ledbat::Ledbat},
    rtte::RttEstimator,
};

const MSS: usize = 1400;
/// Bottleneck drain rate, bytes per second (8 Mbit/s).
const LINK_RATE: f64 = 1_000_000.;
/// Drop-tail buffer: 500 ms worth of data, far more than LEDBAT's 100 ms target.
const BUFFER_BYTES: f64 = LINK_RATE / 2.;
const FORWARD_DELAY: Duration = Duration::from_millis(25);
const REVERSE_DELAY: Duration = Duration::from_millis(25);
/// The receiver's clock is this far ahead of ours, so that sample values cross u32::MAX as soon
/// as the queuing delay exceeds ~5 ms.
const RECEIVER_CLOCK_OFFSET_MICROS: u32 = u32::MAX - 30_000;

enum Event {
    Ack {
        sent_at: Duration,
        delay_sample: u32,
    },
    Loss,
}

struct Scheduled {
    at: Duration,
    seq: u64,
    bytes: usize,
    event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for Scheduled {}

impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

/// What one interval of the simulation looked like.
#[derive(Debug, Default, Clone, Copy)]
struct IntervalStats {
    /// Bytes of our flow acknowledged in the interval.
    acked_bytes: usize,
    losses: usize,
    /// Mean and max queuing delay our packets experienced.
    mean_queuing_delay: Duration,
    max_queuing_delay: Duration,
}

struct Simulation {
    start: Instant,
    now: Duration,
    events: BinaryHeap<Reverse<Scheduled>>,
    next_seq: u64,

    queue_bytes: f64,
    queue_updated: Duration,
    /// Rate of the competing flow, bytes per second, from the given time on.
    competing: Option<(Duration, f64)>,

    flight: usize,
    rtte: RttEstimator,

    // Accumulators of the current interval.
    acked_bytes: usize,
    losses: usize,
    queuing_delay_sum: Duration,
    queuing_delay_max: Duration,
    packets: u32,
}

impl Simulation {
    fn new(competing: Option<(Duration, f64)>) -> Self {
        Self {
            start: Instant::now(),
            now: Duration::ZERO,
            events: BinaryHeap::new(),
            next_seq: 0,
            queue_bytes: 0.,
            queue_updated: Duration::ZERO,
            competing,
            flight: 0,
            rtte: RttEstimator::default(),
            acked_bytes: 0,
            losses: 0,
            queuing_delay_sum: Duration::ZERO,
            queuing_delay_max: Duration::ZERO,
            packets: 0,
        }
    }

    fn schedule(&mut self, at: Duration, bytes: usize, event: Event) {
        self.next_seq += 1;
        self.events.push(Reverse(Scheduled {
            at,
            seq: self.next_seq,
            bytes,
            event,
        }));
    }

    fn competing_rate(&self, at: Duration) -> f64 {
        match self.competing {
            Some((from, rate)) if at >= from => rate,
            _ => 0.,
        }
    }

    /// Drain the queue up to `self.now`, and fill it with the competing flow.
    fn update_queue(&mut self) {
        let dt = (self.now - self.queue_updated).as_secs_f64();
        let inflow = self.competing_rate(self.now) * dt;
        self.queue_bytes = (self.queue_bytes + inflow - LINK_RATE * dt).clamp(0., BUFFER_BYTES);
        self.queue_updated = self.now;
    }

    fn send_one(&mut self) {
        self.update_queue();
        self.flight += MSS;
        if self.queue_bytes + MSS as f64 > BUFFER_BYTES {
            // Drop-tail. The sender notices about one RTT later (duplicate ACKs).
            let detected = self.now
                + FORWARD_DELAY
                + REVERSE_DELAY
                + Duration::from_secs_f64(self.queue_bytes / LINK_RATE);
            self.schedule(detected, MSS, Event::Loss);
            return;
        }
        self.queue_bytes += MSS as f64;
        let queuing = Duration::from_secs_f64(self.queue_bytes / LINK_RATE);
        self.queuing_delay_sum += queuing;
        self.queuing_delay_max = self.queuing_delay_max.max(queuing);
        self.packets += 1;

        let arrival = self.now + FORWARD_DELAY + queuing;
        let sent_ts = self.now.as_micros() as u32;
        let receiver_clock =
            (arrival.as_micros() as u32).wrapping_add(RECEIVER_CLOCK_OFFSET_MICROS);
        let delay_sample = receiver_clock.wrapping_sub(sent_ts);
        self.schedule(
            arrival + REVERSE_DELAY,
            MSS,
            Event::Ack {
                sent_at: self.now,
                delay_sample,
            },
        );
    }

    /// The sender always has data: send as long as the window allows.
    fn fill_window(&mut self, cc: &mut dyn CongestionController) {
        while self.flight + MSS <= cc.window() {
            self.send_one();
        }
        cc.on_send_round(self.flight, true);
    }

    /// Run until `until`, returning statistics for every `interval`.
    fn run(
        &mut self,
        cc: &mut dyn CongestionController,
        until: Duration,
        interval: Duration,
    ) -> Vec<IntervalStats> {
        let mut stats = Vec::new();
        let mut interval_end = self.now + interval;
        self.fill_window(cc);

        while let Some(Reverse(ev)) = self.events.pop() {
            while ev.at >= interval_end {
                stats.push(self.take_stats());
                interval_end += interval;
            }
            if ev.at >= until {
                break;
            }
            self.now = ev.at;
            let now = self.start + self.now;
            self.flight -= ev.bytes;
            match ev.event {
                Event::Ack {
                    sent_at,
                    delay_sample,
                } => {
                    self.acked_bytes += ev.bytes;
                    self.rtte.sample(self.now - sent_at);
                    cc.on_delay_sample(now, delay_sample);
                    cc.on_ack(now, ev.bytes, &self.rtte);
                }
                Event::Loss => {
                    self.losses += 1;
                    // What the recovery module does, squeezed into one step: enter recovery, then
                    // leave it with the window the controller chose.
                    cc.on_enter_recovery(now);
                    let ssthresh = cc.sshthresh();
                    cc.on_recovered(ssthresh, ssthresh);
                }
            }
            self.fill_window(cc);
        }
        stats
    }

    fn take_stats(&mut self) -> IntervalStats {
        let s = IntervalStats {
            acked_bytes: self.acked_bytes,
            losses: self.losses,
            mean_queuing_delay: self
                .queuing_delay_sum
                .checked_div(self.packets)
                .unwrap_or_default(),
            max_queuing_delay: self.queuing_delay_max,
        };
        self.acked_bytes = 0;
        self.losses = 0;
        self.queuing_delay_sum = Duration::ZERO;
        self.queuing_delay_max = Duration::ZERO;
        self.packets = 0;
        s
    }
}

fn new_ledbat() -> Ledbat {
    let mut cc = Ledbat::new(MSS);
    cc.set_remote_window(64 * 1024 * 1024);
    cc
}

const INTERVAL: Duration = Duration::from_secs(10);

fn rate(s: &[IntervalStats]) -> f64 {
    s.iter().map(|s| s.acked_bytes).sum::<usize>() as f64
        / (INTERVAL.as_secs_f64() * s.len() as f64)
}

#[test]
fn test_ledbat_converges_to_target_queuing_delay() {
    let mut cc = new_ledbat();
    let mut sim = Simulation::new(None);
    let stats = sim.run(&mut cc, Duration::from_secs(180), INTERVAL);
    for (i, s) in stats.iter().enumerate() {
        println!("{:>3}s {s:?}", i as u64 * INTERVAL.as_secs());
    }

    assert_eq!(
        stats.iter().map(|s| s.losses).sum::<usize>(),
        0,
        "LEDBAT must not fill the buffer"
    );
    // After a minute it holds the queue around the target and uses the link.
    for s in &stats[6..] {
        assert!(
            (Duration::from_millis(75)..=Duration::from_millis(125))
                .contains(&s.mean_queuing_delay),
            "{s:?}"
        );
        assert!(s.max_queuing_delay < Duration::from_millis(160), "{s:?}");
    }
    let r = rate(&stats[6..]);
    assert!(r > LINK_RATE * 0.95, "LEDBAT should use the link, rate={r}");
}

#[test]
fn test_cubic_fills_the_queue_until_loss() {
    let mut cc = Cubic::new(Instant::now(), MSS);
    cc.set_remote_window(64 * 1024 * 1024);
    let mut sim = Simulation::new(None);
    let stats = sim.run(&mut cc, Duration::from_secs(180), INTERVAL);
    for (i, s) in stats.iter().enumerate() {
        println!("{:>3}s {s:?}", i as u64 * INTERVAL.as_secs());
    }

    assert!(stats[6..].iter().map(|s| s.losses).sum::<usize>() > 0);
    let max = stats[6..]
        .iter()
        .map(|s| s.max_queuing_delay)
        .max()
        .unwrap();
    assert!(
        max > Duration::from_millis(450),
        "CUBIC should fill the 500 ms buffer, max={max:?}"
    );
}

#[test]
fn test_ledbat_yields_to_competing_traffic() {
    let mut cc = new_ledbat();
    let competing_from = Duration::from_secs(120);
    let mut sim = Simulation::new(Some((competing_from, LINK_RATE * 0.6)));
    let stats = sim.run(&mut cc, Duration::from_secs(300), INTERVAL);
    for (i, s) in stats.iter().enumerate() {
        println!("{:>3}s {s:?}", i as u64 * INTERVAL.as_secs());
    }

    let before = rate(&stats[6..12]);
    let after = rate(&stats[18..]);
    assert!(before > LINK_RATE * 0.95, "before={before}");
    // The other flow takes 60% of the link; LEDBAT keeps what is left without driving the queue
    // past its target.
    assert!(after < LINK_RATE * 0.45, "after={after}");
    assert!(after > LINK_RATE * 0.3, "after={after}");
    for s in &stats[18..] {
        assert_eq!(s.losses, 0, "{s:?}");
        assert!(s.mean_queuing_delay < Duration::from_millis(125), "{s:?}");
    }
}
