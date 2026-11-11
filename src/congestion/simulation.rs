//! Closed-loop tests: congestion controllers sending through a simulated bottleneck.
//!
//! The bottleneck is a fluid FIFO queue drained at a fixed rate with a drop-tail buffer, shared by
//! one or more simulated uTP flows (each with its own controller, always having data to send) and
//! optionally a constant-rate stream that pours into the same queue. Every packet's one-way delay
//! is the propagation delay plus the time the backlog in front of it takes to drain. The receiver
//! reports it back the way uTP does: its own clock (with an arbitrary offset, chosen so that the
//! samples wrap around u32, optionally running at a slightly different rate and stamping late by
//! a random amount) minus our timestamp, and its ACK carries its clock, from which we measure the
//! opposite direction. Everything runs on simulated time and a seeded pseudo-random generator.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, Instant},
};

use crate::{
    congestion::{CongestionController, DelaySample, cubic::Cubic, ledbat::Ledbat},
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

/// How the receiver of a flow reads its clock.
#[derive(Clone, Copy, Default)]
struct ReceiverClock {
    /// How much faster than ours the receiver's clock runs, in parts per million.
    skew_ppm: f64,
    /// The receiver handles each packet (stamps the arrival and sends the ACK) up to this much
    /// after it arrives, uniformly distributed.
    max_jitter: Duration,
}

impl ReceiverClock {
    /// The receiver's clock, in microseconds, at simulated time `at`.
    fn read(&self, at: Duration) -> u32 {
        let micros = at.as_secs_f64() * 1e6 * (1. + self.skew_ppm * 1e-6);
        (micros as u64 as u32).wrapping_add(RECEIVER_CLOCK_OFFSET_MICROS)
    }
}

enum Event {
    Start,
    Ack {
        sent_at: Duration,
        sample: DelaySample,
    },
    Loss {
        sent_at: Duration,
    },
}

struct Scheduled {
    at: Duration,
    seq: u64,
    flow: usize,
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

/// What one interval of the simulation looked like for one flow.
#[derive(Debug, Default, Clone, Copy)]
struct IntervalStats {
    /// Bytes of the flow acknowledged in the interval.
    acked_bytes: usize,
    losses: usize,
    /// Mean and max queuing delay the flow's packets experienced.
    mean_queuing_delay: Duration,
    max_queuing_delay: Duration,
}

struct Flow {
    cc: Box<dyn CongestionController>,
    clock: ReceiverClock,
    active: bool,
    flight: usize,
    rtte: RttEstimator,
    /// When the flow last entered loss recovery. Losses of packets sent before that belong to
    /// the same recovery episode (NewReno's recovery point), as in the real recovery module.
    recovery_started: Option<Duration>,

    // Accumulators of the current interval.
    acked_bytes: usize,
    losses: usize,
    queuing_delay_sum: Duration,
    queuing_delay_max: Duration,
    packets: u32,

    stats: Vec<IntervalStats>,
}

impl Flow {
    fn finish_interval(&mut self) {
        self.stats.push(IntervalStats {
            acked_bytes: self.acked_bytes,
            losses: self.losses,
            mean_queuing_delay: self
                .queuing_delay_sum
                .checked_div(self.packets)
                .unwrap_or_default(),
            max_queuing_delay: self.queuing_delay_max,
        });
        self.acked_bytes = 0;
        self.losses = 0;
        self.queuing_delay_sum = Duration::ZERO;
        self.queuing_delay_max = Duration::ZERO;
        self.packets = 0;
    }
}

struct Simulation {
    start: Instant,
    now: Duration,
    events: BinaryHeap<Reverse<Scheduled>>,
    next_seq: u64,
    /// xorshift64 state.
    rng: u64,

    queue_bytes: f64,
    queue_updated: Duration,
    /// Rate of the competing constant-rate stream, bytes per second, from the given time on.
    competing: Option<(Duration, f64)>,

    flows: Vec<Flow>,
}

impl Simulation {
    fn new(competing: Option<(Duration, f64)>) -> Self {
        Self {
            start: Instant::now(),
            now: Duration::ZERO,
            events: BinaryHeap::new(),
            next_seq: 0,
            rng: 0x9E37_79B9_7F4A_7C15,
            queue_bytes: 0.,
            queue_updated: Duration::ZERO,
            competing,
            flows: Vec::new(),
        }
    }

    /// Add a flow that starts sending at `start`. Returns its index in the result of `run`.
    fn add_flow(
        &mut self,
        cc: Box<dyn CongestionController>,
        start: Duration,
        clock: ReceiverClock,
    ) -> usize {
        self.flows.push(Flow {
            cc,
            clock,
            active: false,
            flight: 0,
            rtte: RttEstimator::default(),
            recovery_started: None,
            acked_bytes: 0,
            losses: 0,
            queuing_delay_sum: Duration::ZERO,
            queuing_delay_max: Duration::ZERO,
            packets: 0,
            stats: Vec::new(),
        });
        let flow = self.flows.len() - 1;
        self.schedule(start, flow, 0, Event::Start);
        flow
    }

    fn random_below(&mut self, bound: u64) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        if bound == 0 { 0 } else { self.rng % bound }
    }

    fn schedule(&mut self, at: Duration, flow: usize, bytes: usize, event: Event) {
        self.next_seq += 1;
        self.events.push(Reverse(Scheduled {
            at,
            seq: self.next_seq,
            flow,
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

    /// Drain the queue up to `self.now`, and fill it with the competing stream.
    fn update_queue(&mut self) {
        let dt = (self.now - self.queue_updated).as_secs_f64();
        let inflow = self.competing_rate(self.now) * dt;
        self.queue_bytes = (self.queue_bytes + inflow - LINK_RATE * dt).clamp(0., BUFFER_BYTES);
        self.queue_updated = self.now;
    }

    fn send_one(&mut self, flow: usize) {
        self.update_queue();
        self.flows[flow].flight += MSS;
        if self.queue_bytes + MSS as f64 > BUFFER_BYTES {
            // Drop-tail. The sender notices about one RTT later (duplicate ACKs).
            let detected = self.now
                + FORWARD_DELAY
                + REVERSE_DELAY
                + Duration::from_secs_f64(self.queue_bytes / LINK_RATE);
            self.schedule(detected, flow, MSS, Event::Loss { sent_at: self.now });
            return;
        }
        self.queue_bytes += MSS as f64;
        let queuing = Duration::from_secs_f64(self.queue_bytes / LINK_RATE);
        let clock = self.flows[flow].clock;
        let jitter =
            Duration::from_micros(self.random_below(clock.max_jitter.as_micros() as u64 + 1));

        let f = &mut self.flows[flow];
        f.queuing_delay_sum += queuing;
        f.queuing_delay_max = f.queuing_delay_max.max(queuing);
        f.packets += 1;

        // The receiver stamps the arrival and sends the ACK with its clock in it.
        let handled = self.now + FORWARD_DELAY + queuing + jitter;
        let receiver_clock = clock.read(handled);
        let sent_ts = self.now.as_micros() as u32;
        let ack_arrival = handled + REVERSE_DELAY;
        let sample = DelaySample {
            ours: receiver_clock.wrapping_sub(sent_ts),
            theirs: (ack_arrival.as_micros() as u32).wrapping_sub(receiver_clock),
            acked_rtt: Some(ack_arrival - self.now),
        };
        self.schedule(
            ack_arrival,
            flow,
            MSS,
            Event::Ack {
                sent_at: self.now,
                sample,
            },
        );
    }

    /// Every flow always has data: send as long as the window allows.
    fn fill_window(&mut self, flow: usize) {
        while self.flows[flow].flight + MSS <= self.flows[flow].cc.window() {
            self.send_one(flow);
        }
        let f = &mut self.flows[flow];
        f.cc.on_send_round(f.flight, true);
    }

    /// Run until `until`, recording statistics of every flow for every `interval`.
    fn run(&mut self, until: Duration, interval: Duration) {
        let mut interval_end = self.now + interval;

        while let Some(Reverse(ev)) = self.events.pop() {
            while ev.at >= interval_end && interval_end <= until {
                for f in &mut self.flows {
                    f.finish_interval();
                }
                interval_end += interval;
            }
            if ev.at >= until {
                break;
            }
            self.now = ev.at;
            let now = self.start + self.now;
            let f = &mut self.flows[ev.flow];
            f.flight -= ev.bytes;
            match ev.event {
                Event::Start => f.active = true,
                Event::Ack { sent_at, sample } => {
                    f.acked_bytes += ev.bytes;
                    f.rtte.sample(self.now - sent_at);
                    f.cc.on_delay_sample(now, sample);
                    f.cc.on_ack(now, ev.bytes, &f.rtte);
                }
                Event::Loss { sent_at } => {
                    f.losses += 1;
                    if f.recovery_started.is_none_or(|started| sent_at >= started) {
                        f.recovery_started = Some(self.now);
                        // What the recovery module does, squeezed into one step: enter recovery,
                        // then leave it with the window the controller chose.
                        f.cc.on_enter_recovery(now);
                        let ssthresh = f.cc.sshthresh();
                        f.cc.on_recovered(ssthresh, ssthresh);
                    }
                }
            }
            if self.flows[ev.flow].active {
                self.fill_window(ev.flow);
            }
        }
    }

    fn stats(&self, flow: usize) -> &[IntervalStats] {
        &self.flows[flow].stats
    }

    fn print(&self) {
        for (i, f) in self.flows.iter().enumerate() {
            for (t, s) in f.stats.iter().enumerate() {
                println!("flow {i} {:>3}s {s:?}", t as u64 * INTERVAL.as_secs());
            }
        }
    }
}

fn new_ledbat() -> Box<dyn CongestionController> {
    let mut cc = Ledbat::new(MSS);
    cc.set_remote_window(64 * 1024 * 1024);
    Box::new(cc)
}

fn new_cubic() -> Box<dyn CongestionController> {
    let mut cc = Cubic::new(Instant::now(), MSS);
    cc.set_remote_window(64 * 1024 * 1024);
    Box::new(cc)
}

const INTERVAL: Duration = Duration::from_secs(10);

fn rate(s: &[IntervalStats]) -> f64 {
    s.iter().map(|s| s.acked_bytes).sum::<usize>() as f64
        / (INTERVAL.as_secs_f64() * s.len() as f64)
}

fn losses(s: &[IntervalStats]) -> usize {
    s.iter().map(|s| s.losses).sum()
}

fn assert_holds_target(stats: &[IntervalStats]) {
    for s in stats {
        assert!(
            (Duration::from_millis(75)..=Duration::from_millis(125))
                .contains(&s.mean_queuing_delay),
            "{s:?}"
        );
        assert!(s.max_queuing_delay < Duration::from_millis(160), "{s:?}");
    }
}

#[test]
fn test_ledbat_converges_to_target_queuing_delay() {
    let mut sim = Simulation::new(None);
    let flow = sim.add_flow(new_ledbat(), Duration::ZERO, ReceiverClock::default());
    sim.run(Duration::from_secs(180), INTERVAL);
    sim.print();
    let stats = sim.stats(flow);

    assert_eq!(losses(stats), 0, "LEDBAT must not fill the buffer");
    // After a minute it holds the queue around the target and uses the link.
    assert_holds_target(&stats[6..]);
    let r = rate(&stats[6..]);
    assert!(r > LINK_RATE * 0.95, "LEDBAT should use the link, rate={r}");
}

#[test]
fn test_ledbat_converges_with_jitter_and_clock_skew() {
    // The receiver's clock runs 200 ppm fast, more than quartz clocks usually drift, which adds
    // 12 ms of phantom delay per minute, and it stamps packets up to 5 ms late. Without drift
    // compensation the phantom delay grows until the RTT bound caps it at the 50 ms propagation
    // delay, and the flow keeps only half the target in the queue.
    //
    // The run stays within the base history (10 minutes). Past that, a single flow that held
    // the queue at the target the whole time has no sample of the empty path left, its base
    // delay inflates by the target and the queue doubles: a known property of RFC 6817 (and
    // libutp) in a perfectly smooth model, mitigated on real paths by cross traffic that
    // drains the queue now and then.
    let clock = ReceiverClock {
        skew_ppm: 200.,
        max_jitter: Duration::from_millis(5),
    };
    let mut sim = Simulation::new(None);
    let flow = sim.add_flow(new_ledbat(), Duration::ZERO, clock);
    sim.run(Duration::from_secs(540), INTERVAL);
    sim.print();
    let stats = sim.stats(flow);

    assert_eq!(losses(stats), 0, "LEDBAT must not fill the buffer");
    assert_holds_target(&stats[6..]);
    let r = rate(&stats[6..]);
    assert!(r > LINK_RATE * 0.95, "LEDBAT should use the link, rate={r}");
}

#[test]
fn test_cubic_fills_the_queue_until_loss() {
    let mut sim = Simulation::new(None);
    let flow = sim.add_flow(new_cubic(), Duration::ZERO, ReceiverClock::default());
    sim.run(Duration::from_secs(180), INTERVAL);
    sim.print();
    let stats = sim.stats(flow);

    assert!(losses(&stats[6..]) > 0);
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
    let competing_from = Duration::from_secs(120);
    let mut sim = Simulation::new(Some((competing_from, LINK_RATE * 0.6)));
    let flow = sim.add_flow(new_ledbat(), Duration::ZERO, ReceiverClock::default());
    sim.run(Duration::from_secs(300), INTERVAL);
    sim.print();
    let stats = sim.stats(flow);

    let before = rate(&stats[6..12]);
    let after = rate(&stats[18..]);
    assert!(before > LINK_RATE * 0.95, "before={before}");
    // The other stream takes 60% of the link; LEDBAT keeps what is left without driving the
    // queue past its target.
    assert!(after < LINK_RATE * 0.45, "after={after}");
    assert!(after > LINK_RATE * 0.3, "after={after}");
    for s in &stats[18..] {
        assert_eq!(s.losses, 0, "{s:?}");
        assert!(s.mean_queuing_delay < Duration::from_millis(125), "{s:?}");
    }
}

#[test]
fn test_ledbat_yields_to_a_loss_based_flow() {
    // A CUBIC flow, which fills the queue until loss, joins a LEDBAT flow that has the link.
    let mut sim = Simulation::new(None);
    let ledbat = sim.add_flow(new_ledbat(), Duration::ZERO, ReceiverClock::default());
    let cubic = sim.add_flow(
        new_cubic(),
        Duration::from_secs(120),
        ReceiverClock::default(),
    );
    sim.run(Duration::from_secs(300), INTERVAL);
    sim.print();

    let before = rate(&sim.stats(ledbat)[6..12]);
    assert!(before > LINK_RATE * 0.95, "before={before}");
    let ledbat_after = rate(&sim.stats(ledbat)[15..]);
    let cubic_after = rate(&sim.stats(cubic)[15..]);
    assert!(
        ledbat_after < LINK_RATE * 0.1,
        "ledbat_after={ledbat_after}"
    );
    assert!(cubic_after > LINK_RATE * 0.85, "cubic_after={cubic_after}");
}

#[test]
fn test_two_ledbat_flows() {
    // A second LEDBAT flow starts while the first holds the queue at the target. The latecomer
    // measures its base delay with that queue in it, so it takes the larger share (the
    // "latecomer advantage", RFC 6817 §4.4); what matters here is that together they keep the
    // link busy without loss and without driving the queue to the drop-tail limit.
    let mut sim = Simulation::new(None);
    let first = sim.add_flow(new_ledbat(), Duration::ZERO, ReceiverClock::default());
    let second = sim.add_flow(
        new_ledbat(),
        Duration::from_secs(60),
        ReceiverClock::default(),
    );
    sim.run(Duration::from_secs(300), INTERVAL);
    sim.print();

    let total = rate(&sim.stats(first)[12..]) + rate(&sim.stats(second)[12..]);
    assert!(total > LINK_RATE * 0.95, "total={total}");
    for flow in [first, second] {
        let stats = &sim.stats(flow)[12..];
        assert_eq!(losses(stats), 0, "flow {flow}");
        for s in stats {
            assert!(s.mean_queuing_delay < Duration::from_millis(250), "{s:?}");
        }
    }
}
