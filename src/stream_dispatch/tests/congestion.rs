use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    CongestionConfig, CongestionControllerKind, SocketOpts, UtpSocket,
    raw::{Type::*, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::tests::{TestVsock, make_test_vsock},
    test_util::transport::RememberingTransport,
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, setup_test_logging},
    traits::UtpEnvironment,
};

// Test that congestion control works without testing specific algorithms.
// E.g. increases window on ACKs, reduces on loss etc.
#[tokio::test]
async fn test_congestion_control_basics() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            remote_inactivity_timeout: Some(Duration::from_secs(20)),
            vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(64 * 1024)),
            ..Default::default()
        },
        false,
    );

    let remote_wnd = 64 * 1024;

    // Allow sending by setting large window
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 99.into(),
            wnd_size: remote_wnd, // Large window to not interfere with congestion control
            ..Default::default()
        },
        "",
    );
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    let initial_window = t.vsock.congestion_controller.window();
    assert!(
        initial_window < remote_wnd as usize,
        "{initial_window} >= {remote_wnd}, should be less"
    );
    trace!(initial_window, remote_wnd);

    // Write a lot of data to test windowing
    let big_data = vec![0u8; 64 * 1024];
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&big_data)
        .await
        .unwrap();

    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    assert_eq!(
        initial_window,
        t.vsock.congestion_controller.window(),
        "window shouldn't have changed"
    );

    // Should only send up to initial window
    let sent = t.take_sent();
    let sent_bytes: usize = sent.iter().map(|m| m.payload().len()).sum::<usize>();
    assert!(
        sent_bytes <= initial_window,
        "Should respect initial window. sent_bytes={sent_bytes}, initial_window={initial_window}"
    );
    let first_batch_seq_nrs = sent.iter().map(|m| m.header.seq_nr).collect::<Vec<_>>();

    // ACK all packets - window should increase
    for seq_nr in &first_batch_seq_nrs {
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 1.into(),
                ack_nr: *seq_nr,
                wnd_size: remote_wnd,
                ..Default::default()
            },
            "",
        );
    }
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    let intermediate_window = t.vsock.congestion_controller.window();
    trace!(intermediate_window);

    assert!(
        intermediate_window > initial_window,
        "Window should grow after ACKs"
    );

    let sent = t.take_sent();
    assert!(
        !sent.is_empty(),
        "Should send more data due to increased window"
    );

    // Now simulate packet loss via duplicate ACKs
    let lost_seq_nr = *first_batch_seq_nrs.last().unwrap();
    for _ in 0..4 {
        t.send_msg(
            UtpHeader {
                htype: ST_STATE,
                seq_nr: 0.into(),
                ack_nr: lost_seq_nr,
                wnd_size: remote_wnd,
                ..Default::default()
            },
            "",
        );
    }
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    // Should trigger fast retransmit
    let resent = t.take_sent();
    assert!(
        !resent.is_empty(),
        "Should retransmit on triple duplicate ACK"
    );

    // Window should be reduced
    let window_after_loss = t.vsock.congestion_controller.window();
    trace!(window_after_loss);
    assert!(
        window_after_loss < intermediate_window,
        "Window should decrease after loss: intermediate_window={intermediate_window} window_after_loss={window_after_loss}"
    );

    // Simulate timeout by advancing time
    t.env.increment_now(Duration::from_secs(10));
    t.poll_once_assert_pending().await;

    // Should retransmit and reduce window further
    let resent = t.take_sent();
    assert!(!resent.is_empty(), "Should retransmit on timeout");

    let window_after_timeout = t.vsock.congestion_controller.window();
    assert!(
        window_after_timeout <= window_after_loss,
        "Window should decrease or stay same after timeout"
    );
}

/// An arbitrary raw `timestamp_difference_microseconds`: the peer's clock is unrelated to ours.
const BASE_DELAY: u32 = 3_000_000_000;
/// Time between sending a window of data and its ACKs arriving. Long enough to hold
/// [`QUEUING_DELAY`]: LEDBAT knows that a one-way delay cannot exceed the round trip.
const ROUND_TRIP: Duration = Duration::from_millis(300);
/// Queuing delay above LEDBAT's 100 ms target.
const QUEUING_DELAY: u32 = 250_000;
const REMOTE_WND: u32 = 1024 * 1024;

fn delay_test_opts(kind: CongestionControllerKind) -> SocketOpts {
    SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(20)),
        vsock_tx_bufsize_bytes_initial: Some(non_zero_const!(1024 * 1024)),
        congestion: CongestionConfig {
            kind,
            tracing: false,
        },
        ..Default::default()
    }
}

/// A packet from the remote that reports `delay` as the one-way delay of our packets.
fn remote_header(
    t: &TestVsock,
    htype: crate::raw::Type,
    seq_nr: u16,
    ack_nr: SeqNr,
    delay: u32,
) -> UtpHeader {
    UtpHeader {
        htype,
        connection_id: t.vsock.conn_id_send + 1,
        timestamp_microseconds: 1,
        timestamp_difference_microseconds: delay,
        wnd_size: REMOTE_WND,
        seq_nr: seq_nr.into(),
        ack_nr,
        extensions: Default::default(),
    }
}

/// An established outgoing connection with plenty of data queued: the first window is sent.
async fn start_sending(kind: CongestionControllerKind) -> TestVsock {
    let mut t = make_test_vsock(delay_test_opts(kind), false);
    // Opens the remote window; acknowledges nothing new (our SYN was 100).
    t.send_msg(remote_header(&t, ST_STATE, 1, 100.into(), BASE_DELAY), "");
    t.env.increment_now(ROUND_TRIP);
    t.poll_once_assert_pending().await;
    assert!(t.take_sent().is_empty());

    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![0u8; 512 * 1024])
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t
}

/// The remote acknowledges every data packet sent since the last round one by one, each ACK
/// carrying `delay`. Returns the window afterwards and the last acknowledged seq_nr.
async fn ack_round(t: &mut TestVsock, delay: u32) -> (usize, SeqNr) {
    let sent = t.take_sent();
    let data = sent
        .iter()
        .filter(|m| m.header.get_type() == ST_DATA)
        .map(|m| m.header.seq_nr)
        .collect::<Vec<_>>();
    assert!(!data.is_empty(), "nothing was sent in this round");
    t.env.increment_now(ROUND_TRIP);
    for seq_nr in &data {
        let header = remote_header(t, ST_STATE, 1, *seq_nr, delay);
        t.send_msg(header, "");
    }
    t.poll_once_assert_pending().await;
    (
        t.vsock.congestion_controller.window(),
        *data.last().unwrap(),
    )
}

// Delay samples in the headers of incoming packets reach LEDBAT: the window grows while they
// show no queuing delay and shrinks when they show more than the target.
#[tokio::test]
async fn test_ledbat_window_follows_delay_samples() {
    setup_test_logging();
    let mut t = start_sending(CongestionControllerKind::Ledbat).await;
    let initial_window = t.vsock.congestion_controller.window();

    let mut window = initial_window;
    for _ in 0..5 {
        let (w, _) = ack_round(&mut t, BASE_DELAY).await;
        assert!(w > window, "no queuing delay: grows, {w} <= {window}");
        window = w;
    }
    // Less than doubling per round: MTU probing pauses segmentation now and then.
    assert!(window >= initial_window * 8, "slow start: {window}");

    // 250 ms of queuing delay, 2.5 times the target. The current delay filter (the minimum of
    // the last few samples) sees it after a few ACKs; slow start ends there and from then on
    // the window shrinks every round.
    let (mut window, _) = ack_round(&mut t, BASE_DELAY + QUEUING_DELAY).await;
    for _ in 0..3 {
        let (w, _) = ack_round(&mut t, BASE_DELAY + QUEUING_DELAY).await;
        assert!(
            w < window,
            "queuing delay above target: shrinks, {w} >= {window}"
        );
        window = w;
    }
}

// Packets that acknowledge nothing new still carry valid delay samples (BEP 29).
#[tokio::test]
async fn test_ledbat_delay_samples_from_packets_without_new_acks() {
    setup_test_logging();
    let mut t = start_sending(CongestionControllerKind::Ledbat).await;
    let mut last_acked = SeqNr::from(100);
    for _ in 0..3 {
        (_, last_acked) = ack_round(&mut t, BASE_DELAY).await;
    }
    let window = t.vsock.congestion_controller.window();

    // The remote sends data, acknowledging nothing new, and reports a long queue.
    for seq_nr in 2..6 {
        let header = remote_header(&t, ST_DATA, seq_nr, last_acked, BASE_DELAY + QUEUING_DELAY);
        t.send_msg(header, "x");
    }
    t.poll_once_assert_pending().await;

    // These ACKs carry no sample at all (0: ignored), so only the data packets above tell
    // LEDBAT about the queue. Without them the window would double in this round.
    let (after, _) = ack_round(&mut t, 0).await;
    assert!(after < window, "{after} >= {window}");
}

// A chatty application (small writes, the second of each pair held back by Nagle until the
// first is acknowledged) does not use its window, so LEDBAT does not grow it.
#[tokio::test]
async fn test_ledbat_application_limited_with_nagle() {
    setup_test_logging();
    let mut t = make_test_vsock(delay_test_opts(CongestionControllerKind::Ledbat), false);
    assert!(t.vsock.socket_opts.nagle);
    t.send_msg(remote_header(&t, ST_STATE, 1, 100.into(), BASE_DELAY), "");
    t.env.increment_now(ROUND_TRIP);
    t.poll_once_assert_pending().await;
    let initial_window = t.vsock.congestion_controller.window();

    for _ in 0..10 {
        for msg in [b"hello", b"world"] {
            t.stream.as_mut().unwrap().write_all(msg).await.unwrap();
            t.poll_once_assert_pending().await;
        }
        ack_round(&mut t, BASE_DELAY).await;
    }
    assert_eq!(t.vsock.congestion_controller.window(), initial_window);
}

// Loss and retransmission timeouts reduce LEDBAT's window through the recovery module.
#[tokio::test]
async fn test_ledbat_loss_and_timeout() {
    setup_test_logging();
    let mut t = start_sending(CongestionControllerKind::Ledbat).await;
    let mut last_acked = SeqNr::from(100);
    for _ in 0..4 {
        (_, last_acked) = ack_round(&mut t, BASE_DELAY).await;
    }
    let window = t.vsock.congestion_controller.window();
    assert!(!t.take_sent().is_empty());

    // Triple duplicate ACK.
    t.env.increment_now(ROUND_TRIP);
    for _ in 0..4 {
        let header = remote_header(&t, ST_STATE, 1, last_acked, BASE_DELAY);
        t.send_msg(header, "");
    }
    t.poll_once_assert_pending().await;
    assert!(!t.take_sent().is_empty(), "fast retransmit");
    let after_loss = t.vsock.congestion_controller.window();
    assert_eq!(after_loss, window / 2, "halved");

    t.env.increment_now(Duration::from_secs(10));
    t.poll_once_assert_pending().await;
    assert!(!t.take_sent().is_empty(), "retransmission on timeout");
    let mss = t.vsock.congestion_controller.smss();
    assert_eq!(t.vsock.congestion_controller.window(), 2 * mss, "MIN_CWND");
}

// CUBIC does not look at delay samples: the same exchange with and without queuing delay
// produces the same windows.
#[tokio::test]
async fn test_cubic_ignores_delay_samples() {
    setup_test_logging();
    async fn windows(queuing_delay: u32) -> Vec<usize> {
        let mut t = start_sending(CongestionControllerKind::Cubic).await;
        let mut windows = Vec::new();
        for _ in 0..6 {
            let (w, _) = ack_round(&mut t, BASE_DELAY + queuing_delay).await;
            windows.push(w);
        }
        windows
    }
    assert_eq!(windows(0).await, windows(QUEUING_DELAY).await);
}

// We echo the one-way delay of the remote's packets in microseconds, wrapping around.
#[tokio::test]
async fn test_echoed_delay_in_microseconds() {
    setup_test_logging();
    let mut t = make_test_vsock(delay_test_opts(CongestionControllerKind::Ledbat), false);
    t.env.increment_now(Duration::from_millis(1234));
    let our_clock = (t.env.now() - t.vsock.socket_created).as_micros() as u32;
    // The remote's clock is ahead of ours, so the difference wraps around.
    let remote_timestamp = our_clock.wrapping_add(u32::MAX - 12_344);
    t.send_msg(
        UtpHeader {
            timestamp_microseconds: remote_timestamp,
            ..remote_header(&t, ST_DATA, 2, 100.into(), BASE_DELAY)
        },
        "hello",
    );
    t.poll_once_assert_pending().await;

    let sent = t.take_sent();
    let ack = sent
        .iter()
        .find(|m| m.header.get_type() == ST_STATE)
        .expect("the data was acknowledged");
    assert_eq!(ack.header.timestamp_microseconds, our_clock);
    assert_eq!(ack.header.timestamp_difference_microseconds, 12_345);
}

// The SYN carries a microsecond timestamp like every other packet, so the delay the remote
// echoes back for it is comparable with later samples.
#[tokio::test]
async fn test_syn_timestamp_in_microseconds() {
    setup_test_logging();
    let env = MockUtpEnvironment::new();
    let transport = RememberingTransport::new(ADDR_1);
    let socket =
        UtpSocket::new_with_opts(transport.clone(), env.clone(), SocketOpts::default()).unwrap();
    env.increment_now(Duration::from_micros(1_500_123));

    let connect = tokio::spawn({
        let socket = socket.clone();
        async move { socket.connect(ADDR_2).await }
    });
    let syn = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(syn) = transport.take_sent_utpmessages().into_iter().next() {
                return syn;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the SYN was not sent");
    connect.abort();

    assert_eq!(syn.header.get_type(), ST_SYN);
    assert_eq!(syn.header.timestamp_microseconds, 1_500_123);
    assert_eq!(syn.header.timestamp_difference_microseconds, 0);
}
