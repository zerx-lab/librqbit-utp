mod lossy_socket;

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use libutp_rs2::UtpUdpContext;
use lossy_socket::LossyUtpUdpSocket;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
    try_join,
};
use tracing::{Instrument, error_span, info};

use crate::{
    CongestionConfig, CongestionControllerKind, SocketOpts, UtpSocketUdp,
    test_util::setup_test_logging,
};

trait AcceptConnect {
    /// `congestion` selects our congestion controller; other implementations ignore it.
    async fn bind(addr: SocketAddr, congestion: CongestionControllerKind) -> Self;
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin);
    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin);
    fn bind_addr(&self) -> Option<SocketAddr>;
}

impl AcceptConnect for Arc<UtpSocketUdp> {
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let s = UtpSocketUdp::accept(self).await.unwrap();
        s.split()
    }

    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        UtpSocketUdp::connect(self, addr).await.unwrap().split()
    }

    async fn bind(addr: SocketAddr, congestion: CongestionControllerKind) -> Self {
        UtpSocketUdp::new_udp_with_opts(
            addr,
            SocketOpts {
                congestion: CongestionConfig {
                    kind: congestion,
                    tracing: true,
                },
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap()
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        Some(self.transport.bind_addr())
    }
}

impl AcceptConnect for Arc<UtpUdpContext> {
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let stream = UtpUdpContext::accept(self).await.unwrap();
        tokio::io::split(stream)
    }

    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let s = UtpUdpContext::connect(self, addr).await.unwrap();
        tokio::io::split(s)
    }

    async fn bind(addr: SocketAddr, _congestion: CongestionControllerKind) -> Self {
        UtpUdpContext::new_udp(addr).await.unwrap()
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        None
    }
}

pub const TIMEOUT: Duration = Duration::from_secs(50);

async fn echo(
    reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let mut reader = reader;

    const MAX_COUNTER: u64 = 10_000;
    const PRINT_EVERY: u64 = 1_000;

    let reader = async move {
        for expected in 0..=MAX_COUNTER {
            let current = timeout(TIMEOUT, reader.read_u64())
                .await
                .context("timeout reading")?
                .context("error reading")?;
            if current != expected {
                anyhow::bail!("expected {expected}, got {current}");
            }

            if current % PRINT_EVERY == 0 {
                info!("current counter {current}");
            }
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    let writer = async move {
        let mut writer = writer;
        for counter in 0..=MAX_COUNTER {
            timeout(TIMEOUT, writer.write_u64(counter))
                .await
                .context("timeout writing")?
                .context("error writing")?;
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    try_join!(reader, writer)?;
    Ok(())
}

async fn test_one_echo<T1: AcceptConnect, T2: AcceptConnect>(
    server_addr: SocketAddr,
    client_addr: SocketAddr,
    connect_ip: Option<IpAddr>,
    congestion: CongestionControllerKind,
) {
    setup_test_logging();

    let (server, client) = tokio::join!(
        T1::bind(server_addr, congestion),
        T2::bind(client_addr, congestion)
    );
    let server_port = server
        .bind_addr()
        .map(|a| a.port())
        .unwrap_or(server_addr.port());
    let connect_ip = connect_ip.unwrap_or(server_addr.ip());
    let connect_addr = SocketAddr::from((connect_ip, server_port));

    tracing::debug!(s1_bind_addr=?server.bind_addr(), s2_bind_addr=?client.bind_addr(), connect_addr=?connect_addr);

    let server = async {
        let s = server;
        let (r, w) = s.accept().await;
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", addr = ?server_addr));

    let client = async {
        let s = client;
        let (r, w) = s.connect(connect_addr).await;
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", addr = ?client_addr));

    tokio::join!(server, client);
}

fn localhost_ipv4(port: u16) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, port).into()
}

fn localhost_ipv6(port: u16) -> SocketAddr {
    (Ipv6Addr::LOCALHOST, port).into()
}

fn unspecified_ipv6(port: u16) -> SocketAddr {
    (Ipv6Addr::UNSPECIFIED, port).into()
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
        CongestionControllerKind::Cubic,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_ledbat() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
        CongestionControllerKind::Ledbat,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_ipv6() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv6(0),
        localhost_ipv6(0),
        None,
        CongestionControllerKind::Cubic,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_ipv6_ledbat() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv6(0),
        localhost_ipv6(0),
        None,
        CongestionControllerKind::Ledbat,
    )
    .await;
}

async fn dualstack(congestion: CongestionControllerKind) {
    let f = |server_addr, client_addr, connect_ip| {
        test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
            server_addr,
            client_addr,
            connect_ip,
            congestion,
        )
    };

    f(
        unspecified_ipv6(0),
        unspecified_ipv6(0),
        Some(Ipv4Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack both over IPv4"))
    .await;

    f(localhost_ipv6(0), localhost_ipv6(0), None).await;

    f(
        unspecified_ipv6(0),
        unspecified_ipv6(0),
        Some(Ipv6Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack both over IPv6"))
    .await;

    f(
        unspecified_ipv6(0),
        localhost_ipv4(0),
        Some(Ipv4Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack server over IPv4"))
    .await;

    f(
        unspecified_ipv6(0),
        localhost_ipv6(0),
        Some(Ipv6Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack server over IPv6"))
    .await;

    f(
        localhost_ipv4(0),
        unspecified_ipv6(0),
        Some(Ipv4Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack client over IPv4"))
    .await;

    f(
        localhost_ipv6(0),
        localhost_ipv6(0),
        Some(Ipv6Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("ipv6-only localhost"))
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_dualstack() {
    dualstack(CongestionControllerKind::Cubic).await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_dualstack_ledbat() {
    dualstack(CongestionControllerKind::Ledbat).await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_libutp_rs2_server() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<libutp_rs2::UtpUdpContext>>(
        localhost_ipv4(8530),
        localhost_ipv4(8531),
        None,
        CongestionControllerKind::Cubic,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_libutp_rs2_server_ledbat() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<libutp_rs2::UtpUdpContext>>(
        localhost_ipv4(8532),
        localhost_ipv4(8533),
        None,
        CongestionControllerKind::Ledbat,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_librqbit_utp_server() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv4(8534),
        localhost_ipv4(8535),
        None,
        CongestionControllerKind::Cubic,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_librqbit_utp_server_ledbat() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv4(8538),
        localhost_ipv4(8539),
        None,
        CongestionControllerKind::Ledbat,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_libutp_rs2_server() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<libutp_rs2::UtpUdpContext>>(
        localhost_ipv4(8536),
        localhost_ipv4(8537),
        None,
        CongestionControllerKind::Cubic,
    )
    .await;
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_5_pct() {
    test_one_echo::<LossyUtpUdpSocket<5>, LossyUtpUdpSocket<5>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
        CongestionControllerKind::Cubic,
    )
    .await
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_5_pct_ledbat() {
    test_one_echo::<LossyUtpUdpSocket<5>, LossyUtpUdpSocket<5>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
        CongestionControllerKind::Ledbat,
    )
    .await
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_20_pct() {
    test_one_echo::<LossyUtpUdpSocket<20>, LossyUtpUdpSocket<20>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
        CongestionControllerKind::Cubic,
    )
    .await
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_20_pct_ledbat() {
    test_one_echo::<LossyUtpUdpSocket<20>, LossyUtpUdpSocket<20>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
        CongestionControllerKind::Ledbat,
    )
    .await
}

/// Send `len` bytes over loopback and return how long it took until the receiver had them all.
async fn bulk_transfer(congestion: CongestionControllerKind, len: usize) -> Duration {
    let (server, client) = tokio::join!(
        <Arc<UtpSocketUdp> as AcceptConnect>::bind(localhost_ipv4(0), congestion),
        <Arc<UtpSocketUdp> as AcceptConnect>::bind(localhost_ipv4(0), congestion),
    );
    let server_addr = UtpSocketUdp::bind_addr(&server);
    let started = Instant::now();

    let receive = async {
        let (mut r, _w) = AcceptConnect::accept(&server).await;
        let mut buf = vec![0u8; 64 * 1024];
        let mut received = 0;
        while received < len {
            let n = r.read(&mut buf).await.context("error reading")?;
            anyhow::ensure!(n > 0, "EOF after {received} of {len} bytes");
            received += n;
        }
        Ok::<_, anyhow::Error>(started.elapsed())
    };
    let send = async {
        let (_r, mut w) = AcceptConnect::connect(&client, server_addr).await;
        let chunk = vec![0u8; 64 * 1024];
        let mut sent = 0;
        while sent < len {
            let n = chunk.len().min(len - sent);
            w.write_all(&chunk[..n]).await.context("error writing")?;
            sent += n;
        }
        // Keep the connection open until the receiver has everything.
        Ok::<_, anyhow::Error>(w)
    };

    let (elapsed, _w) = timeout(TIMEOUT, async { try_join!(receive, send) })
        .await
        .expect("timeout")
        .unwrap();
    elapsed
}

// Without queuing delay (loopback) LEDBAT must not crawl: it should be about as fast as CUBIC.
#[tokio::test]
async fn e2e_test_ledbat_loopback_throughput_comparable_to_cubic() {
    setup_test_logging();
    const LEN: usize = 8 * 1024 * 1024;
    let cubic = bulk_transfer(CongestionControllerKind::Cubic, LEN).await;
    let ledbat = bulk_transfer(CongestionControllerKind::Ledbat, LEN).await;
    info!(?cubic, ?ledbat, "8 MiB over loopback");
    // Generous: real sockets on a machine that may be busy running other tests.
    assert!(
        ledbat < cubic * 2 + Duration::from_secs(1),
        "LEDBAT took {ledbat:?}, CUBIC {cubic:?}"
    );
}

async fn assert_connects(client: &Arc<UtpSocketUdp>, server: &Arc<UtpSocketUdp>) {
    timeout(Duration::from_secs(5), async {
        try_join!(
            UtpSocketUdp::accept(server),
            UtpSocketUdp::connect(client, UtpSocketUdp::bind_addr(server))
        )
    })
    .await
    .expect("timed out connecting")
    .expect("the socket stopped working");
}

// On Windows, an ICMP port unreachable that comes back for an earlier send_to makes the next
// recv_from on the same UDP socket fail with WSAECONNRESET (os error 10054).
#[tokio::test]
async fn e2e_test_connect_after_icmp_port_unreachable() {
    setup_test_logging();
    let client = UtpSocketUdp::new_udp(localhost_ipv4(0)).await.unwrap();
    let server = UtpSocketUdp::new_udp(localhost_ipv4(0)).await.unwrap();

    // Nothing listens on this port once the socket is dropped, so the SYN sent there gets an
    // ICMP port unreachable back.
    let closed = std::net::UdpSocket::bind(localhost_ipv4(0))
        .unwrap()
        .local_addr()
        .unwrap();
    let _ = timeout(
        Duration::from_millis(500),
        UtpSocketUdp::connect(&client, closed),
    )
    .await;

    assert_connects(&client, &server).await;
}

// On Windows, a datagram longer than the receive buffer makes recv_from fail with
// WSAEMSGSIZE (os error 10040).
#[tokio::test]
async fn e2e_test_connect_after_oversized_datagram() {
    setup_test_logging();
    let client = UtpSocketUdp::new_udp(localhost_ipv4(0)).await.unwrap();
    let server = UtpSocketUdp::new_udp(localhost_ipv4(0)).await.unwrap();

    // Some platforms refuse to send a datagram this large, which is fine.
    let sender = tokio::net::UdpSocket::bind(localhost_ipv4(0))
        .await
        .unwrap();
    let _ = sender
        .send_to(&[0u8; 20_000], UtpSocketUdp::bind_addr(&client))
        .await;

    assert_connects(&client, &server).await;
}
