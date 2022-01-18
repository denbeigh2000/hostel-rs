use std::sync::Arc;

use futures::{channel::oneshot, Future, FutureExt, StreamExt};
use thrussh::server::{run_stream, Config};
use tokio::net::TcpListener;
use tokio::net::ToSocketAddrs;

use crate::hostel::Server;

pub async fn serve<A, S>(bind_addr: A, serv: S, config: Config)
where
    A: ToSocketAddrs,
    S: Server + 'static + Send,
{
    let mut conn_rx = serv.remaining_connections();
    let listener = TcpListener::bind(&bind_addr).await.unwrap();
    let (stop_tx, stop_rx) = oneshot::channel();
    let stop_rx = stop_rx.map(|_: Result<(), _>| ());

    let serv_fut = run_server(serv, stop_rx, listener, Arc::new(config));
    let serv_hand = tokio::spawn(serv_fut);

    tokio::select! {
        _ = serv_hand => {
            log::info!("Port closed/error serving, shutting down");
        },
        _ = tokio::signal::ctrl_c() => {
            log::info!("received ^c");
            stop_tx.send(()).unwrap();
            let count = conn_rx.borrow_and_update();
            if *count == 0 {
                log::info!("no connected clients, shutting down");
                return;
            }

            log::info!("{} connected clients, waiting to shut down", *count);
            // this holds a read lock - make sure it doesn't stay in the loop's
            // scope
            drop(count);

            while conn_rx.changed().await.is_ok() {
                let count = conn_rx.borrow();
                match *count {
                    n @ 2.. => log::info!("{} clients still connected", n),
                    1 => log::info!("1 client still connected"),
                    0 => {
                        log::info!("All clients disconnected, shutting down");
                        return;
                    },
                    _ => unreachable!(),
                }
            }
        },
    }
}

pub async fn run_server<S, F>(mut server: S, stop: F, listener: TcpListener, config: Arc<Config>)
where
    S: Server + 'static,
    F: Future<Output = ()> + Unpin,
{
    let mut stop_str = stop.into_stream();
    loop {
        tokio::select! {
            socket_result = listener.accept() => {
                match socket_result {
                    Ok((sock, addr)) => {
                        let c = Arc::clone(&config);
                        let handler = server.new(Some(addr));
                        tokio::spawn(run_stream(c, sock, handler));
                    },
                    Err(_) => {
                        // TODO: Useful error detection/logging?
                        return;
                    }
                }
            },
            _ = stop_str.next() => {
                log::info!("stopped listening for incoming connections");
                break;
            }
        }
    }
}
