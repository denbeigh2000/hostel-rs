use std::sync::Arc;

use futures::{Future, FutureExt, StreamExt};
use thrussh::server::{run_stream, Config, Server};
use tokio::net::TcpListener;

pub async fn serve<S, F>(mut server: S, stop: F, listener: TcpListener, config: Arc<Config>)
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
