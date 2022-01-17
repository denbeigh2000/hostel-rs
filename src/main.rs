use std::sync::Arc;
use std::time::Duration;

use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use bollard::Docker;
use futures::FutureExt;
use hostel::active::ResourcePool;
use hostel::hostel::HostelServer;
use hostel::server;
use simplelog::{LevelFilter, TermLogger};
use thrussh::server::Config as ThrusshConfig;
use thrussh::MethodSet;
use thrussh_keys::key::KeyPair;
use tokio::sync::{oneshot, watch};

#[tokio::main]
async fn main() {
    TermLogger::init(
        LevelFilter::Info,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )
    .unwrap();

    let config = ThrusshConfig {
        methods: MethodSet::from_iter([MethodSet::NONE, MethodSet::KEYBOARD_INTERACTIVE]),
        auth_rejection_time: Duration::from_secs(5),
        keys: vec![KeyPair::generate_ed25519().unwrap()],
        ..Default::default()
    };

    let mgr = RedisConnectionManager::new("redis://localhost:6379").unwrap();
    let pool = Pool::builder().build(mgr).await.unwrap();
    let pool = ResourcePool::new(pool);
    let docker = Docker::connect_with_unix_defaults().unwrap();
    let (conn_tx, mut conn_rx) = watch::channel(8);
    let serv = HostelServer::new(Arc::new(docker), Arc::new(conn_tx), Arc::new(pool));

    let bind_addr = "0.0.0.0:2222";
    log::info!("serving on {}", &bind_addr);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    let (stop_tx, stop_rx) = oneshot::channel();
    let stop_rx = stop_rx.map(|_: Result<(), _>| ());
    let serv_fut = server::serve(serv, stop_rx, listener, Arc::new(config));
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
