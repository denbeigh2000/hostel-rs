use std::sync::Arc;
use std::time::Duration;

use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use bollard::Docker;
use hostel::active::ResourcePool;
use hostel::config;
use hostel::hostel::HostelServer;
use hostel::server;
use simplelog::{LevelFilter, TermLogger};
use thrussh::server::Config as ThrusshConfig;
use thrussh::MethodSet;
use thrussh_keys::key::KeyPair;

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

    let hostel_config = config::MetaConfig::load(".").await.unwrap();

    let mgr = RedisConnectionManager::new("redis://localhost:6379").unwrap();
    let pool = Pool::builder().build(mgr).await.unwrap();
    let pool = ResourcePool::new(pool);
    let docker = Docker::connect_with_unix_defaults().unwrap();
    let serv = HostelServer::new(Arc::new(docker), hostel_config, Arc::new(pool));

    let bind_addr = "0.0.0.0:2222";
    log::info!("serving on {}", &bind_addr);

    server::serve(bind_addr, serv, config).await;
}
