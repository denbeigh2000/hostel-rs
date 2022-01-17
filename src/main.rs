use std::sync::Arc;
use std::time::Duration;

use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use bollard::Docker;
use hostel::active::ResourcePool;
use hostel::docker::DockerServer;
use thrussh::server::{self, Config};
use thrussh::MethodSet;
use thrussh_keys::key::KeyPair;

#[tokio::main]
async fn main() {
    let config = Config {
        methods: MethodSet::from_iter([MethodSet::NONE, MethodSet::KEYBOARD_INTERACTIVE]),
        auth_rejection_time: Duration::from_secs(5),
        keys: vec![KeyPair::generate_ed25519().unwrap()],
        ..Default::default()
    };

    let mgr = RedisConnectionManager::new("redis://localhost:6379").unwrap();
    let pool = Pool::builder().build(mgr).await.unwrap();
    let pool = ResourcePool::new(pool);
    let docker = Docker::connect_with_unix_defaults().unwrap();
    let serv = DockerServer::new(Arc::new(docker), Arc::new(pool));

    server::run(config.into(), "0.0.0.0:2222", serv)
        .await
        .unwrap();
}
