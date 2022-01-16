use bollard::Docker;
use hostel::docker::DockerServer;

use std::sync::Arc;
use std::time::Duration;

use thrussh::server::{self, Config};
use thrussh::MethodSet;
use thrussh_keys::key::KeyPair;

#[tokio::main]
async fn main() {
    let mut config = Config::default();
    config.auth_rejection_time = Duration::from_secs(5);
    config.keys.push(KeyPair::generate_ed25519().unwrap());
    config.methods = MethodSet::empty();
    config.methods.insert(MethodSet::NONE);
    config.methods.insert(MethodSet::KEYBOARD_INTERACTIVE);

    let docker = Docker::connect_with_unix_defaults().unwrap();
    let serv = DockerServer::new(Arc::new(docker));

    server::run(config.into(), "0.0.0.0:2222", serv)
        .await
        .unwrap();
}
