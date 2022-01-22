use std::net::SocketAddr;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bb8_redis::bb8::Pool;
use bb8_redis::{redis, RedisConnectionManager};
use bollard::Docker;
use clap::Parser;
use hostel::active::ResourcePool;
use hostel::config;
use hostel::hostel::HostelServer;
use hostel::server;
use hostel::utils::logging;
use thrussh::server::Config as ThrusshConfig;
use thrussh::MethodSet;
use thrussh_keys::key::KeyPair;

fn parse_duration(dur_ms_str: &str) -> Result<Duration, ParseIntError> {
    let dur_ms = dur_ms_str.parse()?;
    Ok(Duration::from_millis(dur_ms))
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to hostel configuration directory
    #[clap(short, long, env, default_value = ".")]
    config_dir: PathBuf,

    /// Socket address to bind server to
    #[clap(short, long, env, default_value = "0.0.0.0:2222")]
    bind_addr: SocketAddr,

    /// URL of redis address
    #[clap(short, long, env, default_value = "redis://localhost:6379")]
    redis_url: redis::ConnectionInfo,

    /// Logging verbosity
    #[clap(short, long, env, default_value = "info")]
    log_level: simplelog::LevelFilter,

    /// SSH Auth rejection timeout (in ms)
    #[clap(name = "SSH_AUTH_TIMEOUT_MS", short = 't', long = "ssh-auth-timeout-ms", env, parse(try_from_str = parse_duration), default_value = "5000")]
    ssh_auth_timeout: Duration,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    logging::init(args.log_level);

    let hostel_config = config::MetaConfig::load(args.config_dir).await.unwrap();

    let key_path = hostel_config.server_key_path();
    let key = if key_path.exists() {
        thrussh_keys::load_secret_key(key_path, None).unwrap()
    } else {
        let key_str = key_path.to_string_lossy();
        log::warn!("No private key found at {key_str}");
        log::warn!("Generating a random key");

        let (_, secret_key) = thrussh_keys::key::ed25519::keypair();
        KeyPair::Ed25519(secret_key)
    };

    let config = ThrusshConfig {
        methods: MethodSet::from_iter([MethodSet::PUBLICKEY]),
        auth_rejection_time: args.ssh_auth_timeout,
        keys: vec![key],
        ..Default::default()
    };

    let mgr = RedisConnectionManager::new(args.redis_url).unwrap();
    let pool = Pool::builder().build(mgr).await.unwrap();
    let pool = ResourcePool::new(pool);
    let docker = Docker::connect_with_unix_defaults().unwrap();
    let serv = HostelServer::new(Arc::new(docker), hostel_config, Arc::new(pool));

    log::info!("serving on {}", &args.bind_addr);

    server::serve(args.bind_addr, serv, config).await;
}
