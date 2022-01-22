use std::convert::Infallible;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thrussh_keys::key::PublicKey;
use thrussh_keys::parse_public_key_base64;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub image: String,
}

pub struct MetaConfig {
    pub config: Config,

    config_dir: PathBuf,
}

impl MetaConfig {
    pub async fn load<P: Into<PathBuf>>(config_dir: P) -> Result<Self, Infallible> {
        let config_dir = config_dir.into();
        let config_file = config_dir.join("config.yaml");

        eprintln!("{}", config_file.to_str().unwrap());
        let mut f = File::open(&config_file).await.unwrap();
        let mut buf = Vec::new();
        if let Err(e) = f.read_to_end(&mut buf).await {
            panic!("failed to read file: {}", e);
        }

        let config: Config = serde_yaml::from_slice(&buf).unwrap();

        Ok(Self { config, config_dir })
    }

    fn user_key_path(&self, user: &str) -> PathBuf {
        self.config_dir.join(user).join("authorized_keys")
    }

    pub async fn user_keys(&self, user: &str) -> Result<Option<Vec<PublicKey>>, Infallible> {
        let key_path = self.user_key_path(user);
        if !key_path.exists() {
            eprintln!("no keys for {}", user);
            return Ok(None);
        }

        let f = File::open(key_path).await.unwrap();

        let mut lines = BufReader::new(f).lines();
        let mut keys = Vec::new();
        while let Some(line) = lines.next_line().await.unwrap() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            log::info!("parsing: {}", line);
            let mut split = line.split_whitespace();
            let res = match (split.next(), split.next()) {
                (Some(_), Some(key)) => parse_public_key_base64(key),
                (Some(key), None) => thrussh_keys::parse_public_key_base64(key),
                _ => {
                    log::warn!("invalid key {line}, skipping");
                    continue;
                }
            };

            match res {
                Ok(key) => keys.push(key),
                Err(e) => log::warn!("failed to parse key: {}", e),
            }
        }

        assert!(!keys.is_empty());
        Ok(Some(keys))
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserConfig {
    pub image: Option<String>,
}
