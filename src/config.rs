use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use thrussh_keys::key::PublicKey;
use thrussh_keys::parse_public_key_base64;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

#[derive(Debug, Error)]
pub enum FileParseError {
    #[error("IO error: {0}")]
    IO(#[from] tokio::io::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] serde_yaml::Error),
}

#[derive(Debug, Error)]
pub enum KeyLoadError {
    #[error("IO error: {0}")]
    IO(#[from] tokio::io::Error),
}

#[derive(Debug, Error)]
pub enum ConfigLoadError {
    #[error("loading config file: {0}")]
    FileParse(#[from] FileParseError),
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub image: String,
}

pub struct MetaConfig {
    pub config: Config,

    config_dir: PathBuf,
}

impl MetaConfig {
    pub async fn load<P: Into<PathBuf>>(config_dir: P) -> Result<Self, ConfigLoadError> {
        let config_dir = config_dir.into();
        let config_file = config_dir.join("config.yaml");

        let mut f = File::open(&config_file)
            .await
            .map_err(FileParseError::from)?;
        let mut buf = Vec::new();
        if let Err(e) = f.read_to_end(&mut buf).await {
            panic!("failed to read file: {e}");
        }

        let config: Config = serde_yaml::from_slice(&buf).map_err(FileParseError::from)?;

        Ok(Self { config, config_dir })
    }

    pub fn server_key_path(&self) -> PathBuf {
        self.config_dir.join("id_ed25519")
    }

    pub fn user_config_path(&self, user: &str) -> PathBuf {
        self.config_dir.join("users").join(user).join("config.yaml")
    }

    pub async fn user_config(&self, user: &str) -> Result<Option<UserConfig>, FileParseError> {
        let path = self.user_config_path(user);
        if !path.exists() {
            return Ok(None);
        }

        let mut f = File::open(&path).await?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await?;
        let data: UserConfig = serde_yaml::from_slice(&buf)?;

        Ok(Some(data))
    }

    fn user_key_path(&self, user: &str) -> PathBuf {
        self.config_dir.join("users").join(user).join("authorized_keys")
    }

    pub async fn user_keys(&self, user: &str) -> Result<Option<Vec<PublicKey>>, KeyLoadError> {
        let key_path = self.user_key_path(user);
        if !key_path.exists() {
            log::info!("no keys for {user} at {}", key_path.to_string_lossy());
            return Ok(None);
        }

        let f = File::open(key_path).await?;

        let mut lines = BufReader::new(f).lines();
        let mut keys = Vec::new();
        while let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

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
                Err(e) => log::warn!("failed to parse key: {e}"),
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
