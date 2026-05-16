use anyhow::{Context, Result};
use lapin::{Connection, ConnectionProperties};

use crate::config::RmqConfig;

pub async fn connect(cfg: &RmqConfig) -> Result<Connection> {
    Connection::connect(&cfg.url, ConnectionProperties::default())
        .await
        .with_context(|| format!("connect rabbitmq at {}", cfg.url))
}
