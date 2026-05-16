use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub instance_id: String,
    pub server: ServerConfig,
    pub db: DbConfig,
    pub rmq: RmqConfig,
    pub regions: RegionsConfig,
    pub jobs: JobsConfig,
    pub retry: RetryConfig,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub bind: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout_ms: u64,
    #[serde(default)]
    #[allow(dead_code)]
    pub statement_cache_capacity: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RmqConfig {
    pub url: String,
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RegionsConfig {
    pub ids: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JobsConfig {
    pub outbox_recovery: JobConfig,
    pub lease_recovery: JobConfig,
    pub retry_scanner: JobConfig,
    pub campaign_finalizer: JobConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JobConfig {
    pub enabled: bool,
    pub batch_size: u32,
    pub idle_interval_ms: u64,
    pub busy_interval_ms: u64,
    pub mid_interval_ms: u64,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_concurrency() -> usize {
    8
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetryConfig {
    pub buckets_seconds: Vec<u64>,
    #[serde(default = "default_grace_seconds")]
    pub repush_grace_seconds: i64,
    #[serde(default = "default_force_retry_ceiling")]
    pub force_retry_max_attempts_ceiling: i32,
}

fn default_grace_seconds() -> i64 {
    30
}

fn default_force_retry_ceiling() -> i32 {
    20
}

#[derive(Debug, Deserialize, Clone)]
pub struct TelemetryConfig {
    pub log_format: String,
    pub log_level: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("APP__").split("__"))
            .extract::<Self>()
            .with_context(|| format!("failed to load config from {path}"))
    }
}

pub fn init_tracing(cfg: &TelemetryConfig) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter =
        EnvFilter::try_new(&cfg.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry().with(filter);

    if cfg.log_format == "json" {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer().compact()).init();
    }
}
