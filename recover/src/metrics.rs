use anyhow::{anyhow, Result};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub fn install() -> Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| anyhow!("install prometheus recorder: {e}"))
}
