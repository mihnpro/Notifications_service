use tokio_util::sync::CancellationToken;

pub fn install() -> CancellationToken {
    let token = CancellationToken::new();
    let listener = token.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => tracing::info!("ctrl_c received"),
            Err(e) => tracing::error!(error = %e, "failed to listen for ctrl_c"),
        }
        listener.cancel();
    });
    token
}
