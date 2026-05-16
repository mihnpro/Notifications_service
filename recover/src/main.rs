use anyhow::Result;
use clap::{Parser, Subcommand};
use std::sync::Arc;

use recover::{chaos, config, db, jobs, metrics, rmq, server, shutdown, state};

use recover::jobs::{
    finalizer::CampaignFinalizer, lease::LeaseRecovery, outbox::OutboxRecovery,
    outbox_failed::OutboxFailedScanner, retry_scanner::RetryScanner, ServiceContext,
};

#[derive(Parser)]
#[command(name = "recover", version, about = "Notification recovery service")]
struct Cli {
    #[arg(long, env = "RECOVER_CONFIG", default_value = "config/default.toml")]
    config: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Serve,
    Once {
        #[arg(long)]
        job: String,
    },
    Chaos {
        #[command(subcommand)]
        action: ChaosAction,
    },
}

#[derive(Subcommand)]
enum ChaosAction {
    StuckLeases {
        #[arg(long, default_value_t = 100)]
        count: u32,
    },
    StuckOutbox {
        #[arg(long, default_value_t = 50)]
        count: u32,
    },
    OrphanRetry {
        #[arg(long, default_value_t = 20)]
        count: u32,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;
    config::init_tracing(&cfg.telemetry);

    tracing::info!(instance_id = %cfg.instance_id, "recover starting");

    match cli.cmd {
        Cmd::Serve => run_serve(cfg).await,
        Cmd::Once { job } => run_once(cfg, &job).await,
        Cmd::Chaos { action } => run_chaos(cfg, action).await,
    }
}

async fn run_serve(cfg: config::Config) -> Result<()> {
    let shutdown = shutdown::install();

    let pool = db::connect(&cfg.db).await?;
    tracing::info!("postgres pool ready");

    let _rmq = if cfg.rmq.enabled {
        let conn = rmq::connect(&cfg.rmq).await?;
        tracing::info!("rabbitmq connection ready");
        Some(conn)
    } else {
        tracing::info!("rabbitmq disabled (recovery emits retries via outbox)");
        None
    };

    let metrics_handle = metrics::install()?;
    let registry = state::Registry::new();
    let cfg_arc = Arc::new(cfg.clone());

    let ctx = ServiceContext::new(pool.clone(), cfg.clone(), registry.clone());

    let app_state = server::AppState {
        pool: pool.clone(),
        metrics: metrics_handle,
        registry: registry.clone(),
        cfg: cfg_arc.clone(),
    };

    let http_task = tokio::spawn(server::serve(
        cfg.server.bind.clone(),
        app_state,
        shutdown.clone(),
    ));

    let outbox_task = tokio::spawn(jobs::run_job(
        OutboxRecovery {
            cfg: cfg.jobs.outbox_recovery.clone(),
        },
        ctx.clone(),
        shutdown.clone(),
    ));
    let lease_task = tokio::spawn(jobs::run_job(
        LeaseRecovery {
            cfg: cfg.jobs.lease_recovery.clone(),
        },
        ctx.clone(),
        shutdown.clone(),
    ));
    let retry_task = tokio::spawn(jobs::run_job(
        RetryScanner {
            cfg: cfg.jobs.retry_scanner.clone(),
        },
        ctx.clone(),
        shutdown.clone(),
    ));
    let finalizer_task = tokio::spawn(jobs::run_job(
        CampaignFinalizer {
            cfg: cfg.jobs.campaign_finalizer.clone(),
        },
        ctx.clone(),
        shutdown.clone(),
    ));
    let outbox_failed_task = tokio::spawn(jobs::run_job(
        OutboxFailedScanner {
            cfg: cfg.jobs.outbox_failed_scanner.clone(),
        },
        ctx.clone(),
        shutdown.clone(),
    ));
    let backlog_task = tokio::spawn(jobs::backlog::run(ctx.clone(), shutdown.clone()));

    shutdown.cancelled().await;
    tracing::info!("shutdown signal received");

    let _ = tokio::join!(
        outbox_task,
        lease_task,
        retry_task,
        finalizer_task,
        outbox_failed_task,
        backlog_task
    );
    if let Err(e) = http_task.await {
        tracing::error!(error = %e, "http task panicked");
    }
    pool.close().await;
    tracing::info!("recover stopped");
    Ok(())
}

async fn run_once(cfg: config::Config, job: &str) -> Result<()> {
    use jobs::Job;

    let pool = db::connect(&cfg.db).await?;
    let _metrics_handle = metrics::install().ok();
    let registry = state::Registry::new();
    let ctx = ServiceContext::new(pool.clone(), cfg.clone(), registry);

    let outcome = match job {
        "outbox_recovery" => {
            OutboxRecovery {
                cfg: cfg.jobs.outbox_recovery.clone(),
            }
            .tick(&ctx)
            .await
        }
        "lease_recovery" => {
            LeaseRecovery {
                cfg: cfg.jobs.lease_recovery.clone(),
            }
            .tick(&ctx)
            .await
        }
        "retry_scanner" => {
            RetryScanner {
                cfg: cfg.jobs.retry_scanner.clone(),
            }
            .tick(&ctx)
            .await
        }
        "campaign_finalizer" => {
            CampaignFinalizer {
                cfg: cfg.jobs.campaign_finalizer.clone(),
            }
            .tick(&ctx)
            .await
        }
        "outbox_failed_scanner" => {
            OutboxFailedScanner {
                cfg: cfg.jobs.outbox_failed_scanner.clone(),
            }
            .tick(&ctx)
            .await
        }
        other => {
            tracing::error!(job = other, "unknown job");
            pool.close().await;
            return Ok(());
        }
    };

    match outcome {
        Ok(jobs::JobOutcome::Processed(rows)) => {
            tracing::info!(job, rows, "once: done");
        }
        Ok(_) => tracing::info!(job, "once: idle"),
        Err(e) => tracing::error!(job, error = %e, "once: failed"),
    }
    pool.close().await;
    Ok(())
}

async fn run_chaos(cfg: config::Config, action: ChaosAction) -> Result<()> {
    let pool = db::connect(&cfg.db).await?;
    let affected = match action {
        ChaosAction::StuckLeases { count } => {
            tracing::warn!(count, "chaos: injecting stuck leases");
            chaos::inject_stuck_leases(&pool, count).await?
        }
        ChaosAction::StuckOutbox { count } => {
            tracing::warn!(count, "chaos: injecting stuck outbox events");
            chaos::inject_stuck_outbox(&pool, count).await?
        }
        ChaosAction::OrphanRetry { count } => {
            tracing::warn!(count, "chaos: injecting orphan retries");
            chaos::inject_orphan_retry(&pool, count).await?
        }
    };
    tracing::warn!(affected, "chaos: injection complete");
    pool.close().await;
    Ok(())
}
