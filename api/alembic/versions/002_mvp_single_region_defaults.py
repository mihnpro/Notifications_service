# pyright: reportUnusedCallResult=false
"""mvp single-region defaults and forward-compatible region fields

Revision ID: 002_mvp_single_region_defaults
Revises: 001
Create Date: 2026-05-15
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "002_mvp_single_region_defaults"
down_revision: str | Sequence[str] | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Keep pgcrypto available for gen_random_uuid() across environments.
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # Add region_id to key runtime tables that still miss it.
    op.add_column(
        "user_channels",
        sa.Column("region_id", sa.Text(), server_default=sa.text("'default'"), nullable=False),
    )
    op.add_column(
        "delivery_attempts",
        sa.Column("region_id", sa.Text(), server_default=sa.text("'default'"), nullable=False),
    )
    op.add_column(
        "dlq_items",
        sa.Column("region_id", sa.Text(), server_default=sa.text("'default'"), nullable=False),
    )
    op.add_column(
        "campaign_stats",
        sa.Column("region_id", sa.Text(), server_default=sa.text("'default'"), nullable=False),
    )

    # Backfill region_id from source-of-truth relations where possible.
    op.execute(
        """
        UPDATE user_channels uc
        SET region_id = u.region_id
        FROM users u
        WHERE uc.user_id = u.id
        """
    )
    op.execute(
        """
        UPDATE delivery_attempts da
        SET region_id = dt.region_id
        FROM delivery_tasks dt
        WHERE da.task_id = dt.id
        """
    )
    op.execute(
        """
        UPDATE dlq_items d
        SET region_id = dt.region_id
        FROM delivery_tasks dt
        WHERE d.task_id = dt.id
        """
    )

    # Add immutable delivery log table for replay-safe history.
    op.create_table(
        "delivery_results",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("campaign_id", sa.UUID(), nullable=False),
        sa.Column("region_id", sa.Text(), server_default=sa.text("'default'"), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("user_channel_id", sa.UUID(), nullable=False),
        sa.Column("channel_code", sa.Text(), nullable=False),
        sa.Column("queue_group", sa.Text(), nullable=False),
        sa.Column("recipient_address_snapshot", sa.Text(), nullable=False),
        sa.Column("message_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("provider_code", sa.Text(), nullable=True),
        sa.Column("provider_request_id", sa.Text(), nullable=True),
        sa.Column("final_error_code", sa.Text(), nullable=True),
        sa.Column("final_error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed', 'dead_lettered', 'cancelled')",
            name="ck_delivery_results_status",
        ),
        sa.CheckConstraint("region_id = 'default'", name="ck_delivery_results_region_default"),
        sa.ForeignKeyConstraint(["task_id"], ["delivery_tasks.id"]),
        sa.ForeignKeyConstraint(["campaign_id"], ["campaigns.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["user_channel_id"], ["user_channels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    # Synchronize cancellation statuses with campaign-level lifecycle.
    op.drop_constraint("ck_campaign_region_runs_status", "campaign_region_runs", type_="check")
    op.create_check_constraint(
        "ck_campaign_region_runs_status",
        "campaign_region_runs",
        "status IN ('fanout_pending', 'fanout_running', 'fanout_completed', 'fanout_failed', 'cancelling', 'cancelled')",
    )

    # Enforce single-region runtime profile for MVP with stable contracts.
    op.create_check_constraint("ck_users_region_default", "users", "region_id = 'default'")
    op.create_check_constraint(
        "ck_campaign_region_runs_region_default",
        "campaign_region_runs",
        "region_id = 'default'",
    )
    op.create_check_constraint("ck_delivery_tasks_region_default", "delivery_tasks", "region_id = 'default'")
    op.create_check_constraint("ck_outbox_events_region_default", "outbox_events", "region_id = 'default'")
    op.create_check_constraint("ck_user_channels_region_default", "user_channels", "region_id = 'default'")
    op.create_check_constraint("ck_delivery_attempts_region_default", "delivery_attempts", "region_id = 'default'")
    op.create_check_constraint("ck_dlq_items_region_default", "dlq_items", "region_id = 'default'")
    op.create_check_constraint("ck_campaign_stats_region_default", "campaign_stats", "region_id = 'default'")

    # Keep region_id explicit and always populated.
    for table_name in (
        "users",
        "campaign_region_runs",
        "delivery_tasks",
        "outbox_events",
        "user_channels",
        "delivery_attempts",
        "dlq_items",
        "campaign_stats",
    ):
        op.alter_column(
            table_name,
            "region_id",
            existing_type=sa.Text(),
            nullable=False,
            server_default=sa.text("'default'"),
        )

    # Outbox transport mode + archived status for cleanup path.
    op.add_column(
        "outbox_events",
        sa.Column("transport_mode", sa.Text(), server_default=sa.text("'rabbitmq_direct'"), nullable=False),
    )
    op.drop_constraint("ck_outbox_events_status", "outbox_events", type_="check")
    op.create_check_constraint(
        "ck_outbox_events_status",
        "outbox_events",
        "status IN ('pending', 'publishing', 'published', 'failed', 'archived')",
    )
    op.create_check_constraint(
        "ck_outbox_events_transport_mode",
        "outbox_events",
        "transport_mode IN ('rabbitmq_direct', 'cdc')",
    )

    # Replace global uniqueness with region-scoped uniqueness for forward compatibility.
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_external_id_key")
    op.create_unique_constraint("uq_users_region_external_id", "users", ["region_id", "external_id"])

    op.execute("ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_dedupe_key_key")
    op.create_unique_constraint("uq_outbox_events_region_dedupe_key", "outbox_events", ["region_id", "dedupe_key"])

    op.execute("ALTER TABLE delivery_tasks DROP CONSTRAINT IF EXISTS delivery_tasks_idempotency_key_key")
    op.create_unique_constraint(
        "uq_delivery_tasks_region_idempotency_key",
        "delivery_tasks",
        ["region_id", "idempotency_key"],
    )

    # Allow repeated DLQ after replay; keep one open item per task.
    op.execute("ALTER TABLE dlq_items DROP CONSTRAINT IF EXISTS dlq_items_task_id_key")
    op.create_index(
        "uq_dlq_items_open_task",
        "dlq_items",
        ["task_id"],
        unique=True,
        postgresql_where=sa.text("status = 'open'"),
    )

    # Must-have worker/recovery indexes.
    op.create_index("idx_users_region_status", "users", ["region_id", "status"], unique=False)
    op.create_index(
        "idx_user_channels_user_channel_status",
        "user_channels",
        ["user_id", "channel_id", "status"],
        unique=False,
    )
    op.create_index("idx_campaign_region_runs_region_status", "campaign_region_runs", ["region_id", "status"], unique=False)
    op.create_index(
        "idx_delivery_tasks_region_queue_available",
        "delivery_tasks",
        ["region_id", "queue_group", "status", "available_at", "priority"],
        unique=False,
        postgresql_where=sa.text("status IN ('queued', 'retry_scheduled')"),
    )
    op.create_index(
        "idx_delivery_tasks_sending_lease_until",
        "delivery_tasks",
        ["region_id", "lease_until"],
        unique=False,
        postgresql_where=sa.text("status = 'sending'"),
    )
    op.create_index("idx_delivery_tasks_campaign_status", "delivery_tasks", ["campaign_id", "status"], unique=False)
    op.create_index(
        "idx_delivery_attempts_task_status_started",
        "delivery_attempts",
        ["task_id", "status", "started_at"],
        unique=False,
    )
    op.create_index(
        "idx_delivery_attempts_campaign_status_error",
        "delivery_attempts",
        ["campaign_id", "status", "error_code", "started_at"],
        unique=False,
    )
    op.create_index(
        "idx_outbox_pending_next_attempt",
        "outbox_events",
        ["region_id", "transport_mode", "status", "next_attempt_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "idx_outbox_locked_until",
        "outbox_events",
        ["region_id", "locked_until"],
        unique=False,
        postgresql_where=sa.text("status = 'publishing'"),
    )
    op.create_index(
        "idx_dlq_open_created",
        "dlq_items",
        ["region_id", sa.text("created_at DESC")],
        unique=False,
        postgresql_where=sa.text("status = 'open'"),
    )
    op.create_index(
        "idx_dlq_items_task_created",
        "dlq_items",
        ["task_id", "created_at"],
        unique=False,
    )
    op.create_index("idx_idempotency_expires", "idempotency_keys", ["expires_at"], unique=False)
    op.create_index("idx_delivery_results_campaign_status", "delivery_results", ["campaign_id", "status"], unique=False)
    op.create_index("idx_delivery_results_completed", "delivery_results", ["completed_at"], unique=False)
    op.create_index("idx_delivery_results_task_completed", "delivery_results", ["task_id", "completed_at"], unique=False)



def downgrade() -> None:
    op.drop_index("idx_delivery_results_task_completed", table_name="delivery_results")
    op.drop_index("idx_delivery_results_completed", table_name="delivery_results")
    op.drop_index("idx_delivery_results_campaign_status", table_name="delivery_results")
    op.drop_index("idx_idempotency_expires", table_name="idempotency_keys")
    op.drop_index("idx_dlq_items_task_created", table_name="dlq_items")
    op.drop_index("idx_dlq_open_created", table_name="dlq_items")
    op.drop_index("idx_outbox_locked_until", table_name="outbox_events")
    op.drop_index("idx_outbox_pending_next_attempt", table_name="outbox_events")
    op.drop_index("idx_delivery_attempts_campaign_status_error", table_name="delivery_attempts")
    op.drop_index("idx_delivery_attempts_task_status_started", table_name="delivery_attempts")
    op.drop_index("idx_delivery_tasks_campaign_status", table_name="delivery_tasks")
    op.drop_index("idx_delivery_tasks_sending_lease_until", table_name="delivery_tasks")
    op.drop_index("idx_delivery_tasks_region_queue_available", table_name="delivery_tasks")
    op.drop_index("idx_campaign_region_runs_region_status", table_name="campaign_region_runs")
    op.drop_index("idx_user_channels_user_channel_status", table_name="user_channels")
    op.drop_index("idx_users_region_status", table_name="users")
    op.drop_index("uq_dlq_items_open_task", table_name="dlq_items")

    op.drop_constraint("uq_delivery_tasks_region_idempotency_key", "delivery_tasks", type_="unique")
    op.create_unique_constraint("delivery_tasks_idempotency_key_key", "delivery_tasks", ["idempotency_key"])

    op.drop_constraint("uq_outbox_events_region_dedupe_key", "outbox_events", type_="unique")
    op.create_unique_constraint("outbox_events_dedupe_key_key", "outbox_events", ["dedupe_key"])

    op.drop_constraint("uq_users_region_external_id", "users", type_="unique")
    op.create_unique_constraint("users_external_id_key", "users", ["external_id"])

    op.drop_constraint("ck_outbox_events_transport_mode", "outbox_events", type_="check")
    op.drop_constraint("ck_outbox_events_status", "outbox_events", type_="check")
    op.create_check_constraint(
        "ck_outbox_events_status",
        "outbox_events",
        "status IN ('pending', 'publishing', 'published', 'failed')",
    )
    op.drop_column("outbox_events", "transport_mode")

    op.drop_constraint("ck_campaign_stats_region_default", "campaign_stats", type_="check")
    op.drop_constraint("ck_dlq_items_region_default", "dlq_items", type_="check")
    op.drop_constraint("ck_delivery_attempts_region_default", "delivery_attempts", type_="check")
    op.drop_constraint("ck_user_channels_region_default", "user_channels", type_="check")
    op.drop_constraint("ck_outbox_events_region_default", "outbox_events", type_="check")
    op.drop_constraint("ck_delivery_tasks_region_default", "delivery_tasks", type_="check")
    op.drop_constraint("ck_campaign_region_runs_region_default", "campaign_region_runs", type_="check")
    op.drop_constraint("ck_users_region_default", "users", type_="check")

    op.drop_constraint("ck_campaign_region_runs_status", "campaign_region_runs", type_="check")
    op.create_check_constraint(
        "ck_campaign_region_runs_status",
        "campaign_region_runs",
        "status IN ('fanout_pending', 'fanout_running', 'fanout_completed', 'fanout_failed')",
    )

    op.drop_table("delivery_results")
    op.create_unique_constraint("dlq_items_task_id_key", "dlq_items", ["task_id"])

    op.drop_column("campaign_stats", "region_id")
    op.drop_column("dlq_items", "region_id")
    op.drop_column("delivery_attempts", "region_id")
    op.drop_column("user_channels", "region_id")
