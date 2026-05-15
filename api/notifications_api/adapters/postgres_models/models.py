from datetime import datetime
from typing import final
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


@final
class UserORM(Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    external_id: Mapped[str | None] = mapped_column(sa.Text, unique=True)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("status IN ('active', 'inactive', 'blocked')", name="ck_users_status"),
    )


@final
class ChannelORM(Base):
    __tablename__ = "channels"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    code: Mapped[str] = mapped_column(sa.Text, nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    state: Mapped[str] = mapped_column(sa.Text, nullable=False)
    adapter_name: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'stub'"))
    provider_code: Mapped[str | None] = mapped_column(sa.Text)
    queue_group: Mapped[str] = mapped_column(sa.Text, nullable=False)
    rate_limits: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=sa.text("jsonb_build_object('rps', 10, 'maxConcurrency', 10)"),
    )
    retry_policy: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=sa.text("jsonb_build_object('maxAttempts', 5, 'baseDelaySeconds', 30, 'maxDelaySeconds', 1800)"),
    )
    disable_policy: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'retry_later'"))
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("state IN ('enabled', 'disabled', 'degraded')", name="ck_channels_state"),
        sa.CheckConstraint("queue_group IN ('email', 'sms', 'push', 'messenger')", name="ck_channels_queue_group"),
        sa.CheckConstraint("disable_policy IN ('retry_later', 'fail_fast')", name="ck_channels_disable_policy"),
    )


@final
class UserChannelORM(Base):
    __tablename__ = "user_channels"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False)
    channel_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("channels.id"), nullable=False)
    address: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    verified: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, server_default=sa.text("true"))
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("status IN ('active', 'inactive')", name="ck_user_channels_status"),
        sa.UniqueConstraint("user_id", "channel_id", "address", name="uq_user_channels_user_channel_address"),
    )


@final
class CampaignORM(Base):
    __tablename__ = "campaigns"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    manager_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    message_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    recipient_selector: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    selected_channel_codes: Mapped[list[str]] = mapped_column(ARRAY(sa.Text()), nullable=False)
    priority: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'normal'"))
    create_idempotency_key: Mapped[str | None] = mapped_column(sa.Text)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint(
            "status IN ('running', 'completed', 'partially_failed', 'failed', 'cancelling', 'cancelled')",
            name="ck_campaigns_status",
        ),
        sa.CheckConstraint("priority IN ('low', 'normal', 'high')", name="ck_campaigns_priority"),
    )


@final
class CampaignRegionRunORM(Base):
    __tablename__ = "campaign_region_runs"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), nullable=False)
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    fanout_lock_owner: Mapped[str | None] = mapped_column(sa.Text)
    fanout_lock_until: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    fanout_attempt_count: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("0"))
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint(
            "status IN ('fanout_pending', 'fanout_running', 'fanout_completed', 'fanout_failed')",
            name="ck_campaign_region_runs_status",
        ),
        sa.UniqueConstraint("campaign_id", "region_id", name="uq_campaign_region_runs_campaign_region"),
    )


@final
class DeliveryTaskORM(Base):
    __tablename__ = "delivery_tasks"

    id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), primary_key=True)
    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), nullable=False)
    campaign_region_run_id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("campaign_region_runs.id"),
        nullable=False,
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    user_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False)
    user_channel_id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey("user_channels.id"), nullable=False
    )
    channel_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("channels.id"), nullable=False)
    channel_code: Mapped[str] = mapped_column(sa.Text, nullable=False)
    queue_group: Mapped[str] = mapped_column(sa.Text, nullable=False)
    recipient_address_snapshot: Mapped[str] = mapped_column(sa.Text, nullable=False)
    message_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(sa.Text, nullable=False, unique=True)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    priority: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'normal'"))
    attempt_count: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("0"))
    max_attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("5"))
    available_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    lease_owner: Mapped[str | None] = mapped_column(sa.Text)
    lease_token: Mapped[UUID | None] = mapped_column(sa.UUID(as_uuid=True))
    lease_until: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    provider_code: Mapped[str | None] = mapped_column(sa.Text)
    provider_request_id: Mapped[str | None] = mapped_column(sa.Text)
    last_error_code: Mapped[str | None] = mapped_column(sa.Text)
    last_error_message: Mapped[str | None] = mapped_column(sa.Text)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    started_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint(
            "status IN ('queued', 'sending', 'succeeded', 'failed', 'retry_scheduled', 'dead_lettered', 'cancelled')",
            name="ck_delivery_tasks_status",
        ),
    )


@final
class DeliveryAttemptORM(Base):
    __tablename__ = "delivery_attempts"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    task_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("delivery_tasks.id"), nullable=False)
    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), nullable=False)
    attempt_no: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(sa.Text)
    channel_code: Mapped[str] = mapped_column(sa.Text, nullable=False)
    provider_code: Mapped[str | None] = mapped_column(sa.Text)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    error_type: Mapped[str | None] = mapped_column(sa.Text)
    error_code: Mapped[str | None] = mapped_column(sa.Text)
    error_message: Mapped[str | None] = mapped_column(sa.Text)
    provider_request_id: Mapped[str | None] = mapped_column(sa.Text)
    started_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint(
            "status IN ('started', 'succeeded', 'failed', 'timed_out', 'stale')",
            name="ck_delivery_attempts_status",
        ),
        sa.CheckConstraint(
            "error_type IS NULL OR error_type IN ('transient', 'permanent', 'unknown')",
            name="ck_delivery_attempts_error_type",
        ),
        sa.UniqueConstraint("task_id", "attempt_no", name="uq_delivery_attempts_task_attempt_no"),
    )


@final
class OutboxEventORM(Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    event_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    exchange: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'notification.direct'"))
    routing_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'pending'"))
    dedupe_key: Mapped[str] = mapped_column(sa.Text, nullable=False, unique=True)
    locked_by: Mapped[str | None] = mapped_column(sa.Text)
    locked_until: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(sa.Integer, nullable=False, server_default=sa.text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    )
    last_error: Mapped[str | None] = mapped_column(sa.Text)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint(
            "status IN ('pending', 'publishing', 'published', 'failed')",
            name="ck_outbox_events_status",
        ),
    )


@final
class CampaignStatsORM(Base):
    __tablename__ = "campaign_stats"

    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), primary_key=True)
    total_tasks: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    queued: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    sending: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    succeeded: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    failed: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    retry_scheduled: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    dead_lettered: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    cancelled: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, server_default=sa.text("0"))
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )


@final
class DlqItemORM(Base):
    __tablename__ = "dlq_items"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    task_id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("delivery_tasks.id"),
        nullable=False,
        unique=True,
    )
    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), nullable=False)
    channel_code: Mapped[str] = mapped_column(sa.Text, nullable=False)
    reason_code: Mapped[str] = mapped_column(sa.Text, nullable=False)
    error_code: Mapped[str | None] = mapped_column(sa.Text)
    error_message: Mapped[str | None] = mapped_column(sa.Text)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'open'"))
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    last_replayed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("status IN ('open', 'replayed', 'ignored')", name="ck_dlq_items_status"),
    )


@final
class IdempotencyKeyORM(Base):
    __tablename__ = "idempotency_keys"

    scope: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    key: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    response_payload: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    resource_id: Mapped[UUID | None] = mapped_column(sa.UUID(as_uuid=True))
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    expires_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False)

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("status IN ('processing', 'completed', 'failed')", name="ck_idempotency_keys_status"),
    )
