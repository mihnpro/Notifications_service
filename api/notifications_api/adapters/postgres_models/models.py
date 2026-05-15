from __future__ import annotations

from datetime import datetime
from typing import final
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from notifications_api.domain import (
    Campaign,
    CampaignPriority,
    CampaignRegionRun,
    CampaignRegionRunStatus,
    CampaignStats,
    CampaignStatus,
    Channel,
    DeliveryError,
    DeliveryRecord,
    RecipientSelector,
)


class Base(DeclarativeBase):
    pass


@final
class ManagerORM(Base):
    __tablename__ = "managers"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    login: Mapped[str] = mapped_column(sa.Text, nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'active'"))
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("status IN ('active', 'blocked')", name="ck_managers_status"),
    )


@final
class UserORM(Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    external_id: Mapped[str | None] = mapped_column(sa.Text)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("status IN ('active', 'inactive', 'blocked')", name="ck_users_status"),
        sa.CheckConstraint("region_id = 'default'", name="ck_users_region_default"),
        sa.UniqueConstraint("region_id", "external_id", name="uq_users_region_external_id"),
        sa.Index("idx_users_region_status", "region_id", "status"),
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

    def to_domain(self) -> Channel:
        return Channel(
            id=self.id,
            code=self.code,
            state=self.state,
        )


@final
class UserChannelORM(Base):
    __tablename__ = "user_channels"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
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
        sa.CheckConstraint("region_id = 'default'", name="ck_user_channels_region_default"),
        sa.UniqueConstraint("user_id", "channel_id", "address", name="uq_user_channels_user_channel_address"),
        sa.Index("idx_user_channels_user_channel_status", "user_id", "channel_id", "status"),
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

    def to_domain(self) -> Campaign:
        return Campaign(
            id=self.id,
            manager_id=self.manager_id,
            name=self.name,
            status=CampaignStatus(self.status),
            message_snapshot=self.message_snapshot,
            recipient_selector=RecipientSelector.from_payload(self.recipient_selector),
            selected_channel_codes=tuple(self.selected_channel_codes),
            priority=CampaignPriority(self.priority),
            created_at=self.created_at,
            completed_at=self.completed_at,
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
            "status IN ('fanout_pending', 'fanout_running', 'fanout_completed', 'fanout_failed', 'cancelling', 'cancelled')",
            name="ck_campaign_region_runs_status",
        ),
        sa.CheckConstraint("region_id = 'default'", name="ck_campaign_region_runs_region_default"),
        sa.UniqueConstraint("campaign_id", "region_id", name="uq_campaign_region_runs_campaign_region"),
        sa.Index("idx_campaign_region_runs_region_status", "region_id", "status"),
    )

    def to_domain(self) -> CampaignRegionRun:
        return CampaignRegionRun(
            id=self.id,
            campaign_id=self.campaign_id,
            region_id=self.region_id,
            status=CampaignRegionRunStatus(self.status),
        )


@final
class DeliveryTaskORM(Base):
    __tablename__ = "delivery_tasks"

    # generated by fan-out as deterministic UUIDv5(campaign_id, user_channel_id, channel_id)
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
    idempotency_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
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
        sa.CheckConstraint("region_id = 'default'", name="ck_delivery_tasks_region_default"),
        sa.UniqueConstraint("region_id", "idempotency_key", name="uq_delivery_tasks_region_idempotency_key"),
        sa.Index(
            "idx_delivery_tasks_region_queue_available",
            "region_id",
            "queue_group",
            "status",
            "available_at",
            "priority",
            postgresql_where=sa.text("status IN ('queued', 'retry_scheduled')"),
        ),
        sa.Index(
            "idx_delivery_tasks_sending_lease_until",
            "region_id",
            "lease_until",
            postgresql_where=sa.text("status = 'sending'"),
        ),
        sa.Index("idx_delivery_tasks_campaign_status", "campaign_id", "status"),
    )

    def to_domain(self) -> DeliveryRecord:
        return DeliveryRecord(
            task_id=self.id,
            region_id=self.region_id,
            user_id=self.user_id,
            channel_code=self.channel_code,
            recipient_address_snapshot=self.recipient_address_snapshot,
            message_snapshot=self.message_snapshot,
            status=self.status,
            attempt_count=self.attempt_count,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            available_at=self.available_at,
            last_error_code=self.last_error_code,
            last_error_message=self.last_error_message,
        )


@final
class DeliveryAttemptORM(Base):
    __tablename__ = "delivery_attempts"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
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
        sa.CheckConstraint("region_id = 'default'", name="ck_delivery_attempts_region_default"),
        sa.UniqueConstraint("task_id", "attempt_no", name="uq_delivery_attempts_task_attempt_no"),
        sa.Index("idx_delivery_attempts_task_status_started", "task_id", "status", "started_at"),
        sa.Index("idx_delivery_attempts_campaign_status_error", "campaign_id", "status", "error_code", "started_at"),
    )

    def to_domain(self) -> DeliveryError:
        return DeliveryError(
            id=self.id,
            task_id=self.task_id,
            channel_code=self.channel_code,
            status=self.status,
            error_type=self.error_type,
            error_code=self.error_code,
            error_message=self.error_message,
            provider_code=self.provider_code,
            provider_request_id=self.provider_request_id,
            started_at=self.started_at,
            completed_at=self.completed_at,
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
    transport_mode: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'rabbitmq_direct'"))
    dedupe_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
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
            "status IN ('pending', 'publishing', 'published', 'failed', 'archived')",
            name="ck_outbox_events_status",
        ),
        sa.CheckConstraint(
            "transport_mode IN ('rabbitmq_direct', 'cdc')",
            name="ck_outbox_events_transport_mode",
        ),
        sa.CheckConstraint("region_id = 'default'", name="ck_outbox_events_region_default"),
        sa.UniqueConstraint("region_id", "dedupe_key", name="uq_outbox_events_region_dedupe_key"),
        sa.Index(
            "idx_outbox_pending_next_attempt",
            "region_id",
            "transport_mode",
            "status",
            "next_attempt_at",
            "created_at",
            postgresql_where=sa.text("status = 'pending'"),
        ),
        sa.Index(
            "idx_outbox_locked_until",
            "region_id",
            "locked_until",
            postgresql_where=sa.text("status = 'publishing'"),
        ),
    )


@final
class CampaignStatsORM(Base):
    __tablename__ = "campaign_stats"

    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), primary_key=True)
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
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

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint("region_id = 'default'", name="ck_campaign_stats_region_default"),
    )

    def to_domain(self) -> CampaignStats:
        return CampaignStats(
            campaign_id=self.campaign_id,
            region_id=self.region_id,
            total_tasks=int(self.total_tasks),
            queued=int(self.queued),
            sending=int(self.sending),
            succeeded=int(self.succeeded),
            failed=int(self.failed),
            retry_scheduled=int(self.retry_scheduled),
            dead_lettered=int(self.dead_lettered),
            cancelled=int(self.cancelled),
            updated_at=self.updated_at,
        )


@final
class DeliveryResultORM(Base):
    __tablename__ = "delivery_results"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        server_default=sa.text("gen_random_uuid()"),
    )
    task_id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("delivery_tasks.id"),
        nullable=False,
    )
    campaign_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("campaigns.id"), nullable=False)
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    user_id: Mapped[UUID] = mapped_column(sa.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False)
    user_channel_id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), sa.ForeignKey("user_channels.id"), nullable=False
    )
    channel_code: Mapped[str] = mapped_column(sa.Text, nullable=False)
    queue_group: Mapped[str] = mapped_column(sa.Text, nullable=False)
    recipient_address_snapshot: Mapped[str] = mapped_column(sa.Text, nullable=False)
    message_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    attempt_count: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    provider_code: Mapped[str | None] = mapped_column(sa.Text)
    provider_request_id: Mapped[str | None] = mapped_column(sa.Text)
    final_error_code: Mapped[str | None] = mapped_column(sa.Text)
    final_error_message: Mapped[str | None] = mapped_column(sa.Text)
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    completed_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False)

    __table_args__: tuple[object, ...] = (
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed', 'dead_lettered', 'cancelled')",
            name="ck_delivery_results_status",
        ),
        sa.CheckConstraint("region_id = 'default'", name="ck_delivery_results_region_default"),
        sa.Index("idx_delivery_results_campaign_status", "campaign_id", "status"),
        sa.Index("idx_delivery_results_completed", "completed_at"),
        sa.Index("idx_delivery_results_task_completed", "task_id", "completed_at"),
    )

    def to_domain(self) -> DeliveryRecord:
        return DeliveryRecord(
            task_id=self.task_id,
            region_id=self.region_id,
            user_id=self.user_id,
            channel_code=self.channel_code,
            recipient_address_snapshot=self.recipient_address_snapshot,
            message_snapshot=self.message_snapshot,
            status=self.status,
            attempt_count=self.attempt_count,
            provider_code=self.provider_code,
            provider_request_id=self.provider_request_id,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
        )


@final
class DlqItemORM(Base):
    __tablename__ = "dlq_items"

    id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
    )
    region_id: Mapped[str] = mapped_column(sa.Text, nullable=False, server_default=sa.text("'default'"))
    task_id: Mapped[UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("delivery_tasks.id"),
        nullable=False,
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
        sa.CheckConstraint("region_id = 'default'", name="ck_dlq_items_region_default"),
        sa.Index(
            "uq_dlq_items_open_task",
            "task_id",
            unique=True,
            postgresql_where=sa.text("status = 'open'"),
        ),
        sa.Index(
            "idx_dlq_open_created",
            "region_id",
            sa.text("created_at DESC"),
            postgresql_where=sa.text("status = 'open'"),
        ),
        sa.Index("idx_dlq_items_task_created", "task_id", "created_at"),
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
        sa.Index("idx_idempotency_expires", "expires_at"),
    )
