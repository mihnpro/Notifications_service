from datetime import UTC, datetime
from typing import final
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from notifications_api.domain import OutboxTaskStatus, OutboxTaskType


class Base(DeclarativeBase):
    pass


@final
class OutboxTaskORM(Base):
    __tablename__ = "outbox_tasks"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    available_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False)
    task_type: Mapped[OutboxTaskType] = mapped_column(
        sa.Enum(OutboxTaskType, native_enum=False, length=50), nullable=False
    )
    status: Mapped[OutboxTaskStatus] = mapped_column(
        sa.Enum(OutboxTaskStatus, native_enum=False, length=50), nullable=False
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0, server_default=sa.text("0"))

    __table_args__: tuple[sa.Index, ...] = (
        sa.Index("ix_outbox_tasks_available_at", "available_at"),
        sa.Index("ix_outbox_tasks_status", "status"),
        sa.Index("ix_outbox_tasks_status_available_at", "status", "available_at"),
    )
