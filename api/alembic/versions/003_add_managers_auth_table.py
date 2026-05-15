# pyright: reportUnusedCallResult=false
"""add managers auth table

Revision ID: 003_add_managers_auth_table
Revises: 002_mvp_single_region_defaults
Create Date: 2026-05-16
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "003_add_managers_auth_table"
down_revision: str | Sequence[str] | None = "002_mvp_single_region_defaults"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "managers",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("login", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'active'"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("status IN ('active', 'blocked')", name="ck_managers_status"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("login"),
    )


def downgrade() -> None:
    op.drop_table("managers")
