"""Bulk-seed users directly via COPY — bypasses the 1k/req limit of /users/bulk so
50k users land in under a second. Mirrors the schema written by the alembic migration."""
from __future__ import annotations

import asyncio
import io
import uuid

import asyncpg


def _csv_escape(val: str) -> str:
    """Minimal CSV escaping — none of our seeded values contain ',' '"' or newline."""
    return val


CHANNELS = (("email", "Email", "email"), ("sms", "SMS", "sms"))


async def ensure_channels(conn: asyncpg.Connection) -> dict[str, uuid.UUID]:
    """Idempotently upsert channels and return {code: channel_id}."""
    for code, display, qg in CHANNELS:
        await conn.execute(
            """
            INSERT INTO channels (code, display_name, state, queue_group)
            VALUES ($1, $2, 'enabled', $3)
            ON CONFLICT (code) DO UPDATE
              SET state='enabled', queue_group=EXCLUDED.queue_group
            """,
            code, display, qg,
        )
    rows = await conn.fetch("SELECT code, id FROM channels WHERE code = ANY($1)",
                            [c[0] for c in CHANNELS])
    return {r["code"]: r["id"] for r in rows}


async def purge_users(conn: asyncpg.Connection, prefix: str) -> None:
    """Delete any prior seed users + their channels so each run is isolated."""
    await conn.execute(
        """
        DELETE FROM user_channels
          WHERE user_id IN (SELECT id FROM users WHERE external_id LIKE $1)
        """,
        f"{prefix}%",
    )
    await conn.execute("DELETE FROM users WHERE external_id LIKE $1", f"{prefix}%")


async def seed_users(dsn: str, total: int, external_id_prefix: str) -> list[uuid.UUID]:
    """Insert `total` active users with email+sms channels. Returns their IDs."""
    pool_dsn = dsn.replace("postgres://", "postgresql://")
    conn = await asyncpg.connect(pool_dsn)
    try:
        channel_ids = await ensure_channels(conn)
        await purge_users(conn, external_id_prefix)

        ids = [uuid.uuid4() for _ in range(total)]

        users_buf = io.BytesIO()
        for i, uid in enumerate(ids):
            ext = f"{external_id_prefix}{i:08d}"
            users_buf.write(f"{uid},default,{ext},active\n".encode())
        users_buf.seek(0)
        await conn.copy_to_table(
            "users",
            source=users_buf,
            columns=("id", "region_id", "external_id", "status"),
            format="csv",
        )

        uc_buf = io.BytesIO()
        for uid in ids:
            for code, _, _ in CHANNELS:
                addr = f"{uid}@example.test" if code == "email" else "+10000000000"
                ch_id = channel_ids[code]
                uc_buf.write(
                    f"{uuid.uuid4()},{uid},{ch_id},{addr},active,true\n".encode()
                )
        uc_buf.seek(0)
        await conn.copy_to_table(
            "user_channels",
            source=uc_buf,
            columns=("id", "user_id", "channel_id", "address", "status", "verified"),
            format="csv",
        )
        return ids
    finally:
        await conn.close()
