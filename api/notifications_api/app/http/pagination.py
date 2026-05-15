import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import sqlalchemy as sa

from notifications_api.app.http.errors import raise_validation


@dataclass(slots=True, frozen=True)
class Cursor:
    created_at: datetime
    row_id: UUID


def encode_cursor(created_at: datetime, row_id: UUID) -> str:
    payload = {
        "createdAt": created_at.astimezone(UTC).isoformat(),
        "id": str(row_id),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(raw_cursor: str) -> Cursor:
    try:
        decoded = base64.urlsafe_b64decode(raw_cursor.encode("ascii")).decode("utf-8")
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            raise TypeError
        created_at_raw = payload["createdAt"]
        row_id_raw = payload["id"]
        if not isinstance(created_at_raw, str) or not isinstance(row_id_raw, str):
            raise TypeError
        return Cursor(
            created_at=datetime.fromisoformat(created_at_raw),
            row_id=UUID(row_id_raw),
        )
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise_validation("Invalid cursor")
        raise AssertionError("unreachable") from exc


def build_cursor_filter(created_at_column: Any, id_column: Any, cursor: Cursor) -> Any:
    return sa.or_(
        created_at_column < cursor.created_at,
        sa.and_(created_at_column == cursor.created_at, id_column < cursor.row_id),
    )
