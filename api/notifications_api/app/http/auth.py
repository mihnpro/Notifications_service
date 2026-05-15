from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from uuid import UUID

from litestar import Request

from notifications_api.app.http.errors import raise_unauthorized


@dataclass(slots=True, frozen=True)
class ManagerIdentity:
    manager_id: UUID
    raw_token: str


def _decode_jwt_payload(token: str) -> dict[str, object] | None:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding).decode("utf-8")
        parsed = json.loads(decoded)
    except (ValueError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def extract_manager_id_from_token(token: str) -> UUID | None:
    stripped = token.strip()
    try:
        return UUID(stripped)
    except ValueError:
        pass

    if ":" in stripped:
        _prefix, value = stripped.split(":", 1)
        try:
            return UUID(value)
        except ValueError:
            pass

    payload = _decode_jwt_payload(stripped)
    if payload is None:
        return None

    for key in ("manager_id", "managerId", "sub"):
        raw = payload.get(key)
        if isinstance(raw, str):
            try:
                return UUID(raw)
            except ValueError:
                continue
    return None


async def provide_manager(request: Request[object, object, object]) -> ManagerIdentity:
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise_unauthorized("Missing Authorization header")

    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise_unauthorized("Invalid Authorization header format")

    manager_id = extract_manager_id_from_token(token)
    if manager_id is None:
        raise_unauthorized("Unable to extract manager_id from Bearer token")

    return ManagerIdentity(manager_id=manager_id, raw_token=token.strip())
