from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID

import sqlalchemy as sa
from litestar import Request, post
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: TC002

from notifications_api.adapters.postgres_models.models import ManagerORM
from notifications_api.app.http.errors import raise_unauthorized, raise_validation
from notifications_api.app.http.schemas import ApiModel
from notifications_api.infra.config import GlobalConfig

if TYPE_CHECKING:
    from litestar.datastructures import State

PBKDF2_SCHEME = "pbkdf2_sha256"
PBKDF2_HASH_NAME = "sha256"
PBKDF2_DERIVED_KEY_LENGTH = 32
PBKDF2_DEFAULT_ITERATIONS = 390_000
PBKDF2_MIN_ITERATIONS = 100_000
JWT_PARTS_COUNT = 3


@dataclass(slots=True, frozen=True)
class ManagerIdentity:
    manager_id: UUID
    raw_token: str


class LoginRequest(ApiModel):
    login: str
    password: str


class LoginResponse(ApiModel):
    access_token: str
    token_type: str = "Bearer"  # noqa: S105
    expires_in: int


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def _b64_encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _b64_decode(raw: str) -> bytes:
    return base64.b64decode(raw, validate=True)


def hash_password(password: str, *, iterations: int = PBKDF2_DEFAULT_ITERATIONS) -> str:
    if not password:
        raise ValueError("Password must not be empty")
    if iterations < PBKDF2_MIN_ITERATIONS:
        raise ValueError("PBKDF2 iterations must be >= 100000")

    salt = os.urandom(16)
    derived = hashlib.pbkdf2_hmac(
        PBKDF2_HASH_NAME,
        password.encode("utf-8"),
        salt,
        iterations,
        dklen=PBKDF2_DERIVED_KEY_LENGTH,
    )
    return f"{PBKDF2_SCHEME}${iterations}${_b64_encode(salt)}${_b64_encode(derived)}"


def verify_password(password: str, password_hash: str) -> bool:
    if not password or not password_hash:
        return False

    try:
        scheme, raw_iterations, encoded_salt, encoded_derived = password_hash.split("$", maxsplit=3)
        if scheme != PBKDF2_SCHEME:
            return False
        iterations = int(raw_iterations)
        if iterations < PBKDF2_MIN_ITERATIONS:
            return False

        salt = _b64_decode(encoded_salt)
        expected_derived = _b64_decode(encoded_derived)
        actual_derived = hashlib.pbkdf2_hmac(
            PBKDF2_HASH_NAME,
            password.encode("utf-8"),
            salt,
            iterations,
            dklen=len(expected_derived),
        )
    except (ValueError, TypeError, binascii.Error):
        return False

    return hmac.compare_digest(actual_derived, expected_derived)


def build_access_token(*, manager_id: UUID, login: str, secret: str, ttl_seconds: int) -> str:
    now_ts = int(datetime.now(tz=UTC).timestamp())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": str(manager_id),
        "login": login,
        "iat": now_ts,
        "exp": now_ts + ttl_seconds,
    }
    encoded_header = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    encoded_payload = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{encoded_header}.{encoded_payload}.{_b64url_encode(signature)}"


def _decode_and_verify_access_token(token: str, *, secret: str) -> dict[str, Any] | None:  # noqa: PLR0911
    parts = token.split(".")
    if len(parts) != JWT_PARTS_COUNT:
        return None

    encoded_header, encoded_payload, encoded_signature = parts
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    expected_signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()

    try:
        actual_signature = _b64url_decode(encoded_signature)
        if not hmac.compare_digest(actual_signature, expected_signature):
            return None

        raw_header = _b64url_decode(encoded_header)
        raw_payload = _b64url_decode(encoded_payload)
        header = json.loads(raw_header)
        payload = json.loads(raw_payload)
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error):
        return None

    if not isinstance(header, dict) or not isinstance(payload, dict):
        return None

    if header.get("alg") != "HS256" or header.get("typ") != "JWT":
        return None

    expires_at = payload.get("exp")
    if not isinstance(expires_at, int):
        return None

    if int(datetime.now(tz=UTC).timestamp()) > expires_at:
        return None

    return payload


def extract_manager_id_from_token(token: str, *, secret: str) -> UUID | None:
    normalized_token = token.strip()
    try:
        # Backward-compatible mode for local smoke scripts that pass raw manager UUID.
        return UUID(normalized_token)
    except ValueError:
        ...

    payload = _decode_and_verify_access_token(normalized_token, secret=secret)
    if payload is None:
        return None

    raw_subject = payload.get("sub")
    if not isinstance(raw_subject, str):
        return None

    try:
        return UUID(raw_subject)
    except ValueError:
        return None


@post("/auth/login")
async def login(
    data: LoginRequest,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    normalized_login = data.login.strip().lower()
    if not normalized_login:
        raise_validation("login must not be empty")
    if not data.password:
        raise_validation("password must not be empty")

    manager = (
        await session.execute(
            sa.select(ManagerORM).where(
                ManagerORM.login == normalized_login,
                ManagerORM.status == "active",
            )
        )
    ).scalar_one_or_none()

    if manager is None or not verify_password(data.password, manager.password_hash):
        raise_unauthorized("Invalid login or password")

    token = build_access_token(
        manager_id=manager.id,
        login=manager.login,
        secret=config.auth_jwt_secret,
        ttl_seconds=config.auth_jwt_ttl_seconds,
    )
    payload = LoginResponse(
        access_token=token,
        expires_in=config.auth_jwt_ttl_seconds,
    ).model_dump(by_alias=True)
    return Response(content=payload, status_code=HTTP_200_OK)


async def provide_manager(
    request: Request[Any, Any, State],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> ManagerIdentity:
    auth_header = request.headers.get("Authorization")
    if auth_header is None:
        raise_unauthorized("Missing Authorization header")
    if not auth_header.strip():
        raise_unauthorized("Invalid Authorization header format")

    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise_unauthorized("Invalid Authorization header format")

    manager_id = extract_manager_id_from_token(token, secret=config.auth_jwt_secret)
    if manager_id is None:
        raise_unauthorized("Invalid or expired access token")

    return ManagerIdentity(manager_id=manager_id, raw_token=token.strip())
