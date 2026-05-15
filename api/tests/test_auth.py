from uuid import UUID, uuid4

from notifications_api.app.http.auth import (
    build_access_token,
    extract_manager_id_from_token,
    hash_password,
    verify_password,
)

SECRET = "test-signing-key"  # noqa: S105


def test_password_hash_verification_roundtrip() -> None:
    password_hash = hash_password("StrongPassw0rd!")

    assert password_hash.startswith("pbkdf2_sha256$")
    assert verify_password("StrongPassw0rd!", password_hash)
    assert not verify_password("wrong-password", password_hash)


def test_password_hash_invalid_format_is_rejected() -> None:
    assert not verify_password("password", "not-a-valid-hash")


def test_extract_manager_id_from_valid_token() -> None:
    manager_id = uuid4()
    token = build_access_token(manager_id=manager_id, login="manager", secret=SECRET, ttl_seconds=3600)

    extracted = extract_manager_id_from_token(token, secret=SECRET)

    assert extracted == manager_id


def test_extract_manager_id_from_expired_token() -> None:
    manager_id = UUID("11111111-1111-1111-1111-111111111111")
    token = build_access_token(manager_id=manager_id, login="manager", secret=SECRET, ttl_seconds=-10)

    extracted = extract_manager_id_from_token(token, secret=SECRET)

    assert extracted is None


def test_extract_manager_id_from_raw_uuid_token() -> None:
    manager_id = UUID("11111111-1111-1111-1111-111111111111")

    extracted = extract_manager_id_from_token(str(manager_id), secret=SECRET)

    assert extracted == manager_id
