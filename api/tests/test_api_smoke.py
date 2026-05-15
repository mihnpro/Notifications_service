from uuid import uuid4

from litestar.testing import TestClient

from notifications_api.app.litestar import app


AUTH_HEADERS = {"Authorization": f"Bearer {uuid4()}"}


def _assert_error_payload(response: object, expected_code: str) -> None:
    payload = response.json()  # type: ignore[attr-defined]
    assert "error" in payload
    assert payload["error"]["code"] == expected_code


def test_smoke_protected_routes_require_authorization() -> None:
    with TestClient(app=app) as client:
        response_campaigns = client.get("/campaigns")
        response_channels = client.get("/channels")
        response_dlq = client.get("/dlq")

    assert response_campaigns.status_code == 401
    _assert_error_payload(response_campaigns, "UNAUTHORIZED")
    assert response_channels.status_code == 401
    _assert_error_payload(response_channels, "UNAUTHORIZED")
    assert response_dlq.status_code == 401
    _assert_error_payload(response_dlq, "UNAUTHORIZED")


def test_smoke_mutating_routes_require_idempotency_key() -> None:
    campaign_body = {
        "name": "Smoke Campaign",
        "regionIds": ["default"],
        "message": {"subject": "Smoke", "body": "Smoke body"},
        "recipientSelector": {"type": "all"},
        "channels": ["email"],
        "priority": "normal",
    }
    channel_body = {
        "code": "whatsapp-smoke",
        "displayName": "WhatsApp Smoke",
        "globalState": "enabled",
        "adapterName": "stub",
        "queueGroup": "messenger",
    }
    dlq_replay_body = {
        "regionId": "default",
        "filter": {},
        "limit": 10,
        "additionalAttempts": 0,
    }
    users_bulk_body = {
        "mode": "skip_duplicates",
        "items": [
            {
                "externalId": "smoke-user-1",
                "status": "active",
                "channels": [{"channel": "email", "address": "smoke@example.com"}],
            }
        ],
    }

    with TestClient(app=app) as client:
        response_campaign = client.post("/campaigns", headers=AUTH_HEADERS, json=campaign_body)
        response_channel = client.post("/channels", headers=AUTH_HEADERS, json=channel_body)
        response_dlq_replay = client.post("/dlq/replay", headers=AUTH_HEADERS, json=dlq_replay_body)
        response_users_bulk = client.post("/users/bulk", headers=AUTH_HEADERS, json=users_bulk_body)

    for response in (response_campaign, response_channel, response_dlq_replay, response_users_bulk):
        assert response.status_code == 400
        _assert_error_payload(response, "VALIDATION_ERROR")
        assert "Idempotency-Key" in response.json()["error"]["message"]


def test_smoke_invalid_cursor_is_rejected() -> None:
    with TestClient(app=app) as client:
        response = client.get(
            "/campaigns",
            headers=AUTH_HEADERS,
            params={"cursor": "not-a-valid-cursor"},
        )

    assert response.status_code == 400
    _assert_error_payload(response, "VALIDATION_ERROR")
    assert response.json()["error"]["message"] == "Invalid cursor"


def test_smoke_campaign_region_guard() -> None:
    body = {
        "name": "Invalid Region Campaign",
        "regionIds": ["eu-central-1"],
        "message": {"subject": "Smoke", "body": "Smoke body"},
        "recipientSelector": {"type": "all"},
        "channels": ["email"],
        "priority": "normal",
    }
    headers = {**AUTH_HEADERS, "Idempotency-Key": "smoke-invalid-region-1"}

    with TestClient(app=app) as client:
        response = client.post("/campaigns", headers=headers, json=body)

    assert response.status_code == 400
    _assert_error_payload(response, "VALIDATION_ERROR")
    assert "regionIds=['default']" in response.json()["error"]["message"]
