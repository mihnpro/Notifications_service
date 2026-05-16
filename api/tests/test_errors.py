import logging
from types import SimpleNamespace

from _pytest.logging import LogCaptureFixture

from notifications_api.app.http.errors import generic_exception_handler


def _fake_request(method: str = "POST", path: str = "/channels") -> SimpleNamespace:
    return SimpleNamespace(method=method, url=SimpleNamespace(path=path))


def test_generic_exception_handler_returns_internal_error_envelope() -> None:
    response = generic_exception_handler(_fake_request(), RuntimeError("boom"))  # type: ignore[arg-type]

    assert response.status_code == 500
    assert response.content == {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "Internal server error",
            "details": {},
        }
    }


def test_generic_exception_handler_logs_exception_with_request_context(caplog: LogCaptureFixture) -> None:
    request = _fake_request(method="PATCH", path="/channels/123")
    exc = ValueError("broken")

    with caplog.at_level(logging.ERROR, logger="notifications_api.app.http.errors"):
        generic_exception_handler(request, exc)  # type: ignore[arg-type]

    records = [record for record in caplog.records if record.name == "notifications_api.app.http.errors"]
    assert records
    message = records[-1].getMessage()
    assert "Unhandled exception while processing request" in message
    assert "method=PATCH" in message
    assert "path=/channels/123" in message
    assert records[-1].exc_info is not None
