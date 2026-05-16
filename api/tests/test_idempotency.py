from types import SimpleNamespace
from uuid import UUID

from notifications_api.app.http.auth import ManagerIdentity
from notifications_api.app.http.idempotency import build_scope


def _fake_request(*, method: str, path: str, route_paths: set[str]) -> SimpleNamespace:
    return SimpleNamespace(
        method=method,
        url=SimpleNamespace(path=path),
        route_handler=SimpleNamespace(paths=route_paths),
    )


def _manager() -> ManagerIdentity:
    return ManagerIdentity(
        manager_id=UUID("11111111-1111-1111-1111-111111111111"),
        raw_token="token",
    )


def test_build_scope_supports_set_route_paths() -> None:
    request = _fake_request(method="post", path="/channels", route_paths={"/channels"})

    scope = build_scope(request, _manager())  # type: ignore[arg-type]

    assert scope == "11111111-1111-1111-1111-111111111111:POST:/channels"


def test_build_scope_falls_back_to_request_path_when_route_paths_empty() -> None:
    request = _fake_request(method="patch", path="/channels/123", route_paths=set())

    scope = build_scope(request, _manager())  # type: ignore[arg-type]

    assert scope == "11111111-1111-1111-1111-111111111111:PATCH:/channels/123"
