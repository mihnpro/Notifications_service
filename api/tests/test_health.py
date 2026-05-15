from collections.abc import AsyncGenerator

from litestar import Litestar
from litestar.di import Provide
from litestar.testing import TestClient
from sqlalchemy.exc import SQLAlchemyError

from notifications_api.app.http.health import health

SUCCESS = 200
SERVICE_ERROR = 503


class HealthySession:
    async def execute(self, _query: object) -> int:
        return 1


class UnhealthySession:
    async def execute(self, _query: object) -> int:
        raise SQLAlchemyError("database unavailable")


async def provide_healthy_session() -> AsyncGenerator[HealthySession, None]:
    yield HealthySession()


async def provide_unhealthy_session() -> AsyncGenerator[UnhealthySession, None]:
    yield UnhealthySession()


def make_test_client(session_provider: object) -> TestClient[Litestar]:
    app = Litestar(route_handlers=[health], dependencies={"session": Provide(session_provider)})
    return TestClient(app=app)


def test_health_ready() -> None:
    with make_test_client(provide_healthy_session) as client:
        response = client.get("/health")

    assert response.status_code == SUCCESS
    assert response.json() == {"status": "ok", "db": "ready"}


def test_health_not_ready() -> None:
    with make_test_client(provide_unhealthy_session) as client:
        response = client.get("/health")

    assert response.status_code == SERVICE_ERROR
    assert response.json() == {"status": "error", "db": "unavailable"}
