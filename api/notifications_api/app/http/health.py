from typing import Annotated

from litestar import get
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK, HTTP_503_SERVICE_UNAVAILABLE
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession


async def _probe_db(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
) -> Response[dict[str, str]]:
    try:
        _ = await session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return Response(
            content={"status": "error", "db": "unavailable"},
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
        )

    return Response(content={"status": "ok", "db": "ready"}, status_code=HTTP_200_OK)


@get("/health")
async def health(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
) -> Response[dict[str, str]]:
    return await _probe_db(session)


@get("/healthz")
async def healthz(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
) -> Response[dict[str, str]]:
    return await _probe_db(session)


@get("/readyz")
async def readyz(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
) -> Response[dict[str, str]]:
    return await _probe_db(session)
