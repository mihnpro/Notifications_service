import logging
from dataclasses import dataclass, field
from typing import Any, NoReturn

from litestar import Request
from litestar.datastructures import State
from litestar.exceptions import HTTPException, NotFoundException, ValidationException
from litestar.response import Response
from litestar.status_codes import (
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_500_INTERNAL_SERVER_ERROR,
)
from pydantic import Field
from sqlalchemy.exc import IntegrityError

from notifications_api.app.http.schemas import ApiModel

logger = logging.getLogger(__name__)


class ErrorPayload(ApiModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorEnvelope(ApiModel):
    error: ErrorPayload


@dataclass(slots=True)
class ApiError(Exception):
    status_code: int
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def build_error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> Response[dict[str, Any]]:
    payload = ErrorEnvelope(
        error=ErrorPayload(code=code, message=message, details=details or {}),
    ).model_dump(by_alias=True)
    return Response(content=payload, status_code=status_code)


def api_error_handler(_request: Request[Any, Any, State], exc: ApiError) -> Response[dict[str, Any]]:
    return build_error_response(
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        details=exc.details,
    )


def validation_exception_handler(
    _request: Request[Any, Any, State],
    exc: ValidationException,
) -> Response[dict[str, Any]]:
    details: dict[str, Any] = {"errors": exc.extra} if exc.extra else {}
    return build_error_response(
        status_code=HTTP_400_BAD_REQUEST,
        code="VALIDATION_ERROR",
        message="Request validation failed",
        details=details,
    )


def not_found_exception_handler(
    _request: Request[Any, Any, State],
    exc: NotFoundException,
) -> Response[dict[str, Any]]:
    return build_error_response(
        status_code=HTTP_404_NOT_FOUND,
        code="NOT_FOUND",
        message=str(exc.detail) if exc.detail else "Resource not found",
    )


def http_exception_handler(
    _request: Request[Any, Any, State],
    exc: HTTPException,
) -> Response[dict[str, Any]]:
    if exc.status_code == HTTP_401_UNAUTHORIZED:
        code = "UNAUTHORIZED"
    elif exc.status_code == HTTP_409_CONFLICT:
        code = "CONFLICT"
    elif exc.status_code == HTTP_404_NOT_FOUND:
        code = "NOT_FOUND"
    else:
        code = "VALIDATION_ERROR"
    return build_error_response(
        status_code=exc.status_code,
        code=code,
        message=str(exc.detail) if exc.detail else "HTTP error",
        details=exc.extra if isinstance(exc.extra, dict) else {},
    )


def generic_exception_handler(
    request: Request[Any, Any, State],
    exc: Exception,
) -> Response[dict[str, Any]]:
    logger.exception(
        "Unhandled exception while processing request: method=%s path=%s",
        request.method,
        request.url.path,
        exc_info=exc,
    )
    return build_error_response(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        code="INTERNAL_ERROR",
        message="Internal server error",
    )


def integrity_error_handler(
    _request: Request[Any, Any, State],
    _exc: IntegrityError,
) -> Response[dict[str, Any]]:
    return build_error_response(
        status_code=HTTP_409_CONFLICT,
        code="CONFLICT",
        message="Database conflict",
    )


def raise_validation(message: str, details: dict[str, Any] | None = None) -> NoReturn:
    raise ApiError(status_code=HTTP_400_BAD_REQUEST, code="VALIDATION_ERROR", message=message, details=details or {})


def raise_unauthorized(message: str = "Unauthorized") -> NoReturn:
    raise ApiError(status_code=HTTP_401_UNAUTHORIZED, code="UNAUTHORIZED", message=message)


def raise_not_found(message: str) -> NoReturn:
    raise ApiError(status_code=HTTP_404_NOT_FOUND, code="NOT_FOUND", message=message)


def raise_conflict(message: str, details: dict[str, Any] | None = None) -> NoReturn:
    raise ApiError(status_code=HTTP_409_CONFLICT, code="CONFLICT", message=message, details=details or {})
