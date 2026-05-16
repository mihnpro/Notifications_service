# ruff: noqa: INP001
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import psycopg2

from notifications_api.app.http.auth import hash_password
from notifications_api.infra.config import GlobalConfig

DEFAULT_API_URL = "http://localhost:8000"
DEFAULT_TOTAL_USERS = 50_000
DEFAULT_BATCH_SIZE = 1_000
DEFAULT_CHANNELS = ("email", "sms", "push", "telegram")
DEFAULT_IDEMPOTENCY_PREFIX = "seed-users"
ALLOWED_CHANNELS = frozenset(DEFAULT_CHANNELS)
HTTP_SERVER_ERROR_MIN = 500
DEFAULT_MANAGER_LOGIN = "admin"
DEFAULT_MANAGER_PASSWORD = "change-me-now"  # noqa: S105
DEFAULT_WORKERS = 1
MAX_WORKERS = 32
CHANNEL_DEFINITIONS: dict[str, tuple[str, str]] = {
    "email": ("Email", "email"),
    "sms": ("SMS", "sms"),
    "push": ("Push", "push"),
    "telegram": ("Telegram", "messenger"),
}


@dataclass(slots=True)
class BatchStats:
    inserted_users: int = 0
    inserted_user_channels: int = 0
    updated_users: int = 0
    updated_user_channels: int = 0
    skipped_users: int = 0
    skipped_user_channels: int = 0
    errors: int = 0

    def add_payload(self, payload: dict[str, object]) -> None:
        inserted = _as_dict(payload.get("inserted"))
        updated = _as_dict(payload.get("updated"))
        skipped = _as_dict(payload.get("skipped"))
        errors = payload.get("errors")
        self.inserted_users += _as_int(inserted.get("users"))
        self.inserted_user_channels += _as_int(inserted.get("userChannels"))
        self.updated_users += _as_int(updated.get("users"))
        self.updated_user_channels += _as_int(updated.get("userChannels"))
        self.skipped_users += _as_int(skipped.get("users"))
        self.skipped_user_channels += _as_int(skipped.get("userChannels"))
        if isinstance(errors, list):
            self.errors += len(errors)


@dataclass(slots=True)
class BatchTask:
    batch_no: int
    start_index: int
    batch_count: int


@dataclass(slots=True)
class BatchResult:
    batch_no: int
    batch_count: int
    payload: dict[str, object]


@dataclass(slots=True)
class SeedExecutionState:
    channels: list[str]
    token: str
    users_bulk_url: str
    batch_tasks: list[BatchTask]
    totals: BatchStats
    start_ts: float
    idempotency_key_prefix: str


def _as_int(value: object) -> int:
    if isinstance(value, int):
        return value
    return 0


def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    return {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed synthetic users through POST /users/bulk")
    parser.add_argument("--total", type=int, default=DEFAULT_TOTAL_USERS, help="Total users to generate")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Users per request (must be <= 1000 by default server config)",
    )
    parser.add_argument(
        "--channels",
        type=str,
        default=",".join(DEFAULT_CHANNELS),
        help="Comma-separated channels to generate: email,sms,push,telegram",
    )
    parser.add_argument("--api-url", type=str, default=DEFAULT_API_URL, help="Base API URL, e.g. http://localhost:8000")
    parser.add_argument(
        "--auth-token",
        type=str,
        default=os.getenv("AUTH_TOKEN", ""),
        help="JWT token override; if empty, script bootstraps manager and logs in",
    )
    parser.add_argument(
        "--idempotency-prefix",
        type=str,
        default=DEFAULT_IDEMPOTENCY_PREFIX,
        help="Prefix for Idempotency-Key header",
    )
    parser.add_argument(
        "--idempotency-run-id",
        type=str,
        default="",
        help="Optional run id suffix for idempotency keys; auto-generated when omitted",
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP timeout per request")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per batch on transport/5xx errors")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Parallel /users/bulk requests (1 for sequential mode)",
    )
    parser.add_argument(
        "--manager-login",
        type=str,
        default=os.getenv("SEED_MANAGER_LOGIN", DEFAULT_MANAGER_LOGIN),
        help="Manager login for auto-auth bootstrap/login",
    )
    parser.add_argument(
        "--manager-password",
        type=str,
        default=os.getenv("SEED_MANAGER_PASSWORD", DEFAULT_MANAGER_PASSWORD),
        help="Manager password for auto-auth bootstrap/login",
    )
    parser.add_argument(
        "--no-bootstrap-manager",
        action="store_true",
        help="Do not auto-create/update manager in DB before /auth/login",
    )
    parser.add_argument(
        "--no-bootstrap-channels",
        action="store_true",
        help="Do not auto-create/update channels in DB before /users/bulk",
    )
    return parser.parse_args()


def parse_channels(channels_csv: str) -> list[str]:
    channels = [item.strip() for item in channels_csv.split(",") if item.strip()]
    unique_channels = list(dict.fromkeys(channels))
    if not unique_channels:
        raise ValueError("channels list is empty")
    unknown = [channel for channel in unique_channels if channel not in ALLOWED_CHANNELS]
    if unknown:
        message = f"unknown channels: {', '.join(unknown)}; allowed: {', '.join(sorted(ALLOWED_CHANNELS))}"
        raise ValueError(message)
    return unique_channels


def build_channels_payload(user_index: int, channels: list[str]) -> list[dict[str, object]]:
    user_channels: list[dict[str, object]] = []
    for channel in channels:
        address = f"seed_user_{user_index}@example.com" if channel == "email" else f"+1{user_index:010d}"
        user_channels.append({
            "channel": channel,
            "address": address,
            "status": "active",
            "verified": True,
        })
    return user_channels


def build_batch_payload(start_index: int, count: int, channels: list[str]) -> dict[str, object]:
    items: list[dict[str, object]] = [
        {
            "externalId": f"seed_user_{index}",
            "status": "active",
            "channels": build_channels_payload(index, channels),
        }
        for index in range(start_index, start_index + count)
    ]
    return {
        "mode": "skip_duplicates",
        "items": items,
    }


def post_batch_with_retry(
    client: httpx.Client,
    url: str,
    headers: dict[str, str],
    payload: dict[str, object],
    max_retries: int,
) -> dict[str, object]:
    idempotency_key = headers["Idempotency-Key"]
    attempt = 0
    while True:
        attempt += 1
        try:
            response = client.post(url, json=payload, headers=headers)
            if response.status_code >= HTTP_SERVER_ERROR_MIN and attempt <= max_retries:
                print(
                    f"[retry] {idempotency_key}: server {response.status_code}, attempt {attempt}/{max_retries}",
                    file=sys.stderr,
                )
                time.sleep(min(1.0 * attempt, 3.0))
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                message = f"{idempotency_key}: response JSON is not an object"
                raise TypeError(message)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt > max_retries:
                message = f"{idempotency_key}: transport error after retries: {exc}"
                raise RuntimeError(message) from exc
            print(f"[retry] {idempotency_key}: transport error, attempt {attempt}/{max_retries}", file=sys.stderr)
            time.sleep(min(1.0 * attempt, 3.0))
        except httpx.HTTPStatusError as exc:
            body = exc.response.text
            message = f"{idempotency_key}: HTTP {exc.response.status_code} {exc.response.reason_phrase}; body: {body}"
            raise RuntimeError(message) from exc
        else:
            return data


def validate_args(args: argparse.Namespace) -> str | None:
    validations = [
        (args.total > 0, "--total must be > 0"),
        (args.batch_size > 0, "--batch-size must be > 0"),
        (args.batch_size <= DEFAULT_BATCH_SIZE, "--batch-size must be <= 1000 (default API limit)"),
        (args.max_retries >= 0, "--max-retries must be >= 0"),
        (1 <= args.workers <= MAX_WORKERS, f"--workers must be between 1 and {MAX_WORKERS}"),
        (
            bool(args.auth_token or args.manager_login.strip()),
            "--manager-login must not be empty when --auth-token is not provided",
        ),
        (
            bool(args.auth_token or args.manager_password),
            "--manager-password must not be empty when --auth-token is not provided",
        ),
    ]
    for is_valid, message in validations:
        if not is_valid:
            return message
    return None


def bootstrap_manager(login: str, password: str) -> None:
    password_hash = hash_password(password)
    conn = open_db_connection()
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(
                """
                    INSERT INTO managers (login, password_hash, status)
                    VALUES (%s, %s, 'active')
                    ON CONFLICT (login)
                    DO UPDATE SET
                        password_hash = EXCLUDED.password_hash,
                        status = 'active'
                    """,
                (login, password_hash),
            )
    finally:
        conn.close()


def open_db_connection() -> psycopg2.extensions.connection:
    config = GlobalConfig.load()
    return psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        user=config.postgres_user,
        password=config.postgres_password,
        dbname=config.postgres_db,
    )


def bootstrap_channels(channels: list[str]) -> None:
    conn = open_db_connection()
    try:
        with conn, conn.cursor() as cursor:
            for code in channels:
                display_name, queue_group = CHANNEL_DEFINITIONS[code]
                cursor.execute(
                    """
                    INSERT INTO channels (code, display_name, state, queue_group)
                    VALUES (%s, %s, 'enabled', %s)
                    ON CONFLICT (code)
                    DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        state = 'enabled',
                        queue_group = EXCLUDED.queue_group
                    """,
                    (code, display_name, queue_group),
                )
    finally:
        conn.close()


def login_and_get_token(
    *,
    client: httpx.Client,
    api_url: str,
    login: str,
    password: str,
) -> str:
    login_url = f"{api_url.rstrip('/')}/auth/login"
    try:
        response = client.post(
            login_url,
            json={"login": login, "password": password},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except httpx.ConnectError as exc:
        message = (
            f"cannot connect to API at {api_url}. "
            "Start API first (for example: `uv run uvicorn notifications_api.app.litestar:app --host 0.0.0.0 --port 8000` "
            "or `docker compose up --build`) or pass correct --api-url."
        )
        raise RuntimeError(message) from exc
    except httpx.HTTPStatusError as exc:
        message = (
            f"login failed with HTTP {exc.response.status_code} {exc.response.reason_phrase}; "
            f"body: {exc.response.text}"
        )
        raise RuntimeError(message) from exc
    except httpx.TransportError as exc:
        message = f"transport error during /auth/login: {exc}"
        raise RuntimeError(message) from exc

    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("login response JSON is not an object")
    raw_token = payload.get("accessToken")
    if not isinstance(raw_token, str) or not raw_token.strip():
        raise RuntimeError("login response does not contain accessToken")
    return raw_token.strip()


def resolve_auth_token(args: argparse.Namespace, client: httpx.Client) -> str:
    if args.auth_token:
        return str(args.auth_token).strip()

    login = str(args.manager_login).strip().lower()
    if not args.no_bootstrap_manager:
        print(f"Bootstrapping manager login='{login}' in DB...")
        bootstrap_manager(login=login, password=args.manager_password)

    print(f"Logging in as manager login='{login}'...")
    return login_and_get_token(
        client=client,
        api_url=args.api_url,
        login=login,
        password=args.manager_password,
    )


def build_batch_tasks(*, total: int, batch_size: int) -> list[BatchTask]:
    tasks: list[BatchTask] = []
    processed_users = 0
    batch_no = 0
    while processed_users < total:
        batch_no += 1
        remaining = total - processed_users
        batch_count = min(batch_size, remaining)
        start_index = processed_users + 1
        tasks.append(BatchTask(batch_no=batch_no, start_index=start_index, batch_count=batch_count))
        processed_users += batch_count
    return tasks


def send_batch_task(
    *,
    task: BatchTask,
    args: argparse.Namespace,
    state: SeedExecutionState,
) -> BatchResult:
    with httpx.Client(timeout=args.timeout_seconds) as client:
        payload = build_batch_payload(start_index=task.start_index, count=task.batch_count, channels=state.channels)
        idempotency_key = f"{state.idempotency_key_prefix}-{task.batch_no}"
        headers = {
            "Authorization": f"Bearer {state.token}",
            "Idempotency-Key": idempotency_key,
            "Content-Type": "application/json",
        }
        batch_data = post_batch_with_retry(
            client=client,
            url=state.users_bulk_url,
            headers=headers,
            payload=payload,
            max_retries=args.max_retries,
        )
    return BatchResult(batch_no=task.batch_no, batch_count=task.batch_count, payload=batch_data)


def execute_seed_batches(
    args: argparse.Namespace,
    state: SeedExecutionState,
) -> int:
    total_batches = len(state.batch_tasks)
    processed_users = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        pending: dict[Future[BatchResult], BatchTask] = {}
        task_index = 0

        while task_index < total_batches or pending:
            while task_index < total_batches and len(pending) < args.workers:
                task = state.batch_tasks[task_index]
                future = executor.submit(
                    send_batch_task,
                    task=task,
                    args=args,
                    state=state,
                )
                pending[future] = task
                task_index += 1

            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for finished in done:
                pending.pop(finished)
                result = finished.result()
                state.totals.add_payload(result.payload)
                processed_users += result.batch_count
                elapsed = time.time() - state.start_ts
                rate = processed_users / elapsed if elapsed > 0 else 0.0
                print(
                    f"[{result.batch_no}/{total_batches}] users={processed_users}/{args.total} "
                    f"inserted={state.totals.inserted_users} skipped={state.totals.skipped_users} "
                    f"errors={state.totals.errors} rate={rate:.1f} users/s"
                )
    return processed_users


def main() -> int:
    args = parse_args()
    args_error = validate_args(args)
    if args_error is not None:
        print(args_error, file=sys.stderr)
        return 2

    try:
        channels = parse_channels(args.channels)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    users_bulk_url = f"{args.api_url.rstrip('/')}/users/bulk"
    run_id = args.idempotency_run_id.strip() or datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    idempotency_key_prefix = f"{args.idempotency_prefix}-{run_id}"
    start_ts = time.time()
    totals = BatchStats()
    batch_tasks = build_batch_tasks(total=args.total, batch_size=args.batch_size)
    total_batches = len(batch_tasks)

    print(
        f"Seeding {args.total} users via {users_bulk_url} "
        f"(batch_size={args.batch_size}, batches={total_batches}, workers={args.workers}, channels={','.join(channels)})"
    )
    print(f"Idempotency key prefix for this run: {idempotency_key_prefix}")

    try:
        if not args.no_bootstrap_channels:
            print(f"Bootstrapping channels in DB: {','.join(channels)}...")
            bootstrap_channels(channels)

        with httpx.Client(timeout=args.timeout_seconds) as client:
            token = resolve_auth_token(args, client)
            print("JWT acquired via /auth/login.")
        state = SeedExecutionState(
            channels=channels,
            token=token,
            users_bulk_url=users_bulk_url,
            batch_tasks=batch_tasks,
            totals=totals,
            start_ts=start_ts,
            idempotency_key_prefix=idempotency_key_prefix,
        )
        processed_users = execute_seed_batches(args, state)
    except (RuntimeError, TypeError, ValueError) as exc:
        print(f"seed failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"seed failed: unexpected error: {exc}", file=sys.stderr)
        return 1

    total_elapsed = time.time() - start_ts
    print("\nDone.")
    print(f"elapsed_seconds={total_elapsed:.2f}")
    print(f"processed_users={processed_users}")
    print(f"inserted_users={totals.inserted_users}")
    print(f"inserted_user_channels={totals.inserted_user_channels}")
    print(f"updated_users={totals.updated_users}")
    print(f"updated_user_channels={totals.updated_user_channels}")
    print(f"skipped_users={totals.skipped_users}")
    print(f"skipped_user_channels={totals.skipped_user_channels}")
    print(f"payload_errors={totals.errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
