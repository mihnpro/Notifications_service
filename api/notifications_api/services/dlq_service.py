from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.repositories.delivery_attempt_repository import DeliveryAttemptRepository
from notifications_api.adapters.repositories.delivery_task_repository import DeliveryTaskRepository
from notifications_api.adapters.repositories.dlq_repository import DlqRepository
from notifications_api.adapters.repositories.outbox_repository import OutboxWriter
from notifications_api.domain.dlq import (
    DeliveryAttemptErrorType,
    DeliveryStatus,
    DlqItemView,
    DlqReasonCode,
    DlqStatus,
    OutboxEventType,
    ReplayResult,
    StatsCounter,
)
from notifications_api.services.stats_helper import CampaignStatsHelper

# Statuses that are already terminal — no further dead-letter writes.
_TERMINAL_STATUSES = frozenset({
    DeliveryStatus.SUCCEEDED.value,
    DeliveryStatus.CANCELLED.value,
    DeliveryStatus.DEAD_LETTERED.value,
})

# Statuses → which counter to decrement when dead_letter fires.
_STATS_FROM_DEAD_LETTER: dict[str, StatsCounter] = {
    DeliveryStatus.QUEUED.value: StatsCounter.QUEUED,
    DeliveryStatus.SENDING.value: StatsCounter.SENDING,
    DeliveryStatus.FAILED.value: StatsCounter.FAILED,
    DeliveryStatus.RETRY_SCHEDULED.value: StatsCounter.RETRY_SCHEDULED,
}


class DlqService:
    """Orchestrates dead-letter and replay flows across multiple tables.

    All operations run inside the caller's session — caller controls
    commit/rollback. For the HTTP entrypoint we wrap each item in its own
    ``session.begin()`` so partial replay batches are visible.
    """

    def __init__(
        self,
        session: AsyncSession,
        tasks: DeliveryTaskRepository,
        attempts: DeliveryAttemptRepository,
        dlq: DlqRepository,
        outbox: OutboxWriter,
        stats: CampaignStatsHelper,
    ) -> None:
        self._session = session
        self._tasks = tasks
        self._attempts = attempts
        self._dlq = dlq
        self._outbox = outbox
        self._stats = stats

    async def dead_letter(
        self,
        task_id: UUID,
        *,
        reason_code: DlqReasonCode,
        error_code: str | None,
        error_message: str | None,
        error_type: DeliveryAttemptErrorType = DeliveryAttemptErrorType.UNKNOWN,
    ) -> bool:
        """Move a delivery task to dead_lettered terminal state.

        Returns True if state was changed, False on no-op (task already terminal
        or row already in DLQ).
        """
        async with self._session.begin():
            task = await self._tasks.get_for_update(task_id)
            if task is None:
                return False
            if task.status in _TERMINAL_STATUSES:
                return False

            previous_status = task.status

            await self._tasks.mark_dead_lettered(task_id, error_code, error_message)
            await self._attempts.insert_final_failure(
                task_id=task_id,
                campaign_id=task.campaign_id,
                attempt_no=task.attempt_count,
                channel_code=task.channel_code,
                error_type=error_type,
                error_code=error_code,
                error_message=error_message,
            )
            inserted = await self._dlq.insert_open(
                task_id=task_id,
                campaign_id=task.campaign_id,
                channel_code=task.channel_code,
                reason_code=reason_code.value,
                error_code=error_code,
                error_message=error_message,
            )
            if not inserted:
                # Race lost — another writer already wrote DLQ; treat as success
                # but skip the stats transition to keep counters balanced.
                return False

            await self._stats.transition(
                task.campaign_id,
                from_status=_STATS_FROM_DEAD_LETTER.get(previous_status),
                to_status=StatsCounter.DEAD_LETTERED,
            )
        return True

    async def replay(self, dlq_item_ids: list[UUID]) -> ReplayResult:
        replayed: list[UUID] = []
        skipped: list[UUID] = []
        not_found: list[UUID] = []

        for dlq_item_id in dlq_item_ids:
            outcome = await self._replay_one(dlq_item_id)
            match outcome:
                case "replayed":
                    replayed.append(dlq_item_id)
                case "skipped":
                    skipped.append(dlq_item_id)
                case "not_found":
                    not_found.append(dlq_item_id)

        return ReplayResult(replayed=replayed, skipped=skipped, not_found=not_found)

    async def _replay_one(self, dlq_item_id: UUID) -> str:
        async with self._session.begin():
            dlq_item = await self._dlq.get_open_for_update(dlq_item_id)
            if dlq_item is None:
                exists = await self._dlq.exists(dlq_item_id)
                return "skipped" if exists else "not_found"

            task = await self._tasks.get_for_update(dlq_item.task_id)
            if task is None:
                return "not_found"

            await self._tasks.reset_for_replay(task.id)
            await self._dlq.mark_replayed(dlq_item_id)
            await self._outbox.insert(
                event_type=OutboxEventType.TASK_REPLAY.value,
                payload={
                    "task_id": str(task.id),
                    "campaign_id": str(task.campaign_id),
                    "channel_code": task.channel_code,
                    "queue_group": task.queue_group,
                    "dlq_item_id": str(dlq_item_id),
                },
                exchange="notification.direct",
                routing_key=task.queue_group,
                dedupe_key=f"replay:{dlq_item_id}",
                region_id=task.region_id,
            )
            await self._stats.transition(
                task.campaign_id,
                from_status=StatsCounter.DEAD_LETTERED,
                to_status=StatsCounter.QUEUED,
            )
        return "replayed"

    async def list_items(
        self,
        *,
        campaign_id: UUID | None,
        status: DlqStatus | None,
        limit: int,
        offset: int,
    ) -> list[DlqItemView]:
        rows = await self._dlq.list_filtered(
            campaign_id=campaign_id, status=status, limit=limit, offset=offset
        )
        return [DlqItemView.model_validate(r) for r in rows]
