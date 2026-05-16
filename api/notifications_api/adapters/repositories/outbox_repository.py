
import sqlalchemy as sa

from notifications_api.adapters.postgres_models.models import OutboxEventORM

from sqlalchemy.ext.asyncio import AsyncSession


class OutboxWriter:
    """Writer-side companion to publisher.OutboxRepository (which only reads).

    Inserts events into outbox_events. Publisher's relay loop will pick them up.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        event_type: str,
        payload: dict[str, object],
        exchange: str,
        routing_key: str,
        dedupe_key: str,
        region_id: str = "default",
    ) -> None:
        stmt = sa.insert(OutboxEventORM).values(
            event_type=event_type,
            payload=payload,
            exchange=exchange,
            routing_key=routing_key,
            dedupe_key=dedupe_key,
            region_id=region_id,
        )
        await self._session.execute(stmt)
