import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection
from aiormq.exceptions import DeliveryError

from publisher.domain.outbox import OutboxRow


class UnroutableMessageError(RuntimeError):
    """Raised when the broker returns a message because no queue matched.

    Surfaces topology/routing-key drift instead of silently losing the message.
    """


class Broker:
    """RabbitMQ publisher with publisher confirms + mandatory.

    aio-pika channels in default `publisher_confirms=True` mode await
    `basic.ack` from the broker on every `publish()` — that's our durability gate.
    `mandatory=True` turns an unroutable message into a `DeliveryError` instead
    of a silent drop, so Relay can mark_retry and an operator can investigate.
    """

    def __init__(self, url: str, publish_timeout_sec: float) -> None:
        self._url = url
        self._timeout = publish_timeout_sec
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel(publisher_confirms=True)

    @property
    def connection(self) -> AbstractRobustConnection:
        if self._connection is None:
            raise RuntimeError("Broker not connected")
        return self._connection

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None

    async def publish(self, row: OutboxRow) -> None:
        if self._channel is None:
            raise RuntimeError("Broker not connected")
        exchange = await self._channel.get_exchange(row.exchange, ensure=False)
        message = Message(
            body=row.payload.encode("utf-8"),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=row.dedupe_key,
            type=row.event_type,
        )
        try:
            await exchange.publish(
                message,
                routing_key=row.routing_key,
                mandatory=True,
                timeout=self._timeout,
            )
        except DeliveryError as exc:
            raise UnroutableMessageError(
                f"unroutable: exchange={row.exchange} routing_key={row.routing_key}"
            ) from exc
