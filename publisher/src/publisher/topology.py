import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

MAIN_EXCHANGE = "notification.direct"
DLX_EXCHANGE = "notification.dlx"
DLQ_QUEUE = "notification.tasks.dead"
DLQ_ROUTING_KEY = "notification.tasks.dead"

_REGIONS = ["default"]
_PRIORITIES = ["high", "normal", "low"]
_QUEUE_GROUPS = ["email", "sms"]


def _all_queues() -> list[str]:
    queues = []
    for region in _REGIONS:
        for priority in _PRIORITIES:
            queues.append(f"notification.{region}.fanout.{priority}")
        for group in _QUEUE_GROUPS:
            for priority in _PRIORITIES:
                queues.append(f"notification.{region}.{group}.{priority}")
    return queues


async def declare_topology(connection: AbstractRobustConnection) -> None:
    channel: AbstractRobustChannel = await connection.channel()
    try:
        main_exchange = await channel.declare_exchange(
            MAIN_EXCHANGE,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        dlx_exchange = await channel.declare_exchange(
            DLX_EXCHANGE,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        dlq = await channel.declare_queue(DLQ_QUEUE, durable=True)
        await dlq.bind(dlx_exchange, routing_key=DLQ_ROUTING_KEY)

        for queue_name in _all_queues():
            q = await channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": DLX_EXCHANGE,
                    "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
                },
            )
            await q.bind(main_exchange, routing_key=queue_name)
    finally:
        await channel.close()
