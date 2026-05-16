import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

MAIN_EXCHANGE = "notification.direct"
MAIN_QUEUE = "notification.tasks"
DLX_EXCHANGE = "notification.dlx"
DLQ_QUEUE = "notification.tasks.dead"


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
        await dlq.bind(dlx_exchange, routing_key=MAIN_QUEUE)

        main_queue = await channel.declare_queue(
            MAIN_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )
        await main_queue.bind(main_exchange, routing_key=MAIN_QUEUE)
    finally:
        await channel.close()
