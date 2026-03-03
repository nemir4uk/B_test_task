import asyncio
import logging
import aio_pika
from aio_pika import Message, DeliveryMode
import pydantic_core
from sqlalchemy.exc import SQLAlchemyError
from src.rabbitmq import rabbit_connector
from src.db import ConsumedData, create_if_not_exist, Async_Session_pg, insert_values
from src.config import settings
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def callback_with_retry(message: aio_pika.IncomingMessage):
    async with message.process(ignore_processed=True):
        logger.info("message received")

        headers = message.headers or {}
        death_headers = headers.get("x-death")
        retry_count = 0

        if death_headers:
            retry_count = death_headers[0].get("count", 0)

        max_retries = settings.retry_count
        body = message.body
        channel = message.channel

        try:
            data = ConsumedData.model_validate_json(body)
            await insert_values(Async_Session_pg, data.model_dump(mode="json"))
            await message.ack()

        except pydantic_core._pydantic_core.ValidationError:
            logger.error(f"pydantic ValidationError with message {body}")
            logger.error("sent to the dead queue")
            await channel.default_exchange.publish(
                Message(
                    body,
                    headers={
                        "x-retry-count": retry_count,
                        "x-error": "ValidationError",
                    },
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key="failed",
            )
            await message.ack()

        except SQLAlchemyError:
            logger.info("SQLAlchemyError")
            if retry_count < max_retries:
                await message.reject(requeue=False)
            else:
                logger.error("SQLAlchemyError, exceeded number of retry -> to dead queue")
                await channel.default_exchange.publish(
                    Message(
                        body,
                        headers={
                            "x-retry-count": retry_count,
                            "x-error": "Exceeded number of retry",
                        },
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key="failed",
                )
                await message.ack()

        except Exception as e:
            logging.critical(f"Unexpected error {e}")
            await channel.default_exchange.publish(
                Message(
                    body,
                    headers={
                        "x-retry-count": retry_count,
                        "x-error": "Unexpected error",
                    },
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key="failed",
            )
            await message.ack()


async def main():
    await create_if_not_exist(Async_Session_pg)
    async with rabbit_connector as connection:
        channel = await connection.channel()
# dead letters
        dlx = await channel.declare_exchange("dlx", type='direct', durable=True)
        dead_queue = await channel.declare_queue("dead_letters", durable=True)
        await dead_queue.bind(dlx, "failed")
# retry
        retry_exchange = await channel.declare_exchange("retry", type='direct', durable=True)
        retry_queue = await channel.declare_queue(
            "retry_5s",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "main",
                "x-dead-letter-routing-key": settings.rabbit_queue,
                "x-message-ttl": 5000,
            },
        )
        await retry_queue.bind(retry_exchange, "retry")
# main
        main_exchange = await channel.declare_exchange("main", type='direct', durable=True)
        queue = await channel.declare_queue(
            settings.rabbit_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "retry",
                "x-dead-letter-routing-key": "retry",
            },
        )
        await queue.bind(main_exchange, settings.rabbit_queue)
        await queue.consume(callback_with_retry)
        logger.info("Start consuming...")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass