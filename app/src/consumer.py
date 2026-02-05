import logging
import pika, sys, os
import pydantic_core
from sqlalchemy.exc import SQLAlchemyError
from src.rabbitmq import rabbit_connector
from src.db import ConsumedData, create_if_not_exist, Session_pg, insert_values
from src.config import settings
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def callback_with_retry(ch, method, properties, body):
    logger.info('message received')
    headers = properties.headers or {}
    death_headers = headers.get('x-death')
    retry_count = 0
    if death_headers:
        retry_count = death_headers[0].get('count', 0)
    max_retries = settings.retry_count
    try:
        data = ConsumedData.model_validate_json(body)
        insert_values(Session_pg, data.model_dump(mode="json"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except pydantic_core._pydantic_core.ValidationError:
        logger.error(f'pydantic ValidationError with message {body}')
        logger.error('sent to the dead queue')
        ch.basic_publish(
            exchange='dlx',
            routing_key='failed',
            body=body,
            properties=pika.BasicProperties(
                headers={'x-retry-count': retry_count, 'x-error': 'ValidationError'}
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except SQLAlchemyError:
        logger.info('SQLAlchemyError')
        if retry_count < max_retries:
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        else:
            logger.error('SQLAlchemyError, exceeded number of retry -> to dead queue')
            ch.basic_publish(
                exchange='dlx',
                routing_key='failed',
                body=body,
                properties=pika.BasicProperties(
                    headers={'x-retry-count': retry_count, 'x-error': 'Exceeded number of retry'}
                )
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.critical(f'Unexpected error {e}')
        ch.basic_publish(
            exchange='dlx',
            routing_key='failed',
            body=body,
            properties=pika.BasicProperties(
                headers={'x-retry-count': retry_count, 'x-error': 'Unexpected error'}
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    create_if_not_exist(Session_pg)
    with rabbit_connector as conn:
        channel = conn.channel()
# dead letters
        channel.exchange_declare(exchange='dlx', exchange_type='direct', durable=True)
        channel.queue_declare(queue='dead_letters', durable=True)
        channel.queue_bind(exchange='dlx', queue='dead_letters', routing_key='failed')
# retry
        channel.exchange_declare(exchange='retry', exchange_type='direct', durable=True)
        channel.queue_declare(
            queue='retry_5s',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'main',
                'x-dead-letter-routing-key': settings.rabbit_queue,
                'x-message-ttl': 5000
            }
        )
        channel.queue_bind(exchange='retry', queue='retry_5s', routing_key='retry')
# main
        channel.exchange_declare(exchange='main', exchange_type='direct', durable=True)
        channel.queue_declare(
            queue=settings.rabbit_queue,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'retry',
                'x-dead-letter-routing-key': 'retry'
            }
        )
        channel.queue_bind(
            exchange='main',
            queue=settings.rabbit_queue,
            routing_key=settings.rabbit_queue
        )

        channel.basic_consume(queue=settings.rabbit_queue, on_message_callback=callback_with_retry, auto_ack=False)
        channel.start_consuming()


if __name__ == '__main__':
    logger.info('Start main process')
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)