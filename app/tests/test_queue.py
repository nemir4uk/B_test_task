import pytest
from sqlalchemy import create_engine, text, inspect
import pika
from pika.exceptions import ChannelClosedByBroker
from datetime import datetime
from pydantic import BaseModel
from src.rabbitmq import rabbit_connector
from src.config import settings


class Metadata(BaseModel):
    timestamp: datetime


class ConsumedData(BaseModel):
    data: dict
    metadata: Metadata


@pytest.mark.parametrize('queue_name', ['dead_letters', 'retry_5s', settings.rabbit_queue])
def test_check_queues_exists(queue_name):
    with rabbit_connector as conn:
        channel = conn.channel()
        try:
            result = channel.queue_declare(queue=queue_name, passive=True)
            assert True
        except ChannelClosedByBroker as e:
            pytest.fail(f"Queue '{queue_name}' was not found: {e}")


@pytest.mark.parametrize('body',
[str({"data": {12345}, "metadata": {"timestamp": 123}}),
ConsumedData(data={123:{1:'d',2:{}}}, metadata=Metadata(timestamp=datetime.now())).model_dump_json()
 ])
def test_message_consuming(body):
    with rabbit_connector as conn:
        channel = conn.channel()
        try:
            channel.basic_publish(exchange='', routing_key='secret_queue', body=body)
            assert True
        except ChannelClosedByBroker as e:
            pytest.fail(f"basic_publish make an exception: {e}")
