import time
import pytest
from faker import Faker
from pika.exceptions import ChannelClosedByBroker
from datetime import datetime
from pydantic import BaseModel
from src.rabbitmq import rabbit_connector
from src.config import settings

fake = Faker()


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
    [ConsumedData(data=fake.pydict(), metadata=Metadata(timestamp=fake.date_time_between(f'-500d', f'-2d'))).model_dump_json(),
    ConsumedData(data=fake.pydict(), metadata=Metadata(timestamp=fake.date_time_between(f'-5d', f'-2d'))).model_dump_json(),
    ConsumedData(data={123:{1:'d',2:{}}}, metadata=Metadata(timestamp=datetime.now())).model_dump_json()
 ])
def test_correct_message_consuming(body):
    with rabbit_connector as conn:
        channel = conn.channel()
        try:
            result = channel.queue_declare(queue=settings.rabbit_queue, passive=True)
            messagecount_before = result.method.message_count

            channel.basic_publish(exchange='', routing_key=settings.rabbit_queue, body=body)
            time.sleep(1)
            result = channel.queue_declare(queue=settings.rabbit_queue, passive=True)
            messagecount_after = result.method.message_count

            assert messagecount_before == messagecount_after
        except ChannelClosedByBroker as e:
            pytest.fail(f"basic_publish make an exception: {e}")


@pytest.mark.parametrize('body',
    [str({"data": {12345}, "metadata": {"timestamp": 123}}),
    str({"data": 12345, "metadata": {"timestamp": 123}}),
    str({"data": '{12345}'})
     ])
def test_incorrect_message_consuming(body):
    with rabbit_connector as conn:
        channel = conn.channel()
        try:
            result = channel.queue_declare(queue='dead_letters', passive=True)
            messagecount_before = result.method.message_count

            channel.basic_publish(exchange='', routing_key=settings.rabbit_queue, body=body)
            time.sleep(1)
            result = channel.queue_declare(queue='dead_letters', passive=True)
            messagecount_after = result.method.message_count

            assert messagecount_before == messagecount_after - 1
        except ChannelClosedByBroker as e:
            pytest.fail(f"basic_publish make an exception: {e}")