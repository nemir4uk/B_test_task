import pika
from config import settings


class RabbitConnectorClass:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def __enter__(self) -> pika.BlockingConnection:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port,
                              credentials=pika.PlainCredentials(username=self.username, password=self.password)))
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


rabbit_connector = RabbitConnectorClass(settings.rabbit_host, settings.rabbit_port,
                                        settings.rabbit_user, settings.rabbit_pass)
