import os
import pika
import logging
import uuid
import json

from sql import SQLAlchemyIngestDB
from ingest import Ingest


# TODO: Move into the class?
sql_session = SQLAlchemyIngestDB()

class Worker:

    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = os.getenv('RABBITMQ_PORT', '5672')
        self.rabbitmq_username = os.getenv('RABBITMQ_USERNAME', 'uiuc-chat-dev')
        self.rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'password')
        self.rabbitmq_queue = os.getenv('RABBITMQ_QUEUE', 'uiuc-chat')
        self.connect()

    # Intended usage is "with Queue() as queue:"
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.channel.close()
        self.connection.close()

    def connect(self):
        credentials = pika.PlainCredentials(self.rabbitmq_username, self.rabbitmq_password)
        parameters = pika.ConnectionParameters(
            host=self.rabbitmq_host,
            port=self.rabbitmq_port,
            credentials=credentials
        )

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rabbitmq_queue, durable=True)

    def is_connected(self):
        return (
                hasattr(self, 'connection') and self.connection.is_open and
                hasattr(self, 'channel') and self.channel.is_open
        )

    def process_job(self, channel, method, properties, body):
        inputs = json.loads(body.decode())
        logging.info("--Incoming job--")
        logging.info(inputs)
        ingester = Ingest()
        ingester.main_ingest(**inputs)

        #sql_session.delete_document_in_progress(inputs['job_id'])

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def listen_for_jobs(self):
        if not self.is_connected():
            logging.error("RabbitMQ is offline")
            return

        self.channel.basic_consume(
            queue=self.rabbitmq_queue,
            on_message_callback=self.process_job,
            auto_ack=False
        )

        logging.info("Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = Worker()
    worker.listen_for_jobs()
