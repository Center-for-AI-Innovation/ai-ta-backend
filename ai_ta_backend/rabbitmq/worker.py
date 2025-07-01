import os
import ssl
import pika
import logging
import uuid
import json
import threading

from rmsql import SQLAlchemyIngestDB
from ingest import Ingest
from flask import Flask, request, jsonify


app = Flask(__name__)
sql_session = SQLAlchemyIngestDB()  # TODO: Move into the class?

class Worker:

    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://uiuc-chat-dev:password@localhost:5672')
        self.rabbitmq_ssl = os.getenv('RABBITMQ_SSL', True)
        self.rabbitmq_queue = os.getenv('RABBITMQ_QUEUE', 'uiuc-chat')
        self.connect()

    # Intended usage is "with Queue() as queue:"
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.channel.close()
        self.connection.close()

    def connect(self):
        parameters = pika.URLParameters(self.rabbitmq_url)
        if self.rabbitmq_ssl:
            # Necessary for AWS AmazonMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rabbitmq_queue, durable=True)

    def is_connected(self):
        return (
                hasattr(self, 'connection') and self.connection.is_open and
                hasattr(self, 'channel') and self.channel.is_open
        )

    def process_job(self, channel, method, properties, body):
        content = json.loads(body.decode())
        job_id = content['job_id']
        logging.info("----------------------------------------")
        logging.info("--------------Incoming job--------------")
        logging.info("----------------------------------------")
        inputs = content['inputs']
        logging.info(inputs)

        ingester = Ingest()
        try:
            ingester.main_ingest(**inputs)
            sql_session.delete_document_in_progress(job_id)
        finally:
            # TODO: Catch errors into a retry loop or something else?
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


@app.route('/api/healthcheck', methods=['GET'])
def return_health():
    message = {'status': 'OK'}
    return jsonify(message)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    worker = Worker()
    worker_thread = threading.Thread(target=worker.listen_for_jobs)
    worker_thread.start()
    worker_thread.join(0)
    logging.info("Running healthcheck endpoint")
    app.run(host='0.0.0.0', port=8001)
