import os
import ssl
import traceback
from typing import List, Optional

import pika
import logging
import json
import threading

from ingest import Ingest
from flask import Flask, jsonify


app = Flask(__name__)

BACKOFF_BASE = float(os.getenv('BACKOFF_BASE', '1.0'))   # seconds
BACKOFF_MAX  = float(os.getenv('BACKOFF_MAX', '30.0'))   # seconds

stop_event = threading.Event()
worker_thread: threading.Thread | None = None
worker_running = threading.Event()


class Worker:

    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672')
        self.rabbitmq_ssl = os.getenv('RABBITMQ_SSL', False)
        self.rabbitmq_queue = os.getenv('RABBITMQ_QUEUE', 'uiuc-chat')
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None
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

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

        self.channel = None
        self.connection = None

    def is_connected(self) -> bool:
        return (
                self.connection is not None
                and self.connection.is_open
                and self.channel is not None
                and self.channel.is_open
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
            ingester.main_ingest(job_id=job_id, **inputs)
            #sql_session
        finally:
            # TODO: Catch errors into a retry loop or something else?
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def listen_for_jobs(self):
        backoff = BACKOFF_BASE
        while not stop_event.is_set():
            try:
                logging.info("Worker connecting to RabbitMQ...")
                if not self.is_connected():
                    logging.error("RabbitMQ is offline")
                    return

                logging.info("Worker connected to RabbitMQ")

                self.channel.basic_consume(
                    queue=self.rabbitmq_queue,
                    on_message_callback=self.process_job,
                    auto_ack=False
                )

                logging.info("Waiting for messages. To exit press CTRL+C")
                worker_running.set()  # mark healthy
                self.channel.start_consuming()

            except Exception:
                worker_running.clear()
                logging.error("Worker crashed/disconnected:\n%s", traceback.format_exc())
                self.close()

                # backoff with cap
                if stop_event.wait(backoff):
                    break
                backoff = min(backoff * 2, BACKOFF_MAX)

        # final cleanup
        worker_running.clear()
        self.close()
        logging.info("Worker exiting")


@app.route('/api/healthcheck', methods=['GET'])
def return_health():
    # Healthy when the worker loop is actively consuming
    status = {
        "status": "OK" if worker_running.is_set() else "DOWN",
        "worker_thread_alive": worker_thread.is_alive() if worker_thread else False
    }
    return jsonify(status), (200 if worker_running.is_set() else 500)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    worker = Worker()
    worker_thread = threading.Thread(target=worker.listen_for_jobs)
    worker_thread.start()
    worker_thread.join(0)

    logging.info("Running healthcheck endpoint")
    try:
        # threaded=True lets Flask serve multiple requests while worker runs
        app.run(host='0.0.0.0', port=8001, threaded=True)
    finally:
        # Graceful shutdown
        stop_event.set()
        if worker_thread:
            worker_thread.join(timeout=10)
