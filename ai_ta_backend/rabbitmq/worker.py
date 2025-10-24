import os
import ssl
import traceback
from typing import List, Optional

import pika
import logging
import json
import threading

from rmsql import SQLAlchemyIngestDB
from ingest import Ingest
from flask import Flask, jsonify


app = Flask(__name__)

BACKOFF_BASE = float(os.getenv('BACKOFF_BASE', '1.0'))   # seconds
BACKOFF_MAX  = float(os.getenv('BACKOFF_MAX', '30.0'))   # seconds
PREFETCH_COUNT = int(os.getenv('RABBITMQ_PREFETCH_COUNT', '1'))  # messages
MAX_JOB_RETRIES = int(os.getenv('MAX_JOB_RETRIES', '10'))  # messages

stop_event = threading.Event()
worker_thread: threading.Thread | None = None
worker_running = threading.Event()




logging.getLogger('pika').setLevel(logging.WARNING)

sql_session = SQLAlchemyIngestDB()

class Worker:

    def __init__(self):
        self.consumer = None
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
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming(self.consumer)
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

    def process_job(self, channel, method, header, body):
        content = json.loads(body.decode())
        job_id = content['job_id']
        logging.info("----------------------------------------")
        logging.info("--------------Incoming job--------------")
        logging.info("----------------------------------------")
        inputs = content['inputs']
        logging.info(inputs)

        retry_count = content['retry_count'] if 'retry_count' in content else 0

        try:
            # flag this message as "processing started" so we can ack it later if memory runs out

            prog_doc = sql_session.fetch_document_in_progress(job_id)
            if not prog_doc:
                logging.error(f"Job ID {job_id} not found in DocumentsInProgress table.")
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                if "error" in prog_doc and prog_doc["error"] == 'Attempting ingest':
                    sql_session.insert_failed_document({
                        "s3_path": str(prog_doc["s3_path"]),
                        "readable_filename": prog_doc["readable_filename"],
                        "course_name": prog_doc["course_name"],
                        "url": prog_doc["url"],
                        "base_url": prog_doc["base_url"],
                        "doc_groups": prog_doc["doc_groups"],
                        "error": "Ingest could not resolve successfully, worker crashed (e.g. ran out of memory)",
                    })
                    sql_session.delete_document_in_progress(job_id)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    prog_doc["error"] = 'Attempting ingest'
                    sql_session.update_document_in_progress(prog_doc)

                    ingester = Ingest()
                    ingester.main_ingest(job_id=job_id, **inputs)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            if retry_count < MAX_JOB_RETRIES:
                logging.info(f"Resubmitting {job_id} to queue (retry #{retry_count+1})")
                content["retry_count"] = retry_count + 1
                properties = pika.BasicProperties(delivery_mode=2, reply_to=header.reply_to)
                # TODO: Channel is likely broken here
                self.connect()
                self.channel.basic_publish(exchange='',
                                      routing_key=self.rabbitmq_queue,
                                      properties=properties,
                                      body=json.dumps(content))
            else:
                logging.info(f"Job {job_id} failed after {MAX_JOB_RETRIES} retries. Discarding job.")

    def listen_for_jobs(self):
        backoff = BACKOFF_BASE
        while not stop_event.is_set():
            logging.info("Worker connecting to RabbitMQ...")
            if not self.is_connected():
                logging.error("RabbitMQ is offline")
                return

            logging.info("Worker connected to RabbitMQ")
            self.consumer = self.channel.basic_consume(
                queue=self.rabbitmq_queue,
                on_message_callback=self.process_job,
                auto_ack=False
            )

            # start listening
            logging.info("Waiting for messages. To exit press CTRL+C")
            worker_running.set()  # mark healthy

            try:
                # pylint: disable=protected-access
                while self.channel and self.channel.is_open and self.channel._consumer_infos:
                    self.channel.connection.process_data_events(time_limit=1)  # 1 second
            except SystemExit:
                raise
            except KeyboardInterrupt:
                raise
            except GeneratorExit:
                raise
            except Exception:  # pylint: disable=broad-except
                logging.error("Worker crashed/disconnected:\n%s", traceback.format_exc())
            finally:
                logging.info("Stopped listening for messages.")
                self.close()
                self.connection = None

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
