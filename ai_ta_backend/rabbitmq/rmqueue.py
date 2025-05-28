import os
import pika
import logging
import uuid
import json

try:
    from ai_ta_backend.rabbitmq.sql import SQLAlchemyIngestDB
except ModuleNotFoundError:
    from sql import SQLAlchemyIngestDB


# TODO: Move into the class?
sql_session = SQLAlchemyIngestDB()

class Queue:

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

    def addJobToIngestQueue(self, inputs, queue_name=None):
        """
        This adds a job to the queue, then eventually the queue worker uses ingest.py to ingest the document.
        """
        logging.info(f"Queueing ingest task for {inputs['course_name']}")
        logging.info(f"Inputs: {inputs}")

        if not self.is_connected():
            logging.error("RabbitMQ is offline")

        job_id = str(uuid.uuid4())
        message = {
            'job_id': job_id,
            'status': 'queued',
            'inputs': inputs
        }

        # SQL record first
        doc_progress_payload = {
            "s3_path": inputs['s3_paths'][0] if 's3_paths' in inputs else '',
            "readable_filename": inputs['readable_filename'],
            "course_name": inputs['course_name'],
            "beam_task_id": job_id,  # TODO: beam name is deprecated
        }
        print("doc_progress_payload: ", doc_progress_payload)
        #sql_session.insert_document_in_progress(doc_progress_payload)

        # RMQ message second
        self.channel.basic_publish(
            exchange='',
            routing_key=self.rabbitmq_queue if queue_name is None else queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        logging.info(f"Job {job_id} enqueued")

        return job_id
