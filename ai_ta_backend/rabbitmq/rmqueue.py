import os
import pika
import logging
import uuid
import json
import ssl

try:
    from ai_ta_backend.rabbitmq.rmsql import SQLAlchemyIngestDB
    from ai_ta_backend.rabbitmq.connection_resolver import WorkerConnectionResolver
    import ai_ta_backend.rabbitmq.models as models
except ModuleNotFoundError:
    from rmsql import SQLAlchemyIngestDB
    from connection_resolver import WorkerConnectionResolver
    import models

logging.getLogger('pika').setLevel(logging.WARNING)


# TODO: Move into the class?
sql_session = SQLAlchemyIngestDB()
# Resolves a project's external documents DB so the in-progress status row lands
# in the same DB the worker and frontend read from. Reuses the host session for
# the (host-only) project_external_connections lookup.
resolver = WorkerConnectionResolver(sql_session)

class Queue:

    def __init__(self):
        self.rabbitmq_queue = os.getenv('RABBITMQ_QUEUE', 'uiuc-chat')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672')
        self.rabbitmq_ssl = os.getenv('RABBITMQ_SSL', False)
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

    def addJobToIngestQueue(self, inputs, queue_name=None):
        """
        This adds a job to the queue, then eventually the queue worker uses ingest.py to ingest the document.
        """
        logging.info(f"Queueing ingest task for {inputs['course_name']}")
        logging.info(f"Inputs: {inputs}")

        if not self.is_connected():
            logging.error("RabbitMQ is offline")

        # SQL record first
        # Handle s3_paths: convert string to list, or get first element from list
        s3_path_value = ''
        if 's3_paths' in inputs:
            s3_paths = inputs['s3_paths']
            if isinstance(s3_paths, str):
                s3_path_value = s3_paths
            elif isinstance(s3_paths, list) and len(s3_paths) > 0:
                s3_path_value = s3_paths[0]
        doc_progress_payload = models.DocumentsInProgress(
            s3_path=s3_path_value,
            readable_filename=inputs['readable_filename'],
            course_name=inputs['course_name']
        )
        # Route the status row to the project's documents DB (external when the
        # project has database_config, else host) so the worker can fetch it by
        # job_id and the frontend's "documents in progress" view finds it.
        resolved = resolver.resolve(inputs.get('course_name', ''))
        status_db = (
            SQLAlchemyIngestDB(engine=resolved.documents_sql_engine)
            if resolved.documents_sql_engine else sql_session
        )
        new_doc = status_db.insert_document_in_progress(doc_progress_payload)
        logging.info("Inserted new in-progress job ID: " + new_doc.get("beam_task_id"))
        new_job_id = new_doc.get("beam_task_id")

        # RMQ message second
        message = {
            'job_id': new_job_id,
            'status': 'queued',
            'inputs': inputs
        }
        self.channel.basic_publish(
            exchange='',
            routing_key=self.rabbitmq_queue if queue_name is None else queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        logging.info(f"Job {new_job_id} enqueued")
        return new_job_id
