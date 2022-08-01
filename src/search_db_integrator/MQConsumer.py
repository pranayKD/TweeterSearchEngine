import pika
import json
import logging
import time
from logger.queue_logger import LogCreator


LOGGER_NAME = "INDEXING_LOG"
LOGGER_LEVEL = logging.DEBUG
logger = LogCreator(LOGGER_NAME,LOGGER_LEVEL).logger


class MQConsumer:
    """
        Consumer class for reading tweets out of messaging queue and inserting in elastic DB
    """

    def __init__(self, host, queue, db_client):
        """constructor

        Args:
            host (String): MQ host
            queue (String): MQ queue
            db_client (DB Client object): database client for elastic
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)

        self.queue = queue
        self.db_client = db_client

    

    def __callback(self, ch, method, properties, body):
        """reads tweets from queue and inserts in elastic

        Args:
            body (String): String received from queue
        """
        print("inserting")
        body = json.loads(body)
        start_time = body.pop()
        
        end_time = time.time()
        process_time = end_time- start_time
        tweet_ids = [tweet['_id'] for tweet in body]
        logger.info('Receive Tweets from Queue', extra={'message_code':'rtq', 'completion_time': process_time, 'data':tweet_ids})

        self.db_client.bulk_insert(body)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    
    def start_consuming(self):
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.__callback)
        self.channel.start_consuming()