"""
    Used for consuming logs from Messaging Queue and Inserting the logs into ES
"""

import sys,os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir))

import pika
import config
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(config.ES_host)


class QueueConsumer:
    """
        Class used for consuming log messages from queue and inserting in ES index
    """

    def __init__(self, host, queue):
        """
           Constructor for creating connection with MQ

        Args:
            host (String): MQ host
            queue (String): MQ to connect to
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)

        self.queue = queue

    
    def __callback(self, ch, method, properties, body):
        """
            Call back for the rabbitMQ

        Args:
            body (string): received from queue
        """
        body = json.loads(body)
        es.index(index=body["process_name"].lower(), body=body)        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
    def start_consuming(self):
        """
            RabbitMQ consumer worked starts consuming data from queue
        """
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.__callback)
        self.channel.start_consuming()


if __name__ == '__main__' :

   logging_queue_consumer = QueueConsumer(config.logger_host, config.logger_queue)
   logging_queue_consumer.start_consuming()

    