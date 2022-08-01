import pika
import json
import time

class MQProducer:
    """Producer class to put tweets in messaging queue once the insertion in mongo is successful
    """

    def __init__(self, host, queue, exchange, routing_key):
        """constructor

        Args:
            host (String): MQ host
            queue (String): MQ queue
            exchange (String): MQ exchange
            routing_key (String): MQ routing key
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue, durable=True)

        self.exchange = exchange
        self.routing_key = routing_key

    
    def __prepare_payload(self,data,ids_to_publish):
        """Filter tweets before inserting in queue. Only tweets which are successfully put in mongo are inserted in queue
            Also, put the timestamps at which tweets are inserted. This is later used to figure out how much time tweet stayed in queue

        Args:
            data (list): List of Tweets
            ids_to_publish (list): List of tweet IDs which were inserted in Mongo

        Returns:
            list: updated list of tweets
        """
        data_to_pass = [data[idx] for idx in ids_to_publish]
        data_to_pass.append(time.time())
        return data_to_pass


    def publish(self,data, ids_to_publish):
        """Publish tweet data to MQ

        Args:
            data (list): List of tweets
            ids_to_publish (list): Tweet IDs which are successfully inserted in mongo
        """
        data = self.__prepare_payload(data, ids_to_publish)
        self.channel.basic_publish(
            exchange = self.exchange, 
            routing_key = self.routing_key,
            body = json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
            )
        )


    def close_connection(self):
        """Close the connection
        """
        self.connection.close()






