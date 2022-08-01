import sys,os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir))

import MQConsumer
import elastic_client
import config
from es_model_tweet import tweet_mapping


if __name__=='__main__':
    print("starting Listener queue for inserting in rabbitmq")
    es_client = elastic_client.ElasticClient()
    es_client.create_index_if_not_present(config.ES_index_name, tweet_mapping)

    queue_consumer = MQConsumer.MQConsumer(config.MQ_host, config.MQ_queue, es_client)

    queue_consumer.start_consuming()