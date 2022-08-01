from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import copy
import logging
import config

from logger.queue_logger import LogCreator


LOGGER_NAME = "INDEXING_LOG"
LOGGER_LEVEL = logging.DEBUG
logger = LogCreator(LOGGER_NAME,LOGGER_LEVEL).logger



class ElasticClient:
    """Client to interact with elastic DB. This is actually not needed since elasticsearch itself is official python client for ES
    """

    def __init__(self):
        self.es = Elasticsearch(config.ES_host)

    def create_index_if_not_present(self, index_name, mappings):
        """Create index if not present

        Args:
            index_name (String): Name of index
            mappings (Json): Mapping for index
        """
        self.es.indices.create(index=index_name, body=mappings, ignore=400)
        

    def bulk_insert(self, data):
        """Insert data in ES in bulk

        Args:
            data (list): List of tweets
        """
        start_time = time.time()
        data_copy = copy.deepcopy(data)

        actions = [
            {
                '_op_type':'create',
                '_index': data_copy[i].pop('_index'),
                '_id': data_copy[i].pop('_id'),
                '_source': data_copy[i]
            }
            for i in range(len(data_copy))
        ]

        helpers.bulk(self.es, actions)

        end_time = time.time()
        process_time = end_time - start_time
        tweet_id = [tweet['_id'] for tweet in data]

        logger.info('Insert Tweets in ES', extra={'message_code':'ite',  'completion_time': process_time, 'data':tweet_id})




    
