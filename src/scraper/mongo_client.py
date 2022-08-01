import pymongo
from pymongo import UpdateOne
import copy
import time
import logging

from mongo_model_tweet import Tweet
from logger.queue_logger import LogCreator


LOGGER_NAME = "INDEXING_LOG"
LOGGER_LEVEL = logging.DEBUG
logger = LogCreator(LOGGER_NAME,LOGGER_LEVEL).logger

class MongoClient:
    """
        DB client to interact with mongo db
    """

    def __init__(self, host_name, db_name, collection_name, queue_producer):
        """Constructor

        Args:
            host_name (String): Mongo Host name
            db_name (String): Mongo DB name
            collection_name (String): Mongo Collection name
            queue_producer (Messaging Queue Producer object): MQ producer object
        """
        self.mongo =  pymongo.MongoClient(host_name)[db_name][collection_name]
        self.queue_producer = queue_producer

    
    def __prepare_for_insert(self, data):
        """Prepare input data for insertion in mongo DB

        Args:
            data (list): List of dict, each dict is a tweet

        Returns:
            list: List of operations that can be used for mongo bulk insert
        """
        data_copy = copy.deepcopy(data)
        ids = [row.pop("_id") for row in data_copy]

        '''
            Validate the schema of all the docs before inserting in mongo
        '''
        for row in data:
            Tweet(**row).validate()

        '''
            MongoEngine documenation does not have bulk upsert (set on insert) mentioned. So using pyMongo for insert
            Document schema validation is done manually before inserting
            Set on Insert - inserts rows only if not present. For rows already present, no change
        '''
        operations=[UpdateOne({"_id":idn},{'$setOnInsert':row},upsert=True) for idn ,row in zip(ids,data_copy)]
        return operations


    def insert_and_inform_queue(self, data):
        start_time = time.time()

        operations = self.__prepare_for_insert(data)

        """
            Result of bulk write has parameter - upserted_ids -> this return the IDs that are inserted into table
        """

        bulk_write_response =  self.mongo.bulk_write(operations)

        end_time = time.time()
        process_time = end_time-start_time
        logger.info('Insert Tweets in Mongo', extra={'message_code':'itm', 'completion_time': process_time, 'data':bulk_write_response.upserted_ids.values()})

        start_time = time.time()
                
        self.queue_producer.publish(data, bulk_write_response.upserted_ids)

        end_time = time.time()
        process_time = end_time-start_time
        logger.info('Send Tweets to Queue', extra={'message_code':'stq', 'completion_time': process_time, 'data':bulk_write_response.upserted_ids.values()})

        return bulk_write_response