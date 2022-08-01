"""
    Contains classes used for creating new Logger, handlers and formatters.
    Logger classs emits data into the MQ
"""

import logging
import config
import pika
import json
import datetime, pytz
from elasticsearch import Elasticsearch
from logger.es_models import indexing_model, api_model


TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f %z'

es = Elasticsearch(config.ES_host)

class QueueLoggingHandler(logging.Handler):
    """
        Handler for Logging
    """
    
    def __init__(self, host, queue, exchange, routing_key):
        """Constructor

        Args:
            host (String): MQ host to connect to
            queue (String): MQ channel to connect to
            exchange (String): MQ exchange
            routing_key (String): MQ Routing Key
        """
        super(QueueLoggingHandler, self).__init__()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue, durable=True)

        self.exchange = exchange
        self.routing_key = routing_key

        """
            Creating indices in ES as soon as the logging handler is created
        """

        es.indices.create(index="api_log", body=api_model.api_log_mapping, ignore=400)
        es.indices.create(index="indexing_log", body=indexing_model.indexing_log_mapping, ignore=400)
        


    def emit(self, record):
        """Sends log data into MQ

        Args:
            record (LogRecord): Record to log
        """
        try:
            data = self.format(record)
            self.channel.basic_publish(
                exchange = self.exchange, 
                routing_key = self.routing_key,
                body = data,
                properties=pika.BasicProperties(
                    delivery_mode = 2, # make message persistent
                )
            )
        except:
            pass


class IndexingLogFormatter(logging.Formatter):
    """
        Formatter used in case of Indexing Tweets
    """

    def __init__(self):
        super(IndexingLogFormatter, self).__init__()
        
    
    def format(self, record):
        """Format LogRecord object received while indexing tweets

        Args:
            record (LogRecord): LogRecord Object recevied

        Returns:
            String: Formatted Json String
        """
        data = {}
        data['start_time'] = datetime.datetime.now(pytz.utc).strftime(TIME_FORMAT)
        data['log_level'] = getattr(record, 'levelname', '')
        data['file_name'] = getattr(record, 'filename', '')
        data['function_name'] = getattr(record, 'funcName', '')
        data['line_number'] = getattr(record, 'lineno', '')
        data['process_name'] = getattr(record, 'name', '')
        data['message'] = getattr(record, 'msg', '')
        data['message_code'] = getattr(record, 'message_code', '')
        data['completion_time'] = getattr(record, 'completion_time', '')
        data['a_1'] = getattr(record, 'a_1', '')
        data['a_2'] = getattr(record, 'a_2', '')
        data['data'] = getattr(record, 'data', '')

        return json.dumps(data)


class ApiLogFormatter(logging.Formatter):
    """Format LogRecord object received while API calls

    Args:
        logging (LogRecord): LogRecord Object
    """

    def __init__(self):
        super(ApiLogFormatter, self).__init__()

    def format(self, record):
        """Format LogRecord Object received while API calls

        Args:
            record (LogRecord): LogRecord Object

        Returns:
            String: Formatted Log
        """
        data = {}
        data['start_time'] = datetime.datetime.now(pytz.utc).strftime(TIME_FORMAT)
        data['log_level'] = getattr(record, 'levelname', '')
        data['file_name'] = getattr(record, 'filename', '')
        data['function_name'] = getattr(record, 'funcName', '')
        data['line_number'] = getattr(record, 'lineno', '')
        data['process_name'] = getattr(record, 'name', '')
        data['server_name'] = getattr(record, 'server_name', '')
        data['remote_addr'] = getattr(record, 'remote_addr', '')
        data['server_port'] = getattr(record, 'server_port', '')
        data['request_method'] = getattr(record, 'request_method', '')
        data['request_path'] = getattr(record, 'request_path', '')
        data['response_status'] = getattr(record, 'response_status', '')
        data['response_time'] = getattr(record, 'response_time', '')

        return json.dumps(data)

class LogCreator:
    """
        This class creates logger with appropriate handler and formatter
    """
    def __init__(self, logger_name, logger_level):
        """Create New LogCreator Class

        Args:
            logger_name (String): Logger to create , ex - INDEXING_LOG, API_LOG
            logger_level (level): Logger Level, ex - logging.INFO

        Raises:
            Exception: If the logger name is incorrect
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logger_level)

        # If handler is already present for the logger, no processing required
        if self.logger.handlers:
            return

        handler = QueueLoggingHandler(config.logger_host, config.logger_queue, 
                                        config.logger_exchange, config.logger_routing_key)
        formatter = None

        if logger_name == "INDEXING_LOG":
            formatter = IndexingLogFormatter()

        elif logger_name == "API_LOG":
            formatter = ApiLogFormatter()

        if formatter is None:
            raise Exception ("Incorrect value provided while creating new logger")

        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

