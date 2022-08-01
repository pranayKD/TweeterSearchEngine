import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


from django.http import HttpResponse
import json
from response_generator.response_generator import Response
from utility.utils import fetch_request_metadata
from query_generator.twitter_queries import TwitterQueries
from elasticsearch import Elasticsearch
import logging
import time
from logger.queue_logger import LogCreator
import config


LOGGER_NAME = "API_LOG"
LOGGER_LEVEL = logging.DEBUG
logger = LogCreator(LOGGER_NAME,LOGGER_LEVEL).logger


es_connection = Elasticsearch(config.ES_host)



def search_tweets(request):

    log_info = fetch_request_metadata(request)

    if (request.method != 'POST'):
        log_info['response_status'] = 400
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.invalid_request_method().format(), content_type='application/json', status=400)

    try : 
        start_time = time.time()
        request_body = json.loads(request.body)
        twitter_queries = TwitterQueries(es_connection)
        query = twitter_queries.generate_search_query(request_body)
        query_response = twitter_queries.search(query)
        
        # Not checking in the es response for possible failures here, this needs to be changed
        response = Response(200, "Execution Successful", query_response)
        response.format_es_search_query_response()

        end_time = time.time()
        process_time = end_time - start_time
        log_info['response_status'] = 200
        log_info['response_time'] = process_time
        logger.info('success', extra=log_info)

        return HttpResponse(response.format(), content_type='application/json')
    except Exception as err:
        log_info['response_status'] = 500
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.processing_error(str(err)).format(), content_type='application/json', status=500)


def get_query(request):

    log_info = fetch_request_metadata(request)
    
    if (request.method != 'POST'):
        log_info['response_status'] = 400
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.invalid_request_method().format(), content_type='application/json', status=400)
        
    try : 
        start_time = time.time()
        request_body = json.loads(request.body)
        twitter_queries = TwitterQueries(es_connection)
        query = twitter_queries.generate_search_query(request_body)
        query = twitter_queries.generate_agg_query(request_body, query)
        
        response = Response(200, "Execution Successful", query)

        end_time = time.time()
        process_time = end_time - start_time
        log_info['response_status'] = 200
        log_info['response_time'] = process_time
        logger.info('success', extra=log_info)
        return HttpResponse(response.format(), content_type='application/json')
    except Exception as err:
        log_info['response_status'] = 500
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.processing_error(str(err)).format(), content_type='application/json', status=500 )


def get_aggregations(request):

    log_info = fetch_request_metadata(request)

    if (request.method != 'POST'):
        log_info['response_status'] = 400
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.invalid_request_method().format(), content_type='application/json', status=400)

    try:
        start_time = time.time()
        request_body = json.loads(request.body)
        twitter_queries = TwitterQueries(es_connection)
        query = twitter_queries.generate_search_query(request_body)
        query = twitter_queries.generate_agg_query(request_body, query)

        query_response = twitter_queries.search(query)
        
        # Not checking in the es response for possible failures here, this needs to be changed
        response = Response(200, "Execution Successful", query_response)
        response.format_es_agg_query_response()

        end_time = time.time()
        process_time = end_time - start_time
        log_info['response_status'] = 200
        log_info['response_time'] = process_time
        logger.info('success', extra=log_info)
        return HttpResponse(response.format(), content_type='application/json')
    except Exception as err:
        log_info['response_status'] = 500
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.processing_error(str(err)).format(), content_type='application/json', status=500 )
    

def get_highlights(request):
    log_info = fetch_request_metadata(request)

    if (request.method != 'POST'):
        log_info['response_status'] = 400
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.invalid_request_method().format(), content_type='application/json', status=400)

    try:
        start_time = time.time()
        request_body = json.loads(request.body)
        twitter_queries = TwitterQueries(es_connection)
        query = twitter_queries.generate_highlight_query(request_body)

        query_response = twitter_queries.search(query)
        response = Response(200, "Execution Successful", query_response)
        response.format_es_highlight_query_response()

        end_time = time.time()
        process_time = end_time - start_time
        log_info['response_status'] = 200
        log_info['response_time'] = process_time
        logger.info('success', extra=log_info)

        return HttpResponse(response.format(), content_type='application/json')
    except Exception as err:
        log_info['response_status'] = 500
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.processing_error(str(err)).format(), content_type='application/json', status=500 )


def get_suggestions(request):
    log_info = fetch_request_metadata(request)

    if (request.method != 'GET'):
        log_info['response_status'] = 400
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.invalid_request_method().format(), content_type='application/json', status=400)

    
    try:
        start_time = time.time()
        
        field = request.GET.get('field', None)
        value = request.GET.get('value', None)
        
        twitter_queries = TwitterQueries(es_connection)
        query = twitter_queries.generate_suggestions_query(field, value)
        query_response = twitter_queries.search(query)
        response = Response(200, "Execution Successful", query_response)
        response.format_es_suggestion_query_response()

        end_time = time.time()
        process_time = end_time - start_time
        log_info['response_status'] = 200
        log_info['response_time'] = process_time
        logger.info('success', extra=log_info)

        return HttpResponse(response.format(), content_type='application/json')
    except Exception as err:
        log_info['response_status'] = 500
        logger.exception('error', extra=log_info)
        return HttpResponse(Response.processing_error(str(err)).format(), content_type='application/json', status=500 )