import json
from error_message import ERROR

class Response:

    def __init__(self, status, message, content):
        self.status = status
        self.message = message 
        self.content = content

    @staticmethod
    def invalid_request_method():
        return Response(400, ERROR['INVALID_REQUEST_METHOD'], {})

    @staticmethod
    def processing_error(message):
        return Response(500, message, {})

    def format(self):
        response = {}
        response['status'] = self.status
        response['message'] = self.message

        result = {}
        result = self.content

        to_return = {
            'response':response,
            'result': result
        }

        return json.dumps(to_return)

    def format_es_search_query_response(self):
        content = {}

        content['count'] = self.content['hits']['total']['value']
        content['count_relation'] = self.content['hits']['total']['relation']
        
        response_array = []
        for response_item in self.content['hits']['hits']:
            response_item['_source']['tweet_id'] = response_item['_id']
            response_array.append((response_item['_source'], response_item['_score']))

        content['docs'] = response_array

        self.content = content


    def format_es_highlight_query_response(self):
        content = {}

        content['count'] = self.content['hits']['total']['value']
        content['count_relation'] = self.content['hits']['total']['relation']
        
        response_array = []
        for response_item in self.content['hits']['hits']:
            response_array.append((response_item['highlight'], response_item['_score']))

        content['docs'] = response_array

        self.content = content

    def format_es_agg_query_response(self):
        content = {}
        content['aggregations'] = self.content['aggregations']

        self.content = content

    def format_es_suggestion_query_response(self):
        content = {}
        response_array = []
        for response_item in self.content['suggest']['completion_suggest'][0]['options']:
            response_array.append(response_item['text'])
        content['suggestions'] = response_array
        self.content= content
        
