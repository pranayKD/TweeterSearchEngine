from es_query_generator import QueryGenerator
from config import ES_index_name
import json

class TwitterQueries:
    """This class uses query generator class for creating queries for twitter search engine
    """

    def __init__(self, db_client):
        """constructor

        Args:
            db_client (Elasticsearch Object): Client for search database
        """
        self.filter = 'filter'
        self.must = 'must'
        self.term = 'term'
        self.match = 'match'
        self.nested_term = 'nested-term'
        self.nested_match = 'nested-match'
        self.date_format = "dd/MM/yyyy HH:mm:ss Z"

        self.db_client = db_client


    def search(self, query):
        """Search query in database

        Args:
            query (dict): query to run

        Returns:
            dict: Result of the query
        """
        response = self.db_client.search(index = ES_index_name, body = query)
        return response

    
    def generate_search_query(self,request_body):
        """Given request body, generate search query for elastic

        Args:
            request_body (dict): Request Body

        Returns:
            dict: Elastic search query
        """
        request_body_keys = request_body.keys()

        hashtag = None
        user_mentioned = None
        by_user = None 
        tweet_text = None 
        tweet_source = None 
        is_original = None 
        created_at = None
        sort = None
        pagination=None
        
        if 'hashtag' in request_body_keys:
            hashtag = request_body['hashtag']
        
        if 'user_mentioned' in request_body_keys:
            user_mentioned = request_body['user_mentioned']

        if 'by_user' in request_body_keys:
            by_user = request_body['by_user']

        if 'tweet_text' in request_body_keys:
            tweet_text = request_body['tweet_text']

        if 'tweet_source' in request_body_keys:
            tweet_source = request_body['tweet_source']

        if 'is_original' in request_body_keys:
            is_original = request_body['is_original']

        if 'created_at' in request_body_keys:
            created_at = request_body['created_at']

        if 'sort' in request_body_keys:
            sort = request_body['sort']

        if 'from' in request_body_keys and 'size' in request_body_keys:
            pagination = []
            pagination.append(request_body['from'])
            pagination.append(request_body['size'])

        return self.__generate_search_query(hashtag, user_mentioned, by_user, tweet_text,
                                            tweet_source, is_original, created_at, sort, 
                                            pagination)
     

    # sort input argument example - "f1:asc, f2:desc, f3:asc"
    # date input arugment example - "gt:d1, lt:d2"
    def __generate_search_query(self, hashtags=None, user_mentioned=None, by_user=None, tweet_text=None, 
                            tweet_source=None, is_original=None, created_at=None, sort=None,
                            pagination=None):
        """Given input parametrs for each search field, generate search query

        Args:
            hashtags (String, optional): hashtag search criteria. Defaults to None.
            user_mentioned (String, optional): user mentioned search criterai. Defaults to None.
            by_user (String, optional): Who posted the tweet. Defaults to None.
            tweet_text (String, optional): Tweet text. Defaults to None.
            tweet_source (String, optional): Which device is used to post the tweets. Defaults to None.
            is_original (bool, optional): Is the tweet original or reply to any other tweet. Defaults to None.
            created_at (String, optional): Date of tweet creation. Defaults to None.
            sort (String, optional): Sort parameters. Defaults to None.
            pagination (list, optional): from and to values. Defaults to None.

        Raises:
            Exception: At least one input is needed between major search criteria
            Exception: At least one input criteria is required for date query

        Returns:
            dict: Combined query
        """

        if not hashtags and not user_mentioned and  not by_user and not tweet_text:
            raise Exception("At least one input param required between hashtag, user_mentioned, by_user and tweet_text ")

        
        query_generator = QueryGenerator()

        parent_query = query_generator.query['query']
        

        if hashtags is not None:
            hashtag_query = query_generator.convert_logical_operation_to_query(hashtags,'hashtags',self.term)
            query_generator.append_to_bool_query(parent_query, [hashtag_query], self.filter)

        if user_mentioned is not None:
            user_mentioned_query = query_generator.convert_logical_operation_to_query(user_mentioned, 'user_mentions.screen_name', self.nested_term)
            query_generator.append_to_bool_query(parent_query, [user_mentioned_query], self.filter)

        if by_user is not None:
            by_user_query = query_generator.convert_logical_operation_to_query(by_user, 'user_screen_name', self.term)
            query_generator.append_to_bool_query(parent_query, [by_user_query], self.filter)

        if tweet_text is not None:
            tweet_text_query = query_generator.convert_logical_operation_to_query(tweet_text, 'tweet_text', self.match)
            query_generator.append_to_bool_query(parent_query,[tweet_text_query], self.must)

        if tweet_source is not None:
            tweet_source_query = query_generator.convert_logical_operation_to_query(tweet_source, 'source', self.term)
            query_generator.append_to_bool_query(parent_query, [tweet_source_query], self.filter)

        if is_original is not None:
            is_original_query = query_generator.create_exist_query(not is_original, 'parent_tweet_id')
            query_generator.append_to_bool_query(parent_query, [is_original_query], self.filter)

        if created_at is not None:
            date_inputs = created_at.split(',')

            gt = None
            lt = None
            gte = None
            lte = None 

            for inp in date_inputs:
                inp = inp.strip()
                comparator, argument = inp.split('|')

                if comparator == 'gt':
                    gt = argument 
                elif comparator == 'lt':
                    lt = argument
                elif comparator == 'gte':
                    gte = argument
                elif comparator == 'lte':
                    lte = argument 
                else:
                    raise Exception ("Invalid argument received in created_at query")

            created_at_query = query_generator.create_range_query('created_at',gt, gte, lt, lte, self.date_format)
            query_generator.append_to_bool_query(parent_query, [created_at_query], self.filter)




        if sort is not None:
            fields = []
            orders = []

            field_order_list = sort.split(',')
            for field_order in field_order_list:
                field, order = field_order.split(':')
                fields.append(field.strip())
                orders.append(order.strip())

            query_generator.append_sort_query(fields, orders)

        if pagination is not None:
            query_generator.append_pagination(pagination[0], pagination[1])
            
                        
        return query_generator.query


    def generate_agg_query(self, request_body, search_query):
        """generate aggreagation query

        Args:
            request_body (dict): request body
            search_query (dict): search query to aggregate on

        Returns:
            dict: Combined query
        """
        request_body_keys = request_body.keys()

        hashtag_agg = None
        user_mentioned_agg=None
        tweet_source_agg=None
        is_original_agg = None
        created_at_agg = None
        retweet_count_agg=None
        favorite_count_agg=None

        if 'hashtag_agg' in request_body_keys:
            hashtag_agg = request_body['hashtag_agg']

        if 'user_mentioned_agg' in request_body_keys:
            user_mentioned_agg = request_body['user_mentioned_agg']

        if 'tweet_source_agg' in request_body_keys:
            tweet_source_agg = request_body['tweet_source_agg']

        if 'is_original_agg' in request_body_keys:
            is_original_agg = request_body['is_original_agg']

        if 'created_at_agg' in request_body_keys:
            created_at_agg = request_body['created_at_agg']

        if 'retweet_count_agg' in request_body_keys:
            retweet_count_agg = request_body['retweet_count_agg']

        if 'favorite_count_agg' in request_body_keys:
            favorite_count_agg = request_body['favorite_count_agg']

        agg_query = self.__generate_agg_query(hashtag_agg, user_mentioned_agg, tweet_source_agg, is_original_agg,
                                                created_at_agg, retweet_count_agg, favorite_count_agg)
        
        if len(agg_query) != 0:
            search_query['aggs'] = agg_query

        return search_query


    def __generate_agg_query(self, hashtag_agg = None, user_mentioned_agg=None, tweet_source_agg=None, 
                            is_original_agg = None, created_at_agg = None, retweet_count_agg=None, 
                            favorite_count_agg=None):
        """Helper function to generate aggregate query

        Args:
            hashtag_agg (dict, optional): hashtag agg arguments. Defaults to None.
            user_mentioned_agg (dict, optional): user mentioned agg arguments. Defaults to None.
            tweet_source_agg (dict, optional): tweet source agg arguments. Defaults to None.
            is_original_agg (dict, optional): is_original agg arguments. Defaults to None.
            created_at_agg (dict, optional): created_at agg arguments. Defaults to None.
            retweet_count_agg (dict, optional): retweet count agg arguments. Defaults to None.
            favorite_count_agg (dict, optional): favourite count agg arguments. Defaults to None.

        Returns:
            dict: combined agg query
        """
        query_generator = QueryGenerator()
        agg_queries = []

        if hashtag_agg is None:
            hashtag_agg = {
                "terms":{
                    "size":15
                }
            }

        if user_mentioned_agg is None:
            user_mentioned_agg = {
                "terms" : {
                    "nested" : "screen_name",
                    "size": 15
                }
            }

        if tweet_source_agg is None:
            tweet_source_agg = {
                "terms": {

                }
            }

        if is_original_agg is None:
            is_original_agg = {
                "missing": {}
            }

        if created_at_agg is None: 
            created_at_agg = {
                "date_histogram": {
                    "interval": "day",
                    
                }
            }


        if retweet_count_agg is None:
            retweet_count_agg = {
                "min":{},
                "max" : {}, 
                "avg" : {}, 
                "histogram": {
                    "interval" : 10,
                    "min_doc_count" : 1
                }
            }

        if favorite_count_agg is None:
            favorite_count_agg = {
                "min":{},
                "max" : {}, 
                "avg" : {}, 
                "histogram": {
                    "interval" : 10,
                    "min_doc_count" : 1
                }
            }

        self.__generate_agg_query_single('hashtags', hashtag_agg, query_generator, agg_queries)
        self.__generate_agg_query_single('user_mentions', user_mentioned_agg, query_generator,agg_queries)
        self.__generate_agg_query_single('source', tweet_source_agg, query_generator,agg_queries)
        self.__generate_agg_query_single('parent_tweet_id', is_original_agg, query_generator,agg_queries)
        self.__generate_agg_query_single('created_at', created_at_agg, query_generator,agg_queries)
        self.__generate_agg_query_single('retweet_count', retweet_count_agg, query_generator,agg_queries)
        self.__generate_agg_query_single('favorite_count', favorite_count_agg, query_generator,agg_queries)

        return query_generator.combine_aggregations(agg_queries)

        
    def __generate_agg_query_single(self, field, field_agg_parameters, query_generator, agg_queries):
        """For a single field, multiple aggregation criterias can be provided. This function does agg for a single field

        Args:
            field (String): Field name to aggregate on
            field_agg_parameters (dict): field aggreagation input parametrs
            query_generator (QueryGenerator object): query generator object
            agg_queries (List): All the agg queries are added in this list
        """

        for field_agg_parameters_item in field_agg_parameters.items():
            agg_clause = field_agg_parameters_item[0]
            agg_params = field_agg_parameters_item[1]
            agg_name = field + '_' + agg_clause

            agg_queries.append(query_generator.create_aggregation_basic(agg_name, agg_clause, field, agg_params))


    def generate_highlight_query(self, request_body):
        request_body_keys = request_body.keys()

        tweet_text = None 
        tweet_id = None

        if 'tweet_text' in request_body_keys:
            tweet_text = request_body['tweet_text']

        if 'tweet_id' in request_body_keys:
            tweet_id = request_body['tweet_id']

        return self.__generate_highlight_query(tweet_text, tweet_id)

        
    def __generate_highlight_query(self, tweet_text, tweet_id):
        if tweet_text == None or tweet_id == None:
            raise Exception("both tweet_id and tweet_text should be provided for getting highlights")

        query_generator = QueryGenerator()
        parent_query = query_generator.query['query']
        
        tweet_text_query = query_generator.convert_logical_operation_to_query(tweet_text, 'tweet_text', self.match)
        query_generator.append_to_bool_query(parent_query,[tweet_text_query], self.must)

        tweet_id_query = query_generator.convert_logical_operation_to_query(tweet_id,'_id',self.term)
        query_generator.append_to_bool_query(parent_query, [tweet_id_query], self.filter)

        query_generator.add_highlighter(query_generator.query)

        print(query_generator.query)

        return query_generator.query
        


    def generate_suggestions_query(self, field, value):
        if field == None or value == None:
            raise Exception('Both field and value should be provided for getting suggestions')

        query_generator = QueryGenerator()
        query_generator.add_auto_complete_suggestor( query_generator.query, field, value)

        return query_generator.query

        


        