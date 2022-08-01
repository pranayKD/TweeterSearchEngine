import requests
import json
from config import ES_index_name
import re
import logging
import time
from logger.queue_logger import LogCreator


LOGGER_NAME = "INDEXING_LOG"
LOGGER_LEVEL = logging.DEBUG
logger = LogCreator(LOGGER_NAME,LOGGER_LEVEL).logger


class APIHelper:
    """For a given API url, argument list and auth token, return filtered response in json format
    """

    def __init__(self, request_url,  auth_token, args=[]):
        """Constructor

        Args:
            request_url (String): Twitter request URL
            auth_token (String): Auth Token
            args (list, optional): Input Arguments for the given request URL. Defaults to [].
        """
        self.request_url = request_url
        self.args = args
        self.header = {'Authorization': auth_token}


    def __create_request(self):
        """Replace the arguments with provided args in request url

        Returns:
            String: Updated request URL
        """
        return self.request_url.format(*self.args)

    
    def __get_tweet_ids(self, response):
        """For the response, filter only tweet IDs

        Args:
            response (dict): Response

        Returns:
            list: Tweet IDs list
        """
        len_response = len(response["statuses"])
        tweet_ids = []

        for row in range(len_response):
            response_row = response["statuses"][row]
            tweet_ids.append(response_row["id_str"])

        return tweet_ids

    
    def __call_api(self, request):
        """Makes a call to twitter API and returns response

        Args:
            request (string): Request URL

        Raises:
            Exception: If rest api call failed

        Returns:
            dict: response
        """
        start_time = time.time()
        response = requests.get(request, headers=self.header)
        end_time = time.time()
        process_time = end_time-start_time

        if response.status_code != 200:
            logger.exception(('Call API to Fetch Tweets'),extra={'message_code':'caft', 'a_1':request, 'a_2':response.status_code, 'data':response.text ,'completion_time':process_time})
            raise Exception(("Request {} failed with error code {} and message {}").format(url, response.status_code, response.text))

        tweet_ids = self.__get_tweet_ids(json.loads(response.text))
        logger.info(('Call API to Fetch Tweets'),extra={'message_code':'caft', 'a_1':request, 'a_2':response.status_code, 'data':tweet_ids, 'completion_time':process_time})
        
        return json.loads(response.text)

    def __remove_html_tags(self, text):
        """Remove html tags and only give the text

        Args:
            text (String): data with html tags

        Returns:
            String: data without html tags
        """
        # Get all html tags and replace them with empty strings
        html_tags = re.compile('<.*?>')
        text = re.sub(html_tags, '', text)

        return text

    
    def __filter_response(self, response):
        """Filter tweet response to only keep required attributes

        Args:
            response (dict): response body

        Returns:
            dict: Filtered response body
        """
        len_response = len(response["statuses"])
        filtered_response = []

        for row in range(len_response):
            response_row = response["statuses"][row]
            filtered_response_row = {}

            # id and index for corresponding search db

            filtered_response_row["_index"] = ES_index_name
            filtered_response_row["_id"] = response_row["id_str"]

            # tweet details 

            filtered_response_row["tweet_text"] = response_row["full_text"] 
            filtered_response_row["created_at"] = response_row["created_at"] 

            hashtag_list = response_row["entities"]["hashtags"]
            filtered_response_row["hashtags"] = [hashtag_list[hashtag_num]["text"] for hashtag_num in range(len(hashtag_list))]

            users_mentioned_list = response_row["entities"]["user_mentions"]
            filtered_response_row["user_mentions"] = [{"screen_name" : users_mentioned_list[user_num]["screen_name"],"user_name": users_mentioned_list[user_num]["name"]} for user_num in range(len(users_mentioned_list))]


            filtered_response_row["source"] = self.__remove_html_tags(response_row["source"])            
            filtered_response_row["parent_tweet_id"] = response_row["in_reply_to_status_id_str"]
            filtered_response_row["parent_user_id"] = response_row["in_reply_to_user_id_str"]
            filtered_response_row["retweet_count"] = response_row["retweet_count"]
            filtered_response_row["favorite_count"] = response_row["favorite_count"]

            # User details

            filtered_response_row["user_id"] = response_row["user"]["id_str"]
            filtered_response_row["user_name"] = response_row["user"]["name"]
            filtered_response_row["user_screen_name"] = response_row["user"]["screen_name"]
            filtered_response_row["user_description"] = response_row["user"]["description"]
            filtered_response_row["user_created_at"] = response_row["user"]["created_at"]
            filtered_response_row["user_verified"] = response_row["user"]["verified"]

            filtered_response.append(filtered_response_row)

        return filtered_response

    
    def __get_metadata(self, response):
        """Fetch response metdata

        Args:
            response (dict): response body

        Returns:
            dict : response metadata
        """
        response["search_metadata"]["actual_count"] = len(response["statuses"])
        return response["search_metadata"]


    def rest_call(self):
        """Makes a call to rest api, filtes the response and returns response

        Returns:
            tuple: response dict and metadata dict
        """
        request  = self.__create_request()
        response = self.__call_api(request)
        metadata = self.__get_metadata(response)
        response = self.__filter_response(response)

        return response, metadata

    
    def set_args(self, new_args):
        self.args = new_args




class Scraper:
    """For given topics, makes a call to API, inserts in mongo and inserts in MQ
    """

    def __init__(self, apiHelper, search_topics, num_tweets_to_fetch, max_tweets_per_call,  db_client):
        """Constructor

        Args:
            apiHelper (APIHelper): Object of API helper class
            search_topics (list): List of search topics
            num_tweets_to_fetch (integer): Total number of tweets to fetch per topic
            max_tweets_per_call (integer): In a single call to api, how many tweets to fetch, max is 100
            db_client (DBClient): object of database client
        """
        self.apiHelper = apiHelper
        self.search_topics = search_topics
        self.num_tweets = num_tweets_to_fetch
        self.max_tweets = max_tweets_per_call
        self.db_client = db_client


    def __search_topic_and_insert(self, topic):
        """Make api calls for given topic and insert in mongo and inform queue

        Args:
            topic (String): Topic to search
        """

        max_id = '-1'

        for num_call in range(self.num_tweets/self.max_tweets):

            self.apiHelper.set_args([topic + " -RT", str(self.max_tweets), max_id])
            response, metadata = self.apiHelper.rest_call()

            db_response = self.db_client.insert_and_inform_queue(response)   
        
            max_id = metadata["next_results"].split("&")[0].split("=")[1]

            print(("topic {} - Inserting {} - Actual Inserted {}").format(topic, metadata["actual_count"], db_response.upserted_count))



    def search_topics_and_insert_all(self):
        """From the list of topics, search and insert tweets for all topics
        """

        for topic in self.search_topics:
            start_time = time.time()
            logger.info(("Start Fetching of Tweets"),extra={'message_code':'sft', 'data':topic})
            
            self.__search_topic_and_insert(topic)
            end_time = time.time()
            process_time = end_time-start_time
            logger.info(("End Fetching of Tweets"),extra={'message_code':'eft', 'data':topic, 'completion_time':process_time})










    






