# Config for Scraper
import os

auth_token = "Bearer AAAAAAAAAAAAAAAAAAAAALrwNQEAAAAAT30X%2FuLN%2BU5s8qADGnRYRA4QaCM%3DCZgChrtUQxVFUagH8joPCNzQLYeqb2VZZ0UZDqBFL7JkquEIdE"

search_url = "https://api.twitter.com/1.1/search/tweets.json?q={}&count={}&max_id={}&lang=en&tweet_mode=extended&result_type=recent"

num_tweets = 1000

max_tweers = 100



# Config for Mongo DB

mongo_localhost = os.environ.get('MONGO_LOCALHOST', 'localhost')

mongo_host_name =  "mongodb://" + mongo_localhost +  ":27017/"

# mongo_host_name = "mongodb://localhost:27017/"

mongo_db_name = "twitter"

mongo_col_name = "tweet"



# Config for Rabbit MQ -  Scraping Tweets

MQ_host = os.environ.get('RABBITMQ_LOCALHOST', 'localhost')

MQ_queue = "DBOperation"

MQ_exchange = ""

MQ_routing_key = "DBOperation"


# configs for Elastic DB (search DB)

ES_host = os.environ.get('ES_LOCALHOST', 'localhost')

ES_index_name = "twitter"


# Config for Rabbit MQ - logger 

logger_host = MQ_host

logger_queue = "logger"

logger_exchange = ""

logger_routing_key = "logger"

