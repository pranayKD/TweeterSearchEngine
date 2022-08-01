import sys,os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir))

import scraper
import mongo_client
import MQProducer
import config

if __name__=="__main__":

    search_topics = sys.argv[1:]
    print("fetching tweets for topics - {}".format(search_topics))

    queue_producer = MQProducer.MQProducer(config.MQ_host, config.MQ_queue, config.MQ_exchange, config.MQ_routing_key)

    db_client = mongo_client.MongoClient(config.mongo_host_name, config.mongo_db_name, config.mongo_col_name, queue_producer)

    apiHelper = scraper.APIHelper(config.search_url , config.auth_token)

    data_scraper = scraper.Scraper(apiHelper, search_topics, config.num_tweets, config.max_tweers,db_client)

    data_scraper.search_topics_and_insert_all()
    

