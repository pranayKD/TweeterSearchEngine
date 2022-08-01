"""
    Mappings for index used for storing tweets in ES
"""

tweet_mapping = {
    "settings": {
        "analysis": {
          "analyzer": {
            "default": {
              "type": "english"
            }
          },
          "normalizer": {
            "lowercase_normalizer": {
                "type": "custom",
                "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "tweet_text": {
                 "type": "text",
                 "analyzer": "english"
            },
            "created_at": {
                "type": "date",
                "format": "E LLL dd HH:mm:ss Z yyyy"
            },
            "hashtags": {
                "type": "keyword",
                "normalizer": "lowercase_normalizer",
                "fields": {
                    "suggest": {
                        "type": "completion"
                    }
                }
            },
            "user_mentions": {
                "type": "nested",
                "properties": {
                    "user_name": {
                        "type": "keyword",
                        "normalizer": "lowercase_normalizer"
                    }, 
                    "screen_name": {
                        "type": "keyword",
                        "normalizer": "lowercase_normalizer"
                    }
                }
            },
            "source": {
                "type": "keyword",
                "normalizer": "lowercase_normalizer"
            },
            "parent_tweet_id": {
                "type": "keyword",
                "index": False
            },
            "parent_user_id": {
                "type": "keyword",
                "index": False
            },
            "retweet_count": {
                "type": "integer"
            },
            "favorite_count": {
                "type": "integer"
            },
            "user_id": {
                "type": "keyword",
                "index": False
            }, 
            "user_name": {
                "type" : "keyword",
                "normalizer": "lowercase_normalizer"
            },
            "user_screen_name": {
                "type": "keyword",
                "normalizer": "lowercase_normalizer"
            },
            "user_description": {
                "type":  "text",
                "analyzer": "english"
            },
            "user_created_at": {
                "type": "date",
                "format": "E LLL dd HH:mm:ss Z yyyy"
            },
            "user_verified":{
                "type": "boolean"
            }
        }
    }
}


