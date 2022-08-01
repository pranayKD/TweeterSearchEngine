from config import ES_index_name
from mongoengine import *


class UserMention(EmbeddedDocument):
    """
        User Mentions Nested Object
    """
    user_name = StringField(required=True)
    screen_name = StringField(required=True)

    
class Tweet(Document):
    """
        Model for Tweet in Mongo
    """

    # Details for elastic reference
    _index = StringField(required=True)

    # Tweet Details
    _id = StringField(required=True,primary_key=True)
    tweet_text = StringField(required=True) 
    created_at = DateTimeField(required=True)

    hashtags = ListField(StringField(), default=list)
    user_mentions = ListField(EmbeddedDocumentField(UserMention), default=list)
    
    source = StringField(required=True)
    parent_tweet_id = StringField(required=False)
    parent_user_id = StringField(required=False)
    retweet_count = IntField(required=True)
    favorite_count = IntField(required=True)

    # User details
    user_id = StringField(required=True)
    user_name = StringField(required=True)
    user_screen_name = StringField(required=True)
    user_description = StringField(required=True)
    user_created_at = DateTimeField(required=True)
    user_verified = BooleanField(required=True)
