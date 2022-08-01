"""
    Logging Codes used for indexing process. There is no restriction on the message. 
    Message can be changed, but message code needs be consistent
"""


INDEXING_LOG_CODES = {
    'sft' : 'Start Fetching of Tweets',
    'eft' : 'End Fetching of Tweets',
    'caft' : 'Call API to Fetch Tweets',
    'itm' : 'Insert Tweets in Mongo',
    'stq' : 'Send Tweets to Queue',
    'rtq' : 'Receive Tweets from Queue',
    'ite' : 'Insert Tweets in ES'
}



