"""
    Mappings used for logs of tweet indexing process
"""

indexing_log_mapping = {
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
            "start_time": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss.SSSSSS Z"
            },
            "log_level": {
                "type": "keyword"
            },
            "file_name": {
                "type": "keyword"
            },
            "function_name": {
                "type": "keyword"
            },
            "line_number": {
                "type": "integer"
            },
            "process_name": {
                "type": "keyword"
            },
            "message": {
                "type": "text",
                "index": "false"
            },
            "message_code" : {
                "type": "keyword"
            },
            "completion_time": {
                "type": "float"
            }, 
            "a_1": {
                "type": "keyword"
            }, 
            "a_2": {
                "type": "keyword"
            }, 
            "data": {
                "type": "keyword"
            }

        }
    }
}


