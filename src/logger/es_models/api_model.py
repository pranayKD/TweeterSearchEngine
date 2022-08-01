"""
    Mapping used for logs of Django APIs 
"""

api_log_mapping = {
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
            "server_name": {
                "type": "keyword"
            },
            "remote_addr": {
                "type": "keyword"
            }, 
            "server_port": {
                "type": "keyword"
            }, 
            "request_method": {
                "type": "keyword"
            }, 
            "request_path": {
                "type": "keyword"
            },
            "response_status": {
                "type": "keyword"
            }, 
            "response_time" : {
                "type": "float"
            }
        }
    }
}


