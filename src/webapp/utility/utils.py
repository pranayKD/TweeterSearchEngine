
def fetch_request_metadata(request):
    try: 
        server_name = request.META['SERVER_NAME']
        remote_addr = request.META['REMOTE_ADDR']
        server_port = request.META['SERVER_PORT']
        request_method = request.META['REQUEST_METHOD']
        request_path = request.META['PATH_INFO']
    except: 
        server_name = remote_addr = server_port = request_method = request_path = ''

    metadata = {}
    metadata['server_name'] = server_name
    metadata['remote_addr'] = remote_addr
    metadata['server_port'] = server_port
    metadata['request_method'] = request_method
    metadata['request_path'] = request_path

    return metadata