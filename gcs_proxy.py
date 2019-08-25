'''
GCS Bucket Proxy

Web proxy to serve GCS using service account token
'''
import json
import time
import urllib.parse
import os
import re
import requests
from flask import Flask, Response, abort, make_response
from google.cloud import storage


assert os.environ.get('GCS_BUCKET')
GCS_PROXY_BUCKET = os.environ['GCS_BUCKET']
app = Flask(__name__)

app.logger.debug(GCS_PROXY_BUCKET)

_storage_client = None


def get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client



def copy_headers(input_headers, extra_headers):
    result = {}
    global GCS_PROXY_HEADER_EXCEPTION
    for key in input_headers:
        if key in GCS_PROXY_HEADER_EXCEPTION:
            continue
        result[key] = input_headers[key]
    for key in extra_headers:
        result[key] = extra_headers[key]
    return result


def reformat_time(iso_date: str):
    stripped_date = re.sub(r'\.\d+Z', 'Z', iso_date)
    timestamp = time.strptime(stripped_date, '%Y-%m-%dT%H:%M:%SZ')
    return time.strftime('%a, %d %b %Y %H:%M:%S GMT', timestamp)


@app.route('/<path:path>', methods=['GET'])
def bucket_proxy(path: str):
    '''
    Object proxy handler
    '''
    global GCS_PROXY_BUCKET
    storage_client = get_storage_client()
    bucket = storage_client.get_bucket(GCS_PROXY_BUCKET)
    blob = bucket.get_blob(path)
    if blob is None:
        abort(404)
    content = blob.download_as_string()
    headers = {
        'Last-Modified': blob.updated,
        'Content-Length': blob.size,
        'Content-type': blob.content_type,
        # 'Content-encoding': blob.content_encoding,
        # 'Content-language': blob.content_language,
        'Etag': blob.etag,
        'Crc32c': blob.crc32c,
        'md5_hash': blob.md5_hash
    }
    if blob.content_encoding is not None:
        headers['Content-encoding'] = blob.content_encoding
    if blob.content_language is not None:
        headers['Content-language'] = blob.content_language
    return make_response(content, headers);


@app.route('/', methods=['GET'])
def default_route():
    '''
    Default route
    TODO: Do something more useful / relevant
    '''
    return 'OK'


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
