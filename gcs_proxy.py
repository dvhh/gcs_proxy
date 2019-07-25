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
from flask import Flask, Response, abort

_bearer_token = None
_bearer_token_ctime = 0

_session = None

assert os.environ.get('GCS_BUCKET')
GCS_PROXY_STREAMING = int(os.environ.get('GCS_PROXY_STREAMING', '0')) > 0
GCS_PROXY_BUCKET = os.environ['GCS_BUCKET']
GCS_PROXY_HEADER_EXCEPTION = os.environ.get('GCS_PROXY_HEADER_EXCEPTION', '').split(',')

app = Flask(__name__)

app.logger.debug(GCS_PROXY_BUCKET)


def get_session():
    '''
    Singular requests session object for the current process
    '''
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def refresh_token():
    '''
    Refresh service account token
    '''
    session = get_session()
    response = session.get(
        'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token',
        headers={
            'Metadata-Flavor': 'Google',
            'User-Agent': 'GCS Proxy'
        }
    )
    return response.json()


def get_bearer_token():
    '''
    Ensure token freshness
    '''
    global _bearer_token
    global _bearer_token_ctime
    if time.time() - _bearer_token_ctime > 60:
        _bearer_token = None

    if _bearer_token is None:
        _bearer_token = refresh_token()
        _bearer_token_ctime = time.time()

    return _bearer_token['access_token']


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


def get_metadata(bucket: str, path: str):
    uri = 'https://www.googleapis.com/storage/v1/b/{}/o/{}'.format(
        bucket,
        path
    )
    app.logger.debug('[{}]\n'.format(uri))
    token = get_bearer_token()
    session = get_session()
    response = session.get(uri, headers={
        'Authorization': 'Bearer {}'.format(token),
        'User-Agent': 'GCS Proxy'
    })
    if response.status_code != 200:
        app.logger.debug(
            '[{}]\n{}\n'.format(response.status_code, response.content)
        )
        response.close()
        abort(response.status_code)
    return response.json()


def reformat_time(iso_date: str):
    stripped_date = re.sub(r'\.\d+Z', 'Z', iso_date)
    timestamp = time.strptime(stripped_date, '%Y-%m-%dT%H:%M:%SZ')
    return time.strftime('%a, %d %b %Y %H:%M:%S GMT', timestamp)


@app.route('/<path:path>', methods=['GET'])
def bucket_proxy(path: str):
    '''
    Object proxy handler
    '''
    token = get_bearer_token()
    session = get_session()
    path = urllib.parse.quote(path, safe='')
    global GCS_PROXY_BUCKET
    metadata = get_metadata(GCS_PROXY_BUCKET, path)
    app.logger.debug(json.dumps(metadata))
    uri = 'https://www.googleapis.com/storage/v1/b/{}/o/{}?alt=media'.format(
        GCS_PROXY_BUCKET,
        path
    )
    extra_headers = {'Last-Modified': reformat_time(metadata['updated'])}
    global GCS_PROXY_STREAMING
    app.logger.debug('[{}]\n'.format(uri))
    if GCS_PROXY_STREAMING:
        # streaming response
        response = session.get(uri, headers={
            'Authorization': 'Bearer {}'.format(token),
            'User-Agent': 'GCS Proxy'
        }, stream=True)
        if response.status_code != 200:
            response.close()
            abort(response.status_code)

        def send_response():
            for chunk in response.iter_content(chunk_size=4096):
                yield chunk
            response.close()

        result = Response(
            send_response(),
            mimetype=response.headers['Content-Type'],
            headers=copy_headers(response.headers, extra_headers)
        )
        return result

    # non streaming response
    response = session.get(uri, headers={
        'Authorization': 'Bearer {}'.format(token),
        'User-Agent': 'GCS Proxy'
    })
    if response.status_code != 200:
        response.close()
        abort(response.status_code)
    return Response(
        response.content,
        mimetype=response.headers['Content-Type'],
        headers=copy_headers(response.headers, extra_headers)
    )


@app.route('/', methods=['GET'])
def default_route():
    '''
    Default route
    TODO: Do something more useful / relevant
    '''
    return 'OK'


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
