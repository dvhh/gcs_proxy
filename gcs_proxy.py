'''
GCS Bucket Proxy

Web proxy to serve GCS using service account token
'''
import time
import urllib.parse
import os
import sys
import requests
from flask import Flask, Response, abort

_bearer_token = None
_bearer_token_ctime = 0

_session = None

GCS_PROXY_STREAMING = False


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


app = Flask(__name__)


def copy_headers(input):
    result = {}
    for key in input:
        result[key] = input[key]
    return result


@app.route('/<path:path>', methods=['GET'])
def bucket_proxy(path: str):
    '''
    Object proxy handler
    '''
    token = get_bearer_token()
    session = get_session()
    path = urllib.parse.quote(path, safe='')
    uri = 'https://www.googleapis.com/storage/v1/b/{}/o/{}?alt=media'.format(
        os.environ['GCS_BUCKET'],
        path
    )
    global GCS_PROXY_STREAMING
    # sys.stderr.write('[{}]\n'.format(uri))
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
            headers=copy_headers(response.headers)
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
        headers=copy_headers(response.headers)
    )


@app.route('/', methods=['GET'])
def default_route():
    '''
    Default route
    TODO: Do something more useful / relevant
    '''
    return 'OK'


if __name__ == "__main__":
    assert os.environ.get('GCS_BUCKET')
    sys.stderr.write('{}\n'.format(os.environ.get('GCS_BUCKET')))
    global GCS_PROXY_STREAMING
    GCS_PROXY_STREAMING = int(os.environ.get('GCS_PROXY_STREAMING', '0')) > 0
    app.run(host='0.0.0.0', port=5000)
