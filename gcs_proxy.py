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
from google.cloud import storage
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.exceptions import ExceptionMiddleware, HTTPException
from starlette.convertors import PathConvertor
import uvicorn
import sys
import logging

assert os.environ.get('GCS_BUCKET')
GCS_PROXY_BUCKET = os.environ['GCS_BUCKET']
app = Starlette(debug=True)

logging.basicConfig(
    level=2,
    format="%(asctime)-15s %(levelname)-8s %(message)s"
)
# app.logger.debug(GCS_PROXY_BUCKET)

_storage_client = None


def get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


def reformat_time(iso_date: str):
    logger = logging.getLogger("uvicorn")
    logger.info(iso_date)
    stripped_date = re.sub(r'\.\d+\+00:00', '', iso_date)
    timestamp = time.strptime(stripped_date, '%Y-%m-%d %H:%M:%S')
    return time.strftime('%a, %d %b %Y %H:%M:%S GMT', timestamp)


@app.exception_handler(404)
async def not_found(request, exc):
    return Response(exc.detail, status_code=exc.status_code)

@app.route('/{path:path}')
async def bucket_proxy(request):
    # return Response('OK')
    '''
    Object proxy handler
    '''
    path = request.path_params['path']
    #return Response(path)
    logger = logging.getLogger("uvicorn")
    #sys.stdout.write(path+'\n')
    logger.info(path)
    global GCS_PROXY_BUCKET
    storage_client = get_storage_client()
    bucket = storage_client.get_bucket(GCS_PROXY_BUCKET)

    blob = bucket.get_blob(path)
    if blob is None:
        raise HTTPException(status_code=404,detail=path)
    content = blob.download_as_string()
    headers = {
        'Last-Modified': reformat_time(str(blob.updated)),
        'Content-Length': str(blob.size),
        'Content-type': blob.content_type,
        'Etag': blob.etag,
        'Crc32c': blob.crc32c,
        'md5_hash': blob.md5_hash
    }
    if blob.content_encoding is not None:
        headers['Content-encoding'] = blob.content_encoding
    if blob.content_language is not None:
        headers['Content-language'] = blob.content_language
    return Response(content, status_code=200, headers=headers);


@app.route('/')
async def default_route(request):
    '''
    Default route
    TODO: Do something more useful / relevant
    '''
    return Response('OK')


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8000, workers=10)
