# Google Cloud Storage Bucket Proxy

A proxy server to serve google cloud storage object to the web.

# Requirements

- Python3
- Flask
- Requests

# Using

Run from a machine having a service account, and ensure that the used service account have sufficient permission to read the bucket and the object you intent to serve via this proxy.

Export to the environment
- GCS_PROXY_STREAMING - set to 1 to enable response steam (disabled by default)
- GCS_BUCKET - set to the bucket name the proxy should serve
- GCS_PROXY_HEADER_EXCEPTION - set header to omit from google storage API, comma separated

```
$ export GCS_PROXY_HEADER_EXCEPTION=X-Goog-Metageneration,X-Goog-Storage-Class,X-Goog-Generation,X-GUploader-UploadID,X-Goog-Hash,Vary,Cache-Control,Pragma
$ export GCS_BUCKET=${BUCKET_NAME} 
$ python3 gcs_proxy.py
```

You can invoke through any other wsgi server ( like gunicorn ).

It is also recommended that you run this through a reverve application proxy, which would take care of extra details (such as caching )
