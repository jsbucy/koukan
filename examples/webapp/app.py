# gunicorn3 -k gthread -w 1 -b 127.0.0.1:9000 'app:create_app()'

from flask import Flask, Response, request
from flask import send_from_directory
import requests
import json

import logging

def create_app():
    app = Flask(__name__)
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 1  # XXX
    app.logger.setLevel(logging.DEBUG)

    @app.route('/')
    def send_root():
        app.logger.info(request)

        return send_from_directory('.', 'app.html')

    @app.route('/app.js')
    def send_js():
        app.logger.info(request)

        return send_from_directory('.', 'app.js')

    # https://stackoverflow.com/questions/6656363/proxying-to-another-web-service-with-flask
    @app.route('/<path:path>', methods=['GET', 'POST', 'PUT'])
    def proxy(path):
        app.logger.info('proxy %s', request)

        req_js = None
        upstream_host = None
        if request.url.endswith('/transactions'):
            req_js = json.loads(request.data)
            upstream_host = req_js['http_host']
            del req_js['http_host']
        # exclude 'host' header
        req_headers = {k:v for k,v in request.headers if k.lower() != 'host'}
        if upstream_host:
            req_headers['host'] = upstream_host
        upstream_url = request.url.replace(
            request.host_url, 'http://localhost:8000/')
        upstream_body = json.dumps(req_js) if req_js else request.get_data()

        resp = requests.request(
            method = request.method,
            url = upstream_url,
            headers = req_headers,
            data = upstream_body)  # cookies? allow-redirects?

        # NOTE we here exclude all "hop-by-hop headers" defined by RFC
        # 2616 section 13.5.1
        # ref. https://www.rfc-editor.org/rfc/rfc2616#section-13.5.1
        excluded_headers = ['content-encoding', 'content-length',
                            'transfer-encoding', 'connection']
        resp_headers = [
            (k,v) for k,v in resp.raw.headers.items()
            if k.lower() not in excluded_headers
        ]

        resp = Response(resp.content, resp.status_code, resp_headers)
        app.logger.info('proxy resp %s', resp)
        return resp

    return app
