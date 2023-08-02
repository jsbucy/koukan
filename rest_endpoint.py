import requests

from werkzeug.datastructures import ContentRange

import werkzeug.http

from typing import Tuple, Optional

from response import Response, Esmtp

def get_resp_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

class RestEndpoint:
    chunk_id = None
    final_status : Optional[Response] = None

    # static_remote_host overrides transaction remote_host to send all
    # traffic to a fixed next-hop
    def __init__(self, base_url, http_host, static_remote_host=None):
        self.base_url = base_url
        self.http_host = http_host
        self.static_remote_host = static_remote_host


    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        next_hop = self.static_remote_host if self.static_remote_host else remote_host
        req_json = {
            'local_host': local_host,
            'remote_host': next_hop,
            'mail_from': mail_from,
            'rcpt_to': rcpt_to
        }
        rest_resp = requests.post(self.base_url + '/transactions',
                                  json=req_json,
                                  headers={'host': self.http_host})
        print(rest_resp)
        resp_json = get_resp_json(rest_resp)
        if not resp_json:
            return Response(400, 'RestEndpoint.start internal error (no json)')

        if 'final_status' in resp_json:
            resp = Response.from_json(resp_json['final_status'])
            if resp.err():
                return resp
        self.transaction_url = self.base_url + resp_json['url']
        # only present if err?
        if 'transaction_status' in rest_resp.json():
            self.final_status = Response.from_json(rest_resp.json()['transaction_status'])
            return self.final_status
        self.chunk_id = 0
        return Response(250, 'RestEndpoint start transaction ok')

    def append_data(self, last : bool, d : bytes = None, blob_id = None):
        print('RestEndpoint.append_data', last, d is None, blob_id is None)
        if self.final_status:
            return Response(500, 'RestEndpoint.append_data transaction already failed'), None

        chunk_id = self.chunk_id
        self.chunk_id += 1
        req_json = {
            'chunk_id': chunk_id,
            'last': last }

        if d is not None and len(d) < 1024:
            uri = False
            try:
                utf8 = d.decode('utf-8')
            except UnicodeError:
                pass  # i.e. skip if not text
            else:
                req_json['d'] = utf8
                requests.post(self.transaction_url + '/appendData',
                              json=req_json,
                              headers={'host': self.http_host})
                return Response(), None

        if blob_id:
            req_json['uri'] = blob_id

        rest_resp = requests.post(self.transaction_url + '/appendData',
                                  json=req_json,
                                  headers={'host': self.http_host})
        print('RestEndpoint.append_data POST resp', rest_resp)
        if rest_resp.status_code > 299:
            return Response(400, 'RestEndpoint.append_data POST failed'), None

        if 'uri' not in rest_resp.json():
            return Response(400, 'RestEndpoint.append_data endpoint didn\'t'
                            ' return uri'), None

        uri = rest_resp.json()['uri']

        # XXX
        # server can't/won't share blob_id
        # get/upload blob data
        assert(blob_id is None)
        self.append_data_chunk(uri, offset=0, d=d, last=True)

        return Response(), uri

    # -> (resp, len)
    def append_data_chunk(self, chunk_uri, offset,
                          d : bytes, last : bool):
        headers = {}
        if d is not None:
            range = ContentRange('bytes', offset, len(d),
                                 offset + len(d) if last else None)
            headers['content-range'] = range.to_header()
        headers['host'] = self.http_host
        rest_resp = requests.put(
            self.base_url + chunk_uri, headers=headers, data=d)
        if 'content-range' not in rest_resp.headers:
            return Response(500, 'RestEndpoint.append_data_chunk resp range'), None
        range = werkzeug.http.parse_content_range_header(
            rest_resp.headers.get('content-range'))
        return Response(), range.stop

    def get_status(self):
        if not self.final_status:
            rest_resp = requests.get(self.transaction_url,
                                headers={'host': self.http_host})
            resp_json = get_resp_json(rest_resp)
            self.final_status = Response.from_json(resp_json['final_status'])
        return self.final_status
