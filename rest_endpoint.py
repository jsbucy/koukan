import requests
import time

from werkzeug.datastructures import ContentRange

import werkzeug.http

from typing import Tuple, Optional

from response import Response, Esmtp

import logging

from blob import Blob

def get_resp_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

class RestEndpoint:
    start_resp : Optional[Response] = None
    final_status : Optional[Response] = None
    transaction_url : Optional[str] = None

    # static_remote_host overrides transaction remote_host to send all
    # traffic to a fixed next-hop
    def __init__(self, base_url, http_host, static_remote_host=None, sync=False):
        self.base_url = base_url
        self.http_host = http_host
        self.static_remote_host = static_remote_host
        self.chunk_id = 0

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):
        next_hop = (self.static_remote_host if self.static_remote_host
                    else remote_host)
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
        if not resp_json or 'url' not in resp_json:
            return Response(400, 'RestEndpoint.start internal error (no json)')

        self.transaction_url = self.base_url + resp_json['url']

        return self.start_response(1000000)  # XXX timeout

    def append_data(self, last : bool, blob : Blob) -> Response:
        logging.info('RestEndpoint.append_data %s %d', last, blob.len())
        if self.final_status:
            return Response(500, 'RestEndpoint.append_data transaction '
                            'already failed')

        chunk_id = self.chunk_id
        self.chunk_id += 1
        req_json = {
            'chunk_id': chunk_id,
            'last': last }

        if blob.len() < 1024:
            uri = False
            try:
                utf8 = blob.contents().decode('utf-8')
            except UnicodeError:
                pass  # i.e. skip if not text
            else:
                req_json['d'] = utf8
                rest_resp = requests.post(self.transaction_url + '/appendData',
                                          json=req_json,
                                          headers={'host': self.http_host})
                logging.info('RestEndpoint.append_data inline %s', rest_resp)
                if not last: return Response()
                return self.get_status(1000000)

        # xxx internal -> external blob id map

        # TODO at a minimum, this needs to wait if start transaction
        # is inflight to get the url, etc.

        # TODO inflight waiter list thing on this: if we get an
        # internal blob id and we have another inflight request for
        # the same one, wait for that to share the upstream blob id
        # we have basically the same thing in the router transaction
        # -> storage code

        # xxx dead code, inline blob has no id
        if blob.id():
            req_json['uri'] = blob_id

        rest_resp = requests.post(self.transaction_url + '/appendData',
                                  json=req_json,
                                  headers={'host': self.http_host})
        print('RestEndpoint.append_data POST resp', rest_resp)
        if rest_resp.status_code > 299:
            return Response(400, 'RestEndpoint.append_data POST failed')

        if 'uri' not in rest_resp.json():
            return Response(400, 'RestEndpoint.append_data endpoint didn\'t'
                            ' return uri')

        uri = rest_resp.json()['uri']

        # XXX
        # server can't/won't share blob_id
        # get/upload blob data
        assert(blob_id is None)
        # XXX len??
        resp,len = self.append_data_chunk(
            uri, offset=0, d=blob.contents(), last=True)
        return resp

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
            return Response(500,
                            'RestEndpoint.append_data_chunk resp range'), None
        range = werkzeug.http.parse_content_range_header(
            rest_resp.headers.get('content-range'))
        if not last:
            return Response(), range.stop
        resp = self.get_status(1000000)
        return resp, range.stop if resp.ok() else 0  # XXX

    def start_response(self, timeout):
        if self.start_resp: return self.start_resp
        resp = self.get_json(timeout, 'start_response')
        if not resp: return None
        self.start_resp = resp
        return self.start_resp

    def get_status(self, timeout):
        if self.final_status: return self.final_status
        resp = self.get_json(timeout, 'final_status')
        if not resp: return None
        self.final_status = resp
        return self.final_status

    # block until transaction json contains field and is non-null or timeout
    def get_json(self, timeout, field):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            logging.info('RestEndpoint.get_json')
            start = time.monotonic()
            rest_resp = requests.get(self.transaction_url,
                                     headers={'host': self.http_host})
            logging.info('RestEndpoint.get_json done %s', rest_resp)
            delta = time.monotonic() - start
            if rest_resp.status_code >= 400 and rest_resp.status_code < 500:
                return Response(400, 'RestEndpoint.start_response GET '
                                'failed')
            resp_json = get_resp_json(rest_resp)
            logging.info('RestEndpoint.get_json %s', resp_json)
            if (rest_resp.status_code >= 500 or
                not (resp_json := get_resp_json(rest_resp)) or
                field not in resp_json or not resp_json[field]):
                # cf rest service get_transaction()
                if delta < 5: time.sleep(5 - delta)
                continue
            return Response.from_json(resp_json[field])
        return None

    def wait_status(self):
        pass

    # only for smtp gw -> router
    # XXX this needs some synchronization with append_data(), this
    # should block until we have received the upload response (vs the
    # final transaction response) for the last upload
    def set_durable(self):
        rest_resp = requests.post(self.transaction_url + '/smtpMode',
                                  json={},
                                  headers={'host': self.http_host})
        return Response()
