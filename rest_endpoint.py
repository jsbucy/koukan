import requests
import time

from werkzeug.datastructures import ContentRange
import werkzeug.http

from typing import Tuple, Optional

from threading import Lock, Condition

from response import Response, Esmtp

import logging

from blob import Blob

# these are artificially low for testing
TIMEOUT_START=5
TIMEOUT_DATA=5
CHUNK_SIZE=1048576

MAX_INLINE=1

def get_resp_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

# TODO dedupe with implementation in RouterTransaction?
class BlobIdMap:
    def __init__(self):
        self.map = {}
        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.appended_last = False

    def lookup_or_insert(self, host, blob_id):
        key = (host, blob_id)
        with self.lock:
            if key not in self.map:
                logging.info('BlobIdMap.lookup_or_insert inserted %s %s',
                             host, blob_id)
                self.map[key] = None
                return None
            logging.info('BlobIdMap.lookup_or_insert waiting for %s %s',
                         host, blob_id)
            self.cv.wait_for(lambda: self.map[key] is not None)
            return self.map[key]

    def finalize(self, host, blob_id, ext_blob_id):
        key = (host, blob_id)
        with self.lock:
            self.map[key] = ext_blob_id
            self.cv.notify_all()

class RestEndpoint:
    start_resp : Optional[Response] = None
    final_status : Optional[Response] = None
    transaction_url : Optional[str] = None
    blob_id_map : BlobIdMap = None

    # static_remote_host overrides transaction remote_host to send all
    # traffic to a fixed next-hop
    def __init__(self,
                 base_url, http_host, static_remote_host=None,
                 timeout_start=TIMEOUT_START,
                 timeout_data=TIMEOUT_DATA,
                 blob_id_map = None,
                 msa = False):
        self.base_url = base_url
        self.http_host = http_host
        self.static_remote_host = static_remote_host
        self.chunk_id = 0
        self.timeout_start = timeout_start
        self.timeout_data = timeout_data
        self.blob_id_map = blob_id_map
        self.msa = msa
        self.lock = Lock()
        self.cv = Condition(self.lock)

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
                                  headers={'host': self.http_host},
                                  timeout=self.timeout_start)
        print(rest_resp)
        resp_json = get_resp_json(rest_resp)
        # XXX  rest_resp.status_code or 'start_response' in resp_json
        if not resp_json or 'url' not in resp_json:
            return Response.Internal(
                'RestEndpoint.start internal error (no json)')

        with self.lock:
            self.transaction_url = self.base_url + resp_json['url']
            self.cv.notify_all()

        return self.start_response(self.timeout_start)

    def append_data(self, last : bool, blob : Blob,
                    mx_multi_rcpt=None) -> Response:
        if self.msa:
            assert(self.start_resp is None or not self.start_resp.perm())
        else:
            assert(self.start_resp is not None and self.start_resp.ok() and
                   self.final_status is None)
        # TODO revisit whether we should explicitly split waiting for
        # the post vs polling the json for the result in this api
        with self.lock:
            self.cv.wait_for(lambda: self.transaction_url is not None)

        logging.info('RestEndpoint.append_data %s %d', last, blob.len())

        chunk_id = self.chunk_id
        self.chunk_id += 1
        req_json = {
            'chunk_id': chunk_id,
            'last': last }
        if mx_multi_rcpt: req_json['mx_multi_rcpt'] = True

        if blob.len() < MAX_INLINE:
            uri = False
            try:
                utf8 = blob.contents().decode('utf-8')
            except UnicodeError:
                pass  # i.e. skip if not text
            else:
                req_json['d'] = utf8
                rest_resp = requests.post(self.transaction_url + '/appendData',
                                          json=req_json,
                                          headers={'host': self.http_host},
                                          timeout=self.timeout_data)
                # XXX rest_resp.status_code or
                #   'final_status' in rest_resp.json()
                logging.info('RestEndpoint.append_data inline %s', rest_resp)
                if not last:
                    return Response()
                else:
                    with self.lock:
                        self.appended_last = True
                        self.cv.notify_all()
                return self.get_status(self.timeout_data)

        ext_id = None
        if blob.id() and self.blob_id_map:
            ext_id = self.blob_id_map.lookup_or_insert(self.base_url, blob.id())
            req_json['uri'] = ext_id

        rest_resp = requests.post(self.transaction_url + '/appendData',
                                  json=req_json,
                                  headers={'host': self.http_host},
                                  timeout=self.timeout_data)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint.append_data POST resp %s', resp_json)
        # XXX http ok (and below)
        if rest_resp.status_code > 299 or 'final_status' in resp_json:
            if 'final_status' in resp_json:
                return Response.from_json(resp_json['final_status'])

            return Response(400, 'RestEndpoint.append_data POST failed')

        if 'uri' not in req_json and 'uri' not in resp_json:
            return Response(400, 'RestEndpoint.append_data endpoint didn\'t'
                            ' return uri')

        if 'uri' not in resp_json:
            logging.info('RestEndpoint.append_data reused blob %s -> %s',
                         blob.id(), ext_id)
        else:
            # i.e. we didn't already have ext_id or server didn't accept
            uri = resp_json['uri']
            logging.info('RestEndpoint.append_data via uri %s', uri)

            offset = 0
            while offset < blob.len():
                resp,offset = self.append_data_chunk(
                    uri, offset=offset,
                    d=blob.contents()[offset:offset+CHUNK_SIZE], last=True)
                if resp.err():
                    self.final_status = (
                        Response(400,
                                 'RestEndpoint.append_data blob put error'))
                    return resp
            logging.info('RestEndpoint.append_data %s %d %s',
                         last, offset, resp)
            if blob.id() and self.blob_id_map:
                self.blob_id_map.finalize(
                    self.base_url, blob.id(), resp_json['uri'])

        if not last:
            return resp
        else:
            with self.lock:
                self.appended_last = True
                self.cv.notify_all()

        return self.get_status(self.timeout_data)


    # -> (resp, len)
    def append_data_chunk(self, chunk_uri, offset,
                          d : bytes, last : bool):
        logging.info('RestEndpoint.append_data_chunk %s %d %d %s',
                     chunk_uri, offset, len(d), last)
        headers = {}
        if d is not None:  # XXX when can this be None?
            range = ContentRange('bytes', offset, len(d),
                                 offset + len(d) if last else None)
            headers['content-range'] = range.to_header()
        headers['host'] = self.http_host
        rest_resp = requests.put(
            self.base_url + chunk_uri, headers=headers, data=d,
            timeout=self.timeout_data)
        logging.info('RestEndpoint.append_data_chunk %s', rest_resp)
        # Most(all?) errors on blob put here means temp
        # transaction final status
        # IOW blob upload is not the place to reject the content, etc.
        if rest_resp.status_code > 299:
            return Response(
                400, 'RestEndpoint.append_data_chunk PUT err'), None

        dlen = len(d)
        if 'content-range' in rest_resp.headers:
            range = werkzeug.http.parse_content_range_header(
                rest_resp.headers.get('content-range'))
            if range is None or range.start != 0:
                return Response(
                    400, 'RestEndpoint.append_data_chunk bad range'), None
            dlen = range.length

        return Response(), dlen

    def start_response(self, timeout):
        if self.start_resp: return self.start_resp
        # TODO inflight waiter list thing?
        resp = self.get_json(timeout, 'start_response')
        if not resp:
            return Response(400, 'RestEndpoint.start_response timeout')
        self.start_resp = resp
        return self.start_resp

    def get_status(self, timeout):
        if self.final_status: return self.final_status
        # TODO inflight waiter list thing?
        resp = self.get_json(timeout, 'final_status')
        if not resp:
            return Response(400, 'RestEndpoint.get_status timeout')
        self.final_status = resp
        return self.final_status

    # block until transaction json contains field and is non-null or timeout
    def get_json(self, timeout, field):
        deadline = time.monotonic() + timeout
        while (now := time.monotonic()) < deadline:
            deadline_left = deadline - now
            logging.info('RestEndpoint.get_json')
            start = time.monotonic()
            # XXX throws on timeout
            # TODO pass the deadline in a header?
            rest_resp = requests.get(self.transaction_url,
                                     headers={'host': self.http_host},
                                     timeout=deadline_left)
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
                if delta < 1: time.sleep(1 - delta)  # XXX min deadline_left
                continue
            return Response.from_json(resp_json[field])
        return None

    def wait_status(self):
        pass

    def set_durable(self):
        # TODO revisit whether we should explicitly split waiting for
        # the post vs polling the json for the result in this api
        with self.lock:
            self.cv.wait_for(
                lambda:
                self.transaction_url is not None and self.appended_last)

        rest_resp = requests.post(self.transaction_url + '/smtpMode',
                                  json={},
                                  headers={'host': self.http_host},
                                  timeout=self.timeout_data)
        if rest_resp.status_code > 299:
            logging.info('RestEndpoint.set_durable %s', rest_resp)
            return Response(400, "RestEndpoint.set_durable")

        return Response()
