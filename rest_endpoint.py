from typing import Any, Dict, Optional, Tuple
from threading import Lock, Condition
import logging
import time


import requests
from werkzeug.datastructures import ContentRange
import werkzeug.http

from filter import Filter, HostPort, TransactionMetadata
from response import Response, Esmtp
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

class RestEndpoint(Filter):
    # XXX I think these responses are dead code, delete
    start_resp : Optional[Response] = None
    final_status : Optional[Response] = None
    transaction_url : Optional[str] = None
    blob_id_map : BlobIdMap = None
    appended_last = False
    static_remote_host : Optional[HostPort] = None
    static_base_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[str] = None

    # static_remote_host overrides transaction remote_host to send all
    # traffic to a fixed next-hop
    # pass base_url/http_host or transaction_url
    def __init__(self,
                 static_base_url=None,
                 http_host=None,
                 transaction_url=None,
                 static_remote_host : Optional[HostPort] = None,
                 timeout_start=TIMEOUT_START,
                 timeout_data=TIMEOUT_DATA,
                 blob_id_map = None,
                 msa = False,
                 wait_response = True,
                 remote_host_resolution = None):
        self.static_base_url = static_base_url
        self.http_host = http_host
        self.transaction_url = transaction_url
        self.static_remote_host = static_remote_host
        self.chunk_id = 0
        self.timeout_start = timeout_start
        self.timeout_data = timeout_data
        self.blob_id_map = blob_id_map
        self.msa = msa
        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.wait_response = wait_response
        self.remote_host_resolution = None

    def _start(self, tx : TransactionMetadata, req_json : Dict[Any,Any]):
        remote_host_disco = [None]
        next_hop = (self.static_remote_host if self.static_remote_host
                    else tx.remote_host)

        if next_hop:
            if self.remote_host_resolution is not None:
                remote_host_disco = self.remote_host_resolution(next_hop)
            else:
                remote_host_disco = [next_hop]

        for remote_host in remote_host_disco:
            if remote_host is not None:  # none if no remote_host/disco (above)
                req_json['remote_host'] = remote_host.to_tuple()
                self.remote_host = remote_host

            rest_resp = requests.post(self.base_url + '/transactions',
                                      json=req_json,
                                      headers={'host': self.http_host},
                                      timeout=self.timeout_start)
            logging.info('RestEndpoint.start resp %s', rest_resp)
            resp_json = get_resp_json(rest_resp)
            logging.info('RestEndpoint.start resp_json %s', resp_json)
            return resp_json

    def on_update(self, tx : TransactionMetadata):
        if self.static_base_url:
            self.base_url = self.static_base_url
        else:
            self.base_url = tx.rest_endpoint

        if not self.transaction_url:
            resp_json = self._start(tx, tx.to_json())
        else:
            req_json = tx.to_json()
            if self.remote_host:
                req_json['remote_host'] = self.remote_host.to_tuple()
            rest_resp = requests.post(self.transaction_url,
                                      json=req_json,
                                      headers={'host': self.http_host},
                                      timeout=self.timeout_start)

        if not self.transaction_url:
            if not resp_json or 'url' not in resp_json:
                return Response.Internal(
                    'RestEndpoint.start internal error (no json)')
            with self.lock:
                self.transaction_url = self.base_url + resp_json['url']
                self.cv.notify_all()

        if self.wait_response:
            self.get_tx_response(self.timeout_start, tx)

    def append_data(self, last : bool, blob : Blob,
                    mx_multi_rcpt=None) -> Optional[Response]:
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
            # XXX transaction_url??
            ext_id = self.blob_id_map.lookup_or_insert(self.base_url, blob.id())
            req_json['uri'] = ext_id

        rest_resp = requests.post(self.transaction_url + '/appendData',
                                  json=req_json,
                                  headers={'host': self.http_host},
                                  timeout=self.timeout_data)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint.append_data POST resp %s %s',
                     rest_resp, resp_json)
        # XXX http ok (and below)
        if rest_resp.status_code > 299:
            return Response(400, 'RestEndpoint.append_data POST failed: http')
        if resp_json is None:
            return Response(400, 'RestEndpoint.append_data POST failed: no json')
        if 'final_status' in resp_json:
            return Response.from_json(resp_json['final_status'])

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
                # XXX transaction_url?
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
        # XXX shouldn't chunk_uri be fully-qualified by this point?
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

    def get_status(self, timeout) -> Optional[Response]:
        if self.final_status: return self.final_status
        if not self.wait_response: return None
        # TODO inflight waiter list thing?
        resp = self.get_json_response(timeout, 'final_status')
        if not resp:
            return Response(400, 'RestEndpoint.get_status timeout')
        self.final_status = resp
        return self.final_status


    # XXX clearly distinguish internal, test methods

    def get_json(self, timeout):
        try:
            rest_resp = requests.get(self.transaction_url,
                                     headers={'host': self.http_host},
                                     timeout=timeout)
        except Exception:
            return None
        return get_resp_json(rest_resp)


    # block until transaction json contains field and is non-null or timeout,
    # returns Response.from_json() from that field
    def get_json_response(self, timeout, field) -> Optional[Response]:
        now = time.monotonic()
        deadline = now + timeout
        delta = 1
        while now <= deadline:
            deadline_left = deadline - now
            # retry at most once per second
            if delta < 1:
                if deadline_left < 1:
                    break
                time.sleep(min(1 - delta, deadline_left))
            logging.info('RestEndpoint.get_json_response %f', deadline_left)
            start = time.monotonic()

            # TODO pass the deadline in a header?
            resp_json = self.get_json(deadline_left)
            logging.info('RestEndpoint.get_json_response done %s', resp_json)
            delta = time.monotonic() - start
            assert(delta >= 0)
            if resp_json is None:
                return Response(400, 'RestEndpoint.start_response GET '
                                'failed')

            if field in resp_json:
                return Response.from_json(resp_json[field])
            now = time.monotonic()
        return None

    def get_tx_response(self, timeout, tx : TransactionMetadata):
        now = time.monotonic()
        deadline = now + timeout
        delta = 1
        while now <= deadline:
            deadline_left = deadline - now
            # retry at most once per second
            if delta < 1:
                if deadline_left < 1:
                    break
                time.sleep(min(1 - delta, deadline_left))
            logging.info('RestEndpoint.get_json_response %f', deadline_left)
            start = time.monotonic()

            # TODO pass the deadline in a header?
            resp_json = self.get_json(deadline_left)
            logging.info('RestEndpoint.get_tx_response done %s', resp_json)
            delta = time.monotonic() - start
            assert(delta >= 0)
            if resp_json is None:
                return Response(400, 'RestEndpoint.start_response GET '
                                'failed')

            fields = {'mail_from': 'mail_response',
                      'rcpt_to': 'rcpt_response'}
            for req_f,resp_f in fields.items():
                done = True
                if hasattr(tx, req_f) and getattr(tx, req_f):
                    if resp_f in resp_json and resp_json[resp_f]:
                        setattr(tx, resp_f,
                                Response.from_json(resp_json[resp_f]))
                    else:
                        done = False
            if done:
                break

            now = time.monotonic()
        return None


    def set_durable(self):
        # XXX appended_last only gets set after the append has
        # succeeded but this is valid as long as the server has
        # *received* the append i.e. concurrent with inflight
        with self.lock:
            self.cv.wait_for(
                lambda:
                self.transaction_url is not None)  # and self.appended_last)

        rest_resp = requests.post(self.transaction_url + '/smtpMode',
                                  json={},
                                  headers={'host': self.http_host},
                                  timeout=self.timeout_data)
        if rest_resp.status_code > 299:
            logging.info('RestEndpoint.set_durable %s', rest_resp)
            return Response(400, "RestEndpoint.set_durable")

        return Response()

    def abort(self):
        # TODO
        pass
