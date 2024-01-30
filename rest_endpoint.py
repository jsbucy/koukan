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

def get_resp_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

# This is a map from internal blob IDs to upstream so parallel sends
# to the same place (i.e. smtp outbound gw) can reuse blobs.
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
    transaction_url : Optional[str] = None
    blob_id_map : BlobIdMap = None
    static_remote_host : Optional[HostPort] = None
    static_base_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[HostPort] = None
    etag : Optional[str] = None
    max_inline : int
    chunk_size : int

    # PATCH sends rcpts to append but it sends back the responses for
    # all rcpts so far, need to remember how many we've sent to know
    # if we have all the responses.
    rcpts = 0

    def _set_request_timeout(self, headers, timeout : Optional[float] = None):
        if timeout:
            headers['request-timeout'] = str(int(timeout))

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
                 wait_response = True,
                 remote_host_resolution = None,
                 min_poll=1,
                 max_inline=1024,
                 chunk_size=1048576):
        self.base_url = self.static_base_url = static_base_url
        self.http_host = http_host
        self.transaction_url = transaction_url
        self.static_remote_host = static_remote_host
        self.timeout_start = timeout_start
        self.timeout_data = timeout_data
        self.blob_id_map = blob_id_map

        # TODO this is only false in router_service test which needs
        # to be ported to the raw request subset of the api.
        self.wait_response = wait_response
        self.remote_host_resolution = None

        self.min_poll = min_poll
        self.max_inline = max_inline
        self.chunk_size = chunk_size

    def _start(self, tx : TransactionMetadata, req_json : Dict[Any,Any],
               timeout : Optional[float] = None):
        logging.debug('RestEndpoint._start %s', req_json)
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

            req_headers = {'host': self.http_host}
            self._set_request_timeout(req_headers, timeout)
            rest_resp = requests.post(self.base_url + '/transactions',
                                      json=req_json,
                                      headers=req_headers,
                                      timeout=self.timeout_start)
            # XXX rest_resp.status_code?
            logging.info('RestEndpoint.start resp %s', rest_resp)
            self.etag = rest_resp.headers.get('etag', None)
            resp_json = get_resp_json(rest_resp)
            logging.info('RestEndpoint.start resp_json %s', resp_json)

            assert resp_json.get('url', None) is not None
            # XXX return type!
            #if not resp_json or 'url' not in resp_json:
            #    return Response.Internal(
            #        'RestEndpoint.start internal error (no json)')
            self.transaction_url = self.base_url + resp_json['url']

            return rest_resp

    def _update(self, tx, timeout : Optional[float] = None):
        req_json = tx.to_json()
        if self.remote_host:
            req_json['remote_host'] = self.remote_host.to_tuple()
        req_headers = { 'host': self.http_host }
        self._set_request_timeout(req_headers, timeout)
        if self.etag:
            req_headers['if-match'] = self.etag
        rest_resp = requests.patch(self.transaction_url,
                                   json=req_json,
                                   headers=req_headers,
                                   timeout=self.timeout_start)

        logging.info('RestEndpoint.on_update resp %s', rest_resp)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint.on_update resp_json %s', resp_json)

        if rest_resp.status_code < 300:
            self.rcpts += len(tx.rcpt_to)
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
            # xxx err?
        return rest_resp


    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        if self.base_url is None and tx.rest_endpoint:
            self.base_url = tx.rest_endpoint

        if not self.transaction_url:
            rest_resp = self._start(tx, tx.to_json())
            self.rcpts = len(tx.rcpt_to)
        else:
            rest_resp = self._update(tx)

        timeout = timeout if timeout is not None else self.timeout_start
        if not self.wait_response:
            return
        # xxx filter api needs to accomodate returning an error here
        # or else stuff the http error in the response for the first
        # inflight req field in tx?
        if rest_resp.status_code >= 300:
            return
        self.get_tx_response(timeout, tx, rest_resp.json())

    def _append_inline(self, last, blob : Blob):
        req_headers = {'host': self.http_host}
        if self.etag:
            req_headers['if-match'] = self.etag

        req_json = {'last': last}

        try:
            utf8 = blob.contents().decode('utf-8')
        except UnicodeError:
            pass  # i.e. skip if not text
        else:
            req_json['d'] = utf8
        rest_resp = requests.post(self.transaction_url + '/appendData',
                                  json=req_json,
                                  headers=req_headers,
                                  timeout=self.timeout_data)
        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)

        # XXX rest_resp.status_code or
        #   'data_response' in rest_resp.json()
        logging.info('RestEndpoint.append_data inline %s', rest_resp)
        return rest_resp

    def _append_blob(self, last, blob_uri):
        req_headers = {'host': self.http_host}
        if self.etag:
            req_headers['if-match'] = self.etag

        req_json = { 'last': last,
                     'uri': blob_uri }
        rest_resp = requests.post(self.transaction_url + '/appendData',
                                  json=req_json,
                                  headers=req_headers,
                                  timeout=self.timeout_data)
        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        return rest_resp

    def append_data(self, last : bool, blob : Blob) -> Optional[Response]:
        logging.info('RestEndpoint.append_data %s %d', last, blob.len())

        if blob.len() < self.max_inline:
            self._append_inline(last, blob)
            if not last:
                return Response()
            # XXX -> get_tx_response()?
            return self.get_status(self.timeout_data)

        ext_id = None
        if blob.id() and self.blob_id_map:
            # XXX transaction_url??
            ext_id = self.blob_id_map.lookup_or_insert(self.base_url, blob.id())

        rest_resp = self._append_blob(last, ext_id)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint.append_data POST resp %s %s',
                     rest_resp, resp_json)
        # XXX http ok (and below)
        if rest_resp.status_code > 299:
            return Response(400, 'RestEndpoint.append_data POST failed: http')
        if resp_json is None:
            return Response(400, 'RestEndpoint.append_data POST failed: '
                            'no json')

        if data_response := Response.from_json(
                resp_json.get('data_response', {})):
            return data_response

        if not ext_id and 'uri' not in resp_json:
            return Response(400, 'RestEndpoint.append_data endpoint didn\'t'
                            ' return uri')

        if 'uri' not in resp_json:
            logging.info('RestEndpoint.append_data reused blob %s -> %s',
                         blob.id(), ext_id)
        else:
            # i.e. we didn't already have ext_id or server didn't accept
            resp = self._put_blob(blob, resp_json['uri'])

        if not last:
            return resp
        else:
            with self.lock:
                self.cv.notify_all()

        return self.get_status(self.timeout_data)


    def _put_blob(self, blob, uri):
        logging.info('RestEndpoint.append_data via uri %s', uri)

        offset = 0
        while offset < blob.len():
            chunk = blob.contents()[offset:offset+self.chunk_size]
            last = (offset + self.chunk_size) > len(chunk)
            resp,offset = self._put_blob_chunk(
                uri, offset=offset,
                d=chunk,
                last=last)  # XXX
            if resp.err():
                return resp
        #logging.info('RestEndpoint.append_data %s %d %s',
        #             last, offset, resp)
        if blob.id() and self.blob_id_map:
            # XXX transaction_url?
            self.blob_id_map.finalize(self.base_url, blob.id(), uri)
        return resp

    # -> (resp, len)
    def _put_blob_chunk(self, chunk_uri, offset,
                         d : bytes, last : bool):
        logging.info('RestEndpoint.append_data_chunk %s %d %d %s',
                     chunk_uri, offset, len(d), last)
        headers = {}
        if d is not None:  # XXX when can this be None?
            range = ContentRange('bytes', offset, offset + len(d),
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
            if range is None or range.start != offset:
                return Response(
                    400, 'RestEndpoint.append_data_chunk bad range'), None
            dlen = range.stop

        return Response(), dlen

    def get_status(self, timeout) -> Optional[Response]:
        if not self.wait_response: return None
        resp = self.get_json_response(timeout, 'data_response')
        if not resp:
            return Response(400, 'RestEndpoint.get_status timeout')
        return resp


    # XXX clearly distinguish internal, test methods

    def get_json(self, timeout : Optional[float] = None):
        try:
            req_headers = {'host': self.http_host}
            self._set_request_timeout(req_headers, timeout)
            rest_resp = requests.get(self.transaction_url,
                                     headers=req_headers,
                                     timeout=timeout)
            logging.debug('RestEndpoint.get_json %s', rest_resp)
        except requests.Timeout:
            return None
        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
        return get_resp_json(rest_resp)

    # block until transaction json contains field and is non-null or timeout,
    # returns Response.from_json() from that field
    # TODO used by router_service_test which should get off of this,
    # get_status() which should be refactored with the rest of the
    # Filter api around data to take tx?
    def get_json_response(self, timeout, field) -> Optional[Response]:
        # XXX verify this timeout code
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
                return Response(400, 'RestEndpoint.get_json_response GET '
                                'failed')

            if resp_json.get(field, None) is not None:
                if field == 'rcpt_response':
                    # xxx wait until rcpt_response is expected length?
                    return [Response.from_json(r) for r in resp_json[field]]
                return Response.from_json(resp_json[field])
            now = time.monotonic()
        return None

    # update tx response fields per json
    def _update_tx(self, tx, tx_json):
        done = True
        if tx.mail_from and tx.mail_response is None:
            if mail_resp := Response.from_json(
                    tx_json.get('mail_response', {})):
                tx.mail_response = mail_resp
            else:
                done = False
        if (len([r for r in tx.rcpt_response if r is not None]) !=
            len(tx.rcpt_to)):
            rcpt_resp = [
                Response.from_json(r)
                for r in tx_json.get('rcpt_response', []) ]
            new_rcpt_offset = self.rcpts - len(tx.rcpt_to)
            rcpt_resp = rcpt_resp[new_rcpt_offset:]
            if len([r for r in rcpt_resp if r is not None]
                   ) == len(tx.rcpt_to):
                tx.rcpt_response = rcpt_resp
            else:
                done = False
        return done

    # poll/GET the tx until all mail/rcpts in tx have corresponding
    # responses or timeout
    # if you already have tx json from a previous POST/PATCH, pass it
    # in resp_json
    def get_tx_response(self, timeout, tx : TransactionMetadata,
                        tx_json={}):
        deadline = time.monotonic() + timeout
        prev = 0
        while True:
            if self._update_tx(tx, tx_json):
                break
            now = time.monotonic()
            deadline_left = deadline - now
            if deadline_left < self.min_poll:
                break
            delta = now - prev
            prev = now
            # retry at most once per self.min_poll
            if delta < self.min_poll:
                wait = self.min_poll - delta
                time.sleep(wait)

            # TODO pass the deadline in a header?
            tx_json = self.get_json(deadline_left)
            logging.info('RestEndpoint.get_tx_response done %s', tx_json)
            if tx_json is None:
                return Response(400, 'RestEndpoint.get_tx_response GET '
                                'failed')

        return None


    def abort(self):
        # TODO
        pass
