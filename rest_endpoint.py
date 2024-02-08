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

class RestEndpoint(Filter):
    transaction_url : Optional[str] = None
    static_remote_host : Optional[HostPort] = None
    static_base_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[HostPort] = None
    etag : Optional[str] = None
    max_inline : int
    chunk_size : int
    body_url : Optional[str] = None
    body_len : int = 0

    # PATCH sends rcpts to append but it sends back the responses for
    # all rcpts so far, need to remember how many we've sent to know
    # if we have all the responses.
    rcpts = 0

    def _set_request_timeout(self, headers, timeout : Optional[float] = None):
        if timeout and int(timeout):
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
            if body := resp_json.get('body', None):
                self.body_url = self.base_url + body

            return rest_resp

    def _update(self, tx=None, req_json=None, timeout : Optional[float] = None):
        if tx:
            req_json = tx.to_json()
            if not req_json:
                return  # noop

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
            if tx:
                self.rcpts += len(tx.rcpt_to)
            self.etag = rest_resp.headers.get('etag', None)
            if body := resp_json.get('body', None):
                self.body_url = self.base_url + body
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

        if rest_resp is not None:
            timeout = timeout if timeout is not None else self.timeout_start
            if not self.wait_response:
                return
            # xxx filter api needs to accomodate returning an error here
            # or else stuff the http error in the response for the first
            # inflight req field in tx?
            if rest_resp.status_code >= 300:
                return
            self.get_tx_response(timeout, tx, rest_resp.json())

        # TODO once we've dismantled the old path, this can trickle
        # out the blob as it grows
        if tx.body_blob and tx.body_blob.len() == tx.body_blob.content_length():
            self._append_body(True, tx.body_blob)
            self.get_tx_response(self.timeout_data, tx)

    def _append_body(self, last, blob):
        logging.debug('RestEndpoint._append_body %s %s %s %d',
                      self.transaction_url, self.body_url, last, blob.len())
        if not self.body_url:
            self._update(req_json={'body': ''})
        assert self.body_url

        self._put_blob(blob, last)


    def append_data(self, last : bool, blob : Blob) -> Optional[Response]:
        logging.info('RestEndpoint.append_data %s %s %s %d',
                     self.transaction_url, self.body_url, last, blob.len())

        self._append_body(last, blob)
        if not last:
            return Response()
        return self.get_status(self.timeout_data)

    def _put_blob(self, blob, last):
        logging.info('RestEndpoint._put_blob via uri %s blob.len=%d last %s',
                     self.body_url, blob.len(), last)

        offset = 0
        while offset < blob.len():
            chunk = blob.read(offset, self.chunk_size)
            chunk_last = last and (offset + len(chunk) >= blob.len())
            logging.debug('_put_blob chunk_offset %d chunk len %d '
                          'body_len %d chunk_last %s',
                          offset, len(chunk), self.body_len, chunk_last)

            resp,body_offset = self._put_blob_chunk(
                self.body_url,
                offset=self.body_len,
                d=chunk,
                last=chunk_last)
            if resp.err():
                return resp
            # how many bytes from chunk were actually accepted?
            chunk_out = body_offset - self.body_len
            logging.debug('_put_blob body_offset %d chunk_out %d',
                          body_offset, chunk_out)
            offset += chunk_out
            self.body_len += chunk_out

        return resp

    # -> (resp, len)
    def _put_blob_chunk(self, chunk_uri, offset,
                         d : bytes, last : bool):
        logging.info('RestEndpoint._put_blob_chunk %s %d %d %s',
                     chunk_uri, offset, len(d), last)
        headers = {}
        if d is not None:  # XXX when can this be None?
            range = ContentRange('bytes', offset, offset + len(d),
                                 offset + len(d) if last else None)
            headers['content-range'] = range.to_header()
        headers['host'] = self.http_host
        rest_resp = requests.put(
            chunk_uri, headers=headers, data=d, timeout=self.timeout_data)
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
            logging.debug('_put_blob_chunk resp range %s', range)
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
                if resp:= Response.from_json(resp_json[field]):
                    return resp
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
        if tx.body_blob and (
                tx.body_blob.len() == tx.body_blob.content_length()):
            if data_resp := Response.from_json(
                    tx_json.get('data_response', {})):
                tx.data_response = data_resp
            else:
                done = False

        return done

    # poll/GET the tx until all mail/rcpts in tx have corresponding
    # responses or timeout
    # if you already have tx json from a previous POST/PATCH, pass it
    # in tx_json
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
