from typing import Any, Dict, Optional, Tuple
from threading import Lock, Condition
import logging
import time
import json.decoder

import requests
from werkzeug.datastructures import ContentRange
import werkzeug.http

from filter import Filter, HostPort, TransactionMetadata
from response import Response, Esmtp
from blob import Blob

# these are artificially low for testing
TIMEOUT_START=5
TIMEOUT_DATA=5

# TODO maybe distinguish empty resp.content vs wrong content-type/invalid json?
def get_resp_json(resp):
    if (resp.headers.get('content-type', None) != 'application/json' or
        not resp.content):
        return None
    try:
        return resp.json()
    except json.decoder.JSONDecodeError:
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

    # true if we've sent all of the data upstream and should wait for
    # the data_response
    data_last = False

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
        self.remote_host_resolution = remote_host_resolution

        self.min_poll = min_poll
        self.max_inline = max_inline
        self.chunk_size = chunk_size

    def _start(self, tx : TransactionMetadata, req_json : Dict[Any,Any],
               timeout : Optional[float] = None):
        logging.debug('RestEndpoint._start %s', req_json)
        remote_host_disco = [None]
        next_hop = (self.static_remote_host if self.static_remote_host
                    else tx.remote_host)
        logging.debug('RestEndpoint._start next_hop %s', next_hop)
        if next_hop:
            if self.remote_host_resolution is not None:
                remote_host_disco = self.remote_host_resolution(next_hop.host)
            else:
                remote_host_disco = [next_hop.host]

        for host in remote_host_disco:
            remote_host : Optional[HostPort] = (
                HostPort(host, next_hop.port) if host else None)
            logging.debug('RestEndpoint._start remote_host %s', remote_host)
            # none if no remote_host/disco (above)
            if remote_host is not None:
                req_json['remote_host'] = remote_host.to_tuple()
                self.remote_host = remote_host

            req_headers = {'host': self.http_host}
            self._set_request_timeout(req_headers, timeout)
            try:
                rest_resp = requests.post(self.base_url + '/transactions',
                                          json=req_json,
                                          headers=req_headers,
                                          timeout=self.timeout_start)
            except requests.RequestException:
                return None
            # XXX rest_resp.status_code?
            logging.info('RestEndpoint.start resp %s', rest_resp)
            self.etag = rest_resp.headers.get('etag', None)
            resp_json = get_resp_json(rest_resp)
            logging.info('RestEndpoint.start resp_json %s', resp_json)
            if not resp_json:
                return rest_resp
            self.transaction_url = self.base_url + resp_json['url']
            if body := resp_json.get('body', None):
                self.body_url = self.base_url + body

            return rest_resp

    def _update(self, req_json, timeout : Optional[float] = None):
        req_headers = { 'host': self.http_host }
        self._set_request_timeout(req_headers, timeout)
        if self.etag:
            req_headers['if-match'] = self.etag
        try:
            rest_resp = requests.patch(self.transaction_url,
                                       json=req_json,
                                       headers=req_headers,
                                       timeout=self.timeout_start)
        except requests.RequestException:
            return None
        logging.info('RestEndpoint.on_update resp %s', rest_resp)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint.on_update resp_json %s', resp_json)
        if not resp_json:
            return None

        if rest_resp.status_code < 300:
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

        req_json = tx.to_json()
        body_last = tx.body_blob and (
            tx.body_blob.len() == tx.body_blob.content_length())
        if body_last and not self.body_url:
            req_json['body'] = ''

        if req_json:
            if not self.transaction_url:
                rest_resp = self._start(tx, req_json)
            else:
                rest_resp = self._update(req_json)

            # xxx think a little more carefully about errors here, if
            # we see a timeout on response but the server handled it,
            # we'll get an etag failure on the next req?
            if len(tx.rcpt_response) == len(tx.rcpt_to):
                self.rcpts += len(tx.rcpt_to)

            timeout = timeout if timeout is not None else self.timeout_start
            # TODO test hook is only used in startup probing in router
            # service test, should be able to remove
            if not self.wait_response:
                return
            # xxx filter api needs to accomodate returning an error here
            # or else stuff the http error in the response for the first
            # inflight req field in tx?
            resp_json = get_resp_json(rest_resp) if rest_resp else None
            http_err = rest_resp is None or rest_resp.status_code >= 300 or resp_json is None
            resp_json = resp_json if resp_json else {}
            self.get_tx_response(timeout, tx, resp_json, http_err)

        # TODO this can trickle out the blob as it grows
        if body_last:
            logging.debug('RestEndpoint.on_update body_blob')
            assert self.body_url
            data_resp = self._put_blob(tx.body_blob, last=True)
            logging.debug('on_update %s', data_resp)
            http_err = (data_resp is None)
            self.data_last = True
            self.get_tx_response(self.timeout_data, tx, {}, http_err)

    def _put_blob(self, blob, last) -> Optional[Response]:
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
            if resp is None or resp.err():
                return resp
            # how many bytes from chunk were actually accepted?
            chunk_out = body_offset - self.body_len
            logging.debug('_put_blob body_offset %d chunk_out %d',
                          body_offset, chunk_out)
            offset += chunk_out
            self.body_len += chunk_out
        logging.debug('_put_blob %s', resp)
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
        try:
            rest_resp = requests.put(
                chunk_uri, headers=headers, data=d, timeout=self.timeout_data)
        except requests.RequestException:
            return None, None
        logging.info('RestEndpoint._put_blob_chunk %s', rest_resp)
        # Most(all?) errors on blob put here means temp
        # transaction final status
        # IOW blob upload is not the place to reject the content, etc.
        if rest_resp.status_code > 299:
            return Response(
                400, 'RestEndpoint._put_blob_chunk PUT err'), None

        dlen = len(d)
        if 'content-range' in rest_resp.headers:
            range = werkzeug.http.parse_content_range_header(
                rest_resp.headers.get('content-range'))
            logging.debug('_put_blob_chunk resp range %s', range)
            if range is None or range.start != offset:
                return Response(
                    400, 'RestEndpoint._put_blob_chunk bad range'), None
            dlen = range.stop

        return Response(), dlen


    # XXX clearly distinguish internal, test methods

    def get_json(self, timeout : Optional[float] = None):
        try:
            req_headers = {'host': self.http_host}
            self._set_request_timeout(req_headers, timeout)
            rest_resp = requests.get(self.transaction_url,
                                     headers=req_headers,
                                     timeout=timeout)
            logging.debug('RestEndpoint.get_json %s', rest_resp)
        except requests.RequestException:
            return None
        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
        return get_resp_json(rest_resp)


    # update tx response fields per json
    # timeout: have we reached the deadline and need to fill in temp
    # error responses for any unpopulated req fields
    def _update_tx(self, tx, tx_json, timeout : bool):
        done = True
        if tx.mail_from and tx.mail_response is None:
            if mail_resp := Response.from_json(
                    tx_json.get('mail_response', {})):
                tx.mail_response = mail_resp
            elif timeout:
                tx.mail_response = Response(
                    400, 'RestEndpoint upstream timeout MAIL')
            else:
                done = False

        rcpt_resp = [
            Response.from_json(r)
            for r in tx_json.get('rcpt_response', []) ]
        new_rcpt_offset = self.rcpts - len(tx.rcpt_to)
        rcpt_resp = rcpt_resp[new_rcpt_offset:]
        for i in range(0,len(tx.rcpt_to)):
            if len(tx.rcpt_response) <= i:
                tx.rcpt_response.append(None)
            if tx.rcpt_response[i] is not None:
                continue
            elif timeout:
                tx.rcpt_response[i] = Response(
                    400, 'RestEndpoint upstream timeout RCPT')
            elif i >= len(rcpt_resp):
                done = False
            elif rcpt_resp[i] is not None:
                tx.rcpt_response[i] = rcpt_resp[i]
            else:
                done = False

        if tx.data_response is None and self.data_last:
            if data_resp := Response.from_json(
                    tx_json.get('data_response', {})):
                tx.data_response = data_resp
            elif timeout:
                tx.data_response = Response(
                    400, 'RestEndpoint upstream timeout DATA')
            else:
                done = False

        return done

    # poll/GET the tx until all mail/rcpts in tx have corresponding
    # responses or timeout
    # if you already have tx json from a previous POST/PATCH, pass it
    # in tx_json
    def get_tx_response(self, timeout, tx : TransactionMetadata,
                        tx_json={}, http_err : bool = False):
        logging.debug('get_tx_response %s %s', tx_json, http_err)
        deadline = time.monotonic() + timeout
        prev = 0
        timedout = http_err
        while True:
            if self._update_tx(tx, tx_json, timedout):
                break
            now = time.monotonic()
            deadline_left = deadline - now
            if deadline_left < self.min_poll:
                timedout = True
                continue
            delta = now - prev
            prev = now
            # retry at most once per self.min_poll
            if delta < self.min_poll:
                wait = self.min_poll - delta
                time.sleep(wait)

            tx_json = self.get_json(deadline_left)
            logging.info('RestEndpoint.get_tx_response done %s', tx_json)
            if tx_json is None:
                tx_json = {}
                timedout = True

        return None


    def abort(self):
        # TODO
        pass
