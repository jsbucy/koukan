from typing import Any, Callable, Dict, Generator, Optional, Tuple
from threading import Lock, Condition
import logging
import time
import json.decoder
from urllib.parse import urljoin, urlparse

from httpx import Client, RequestError, Response as HttpResponse
from werkzeug.datastructures import ContentRange
import werkzeug.http

from filter import Filter, HostPort, TransactionMetadata, WhichJson
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

Resolution = Callable[[HostPort], Generator[HostPort, None, Any]]

def identity_resolution(x):
    yield x

def constant_resolution(x):
  return lambda ignored: identity_resolution(x)

class RestEndpoint(Filter):
    transaction_url : Optional[str] = None
    static_base_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[HostPort] = None
    etag : Optional[str] = None
    max_inline : int
    chunk_size : int
    body_len : int = 0

    blob_url : Optional[str] = None
    full_blob_url : Optional[str] = None

    # PATCH sends rcpts to append but it sends back the responses for
    # all rcpts so far, need to remember how many we've sent to know
    # if we have all the responses.
    rcpts = 0

    # true if we've sent all of the data upstream and should wait for
    # the data_response
    data_last = False

    client : Client

    def _set_request_timeout(self, headers, timeout : Optional[float] = None):
        if timeout and int(timeout):
            headers['request-timeout'] = str(int(timeout))

    # pass base_url/http_host or transaction_url
    def __init__(self,
                 static_base_url=None,
                 http_host=None,
                 transaction_url=None,
                 timeout_start=TIMEOUT_START,
                 timeout_data=TIMEOUT_DATA,
                 remote_host_resolution : Resolution = identity_resolution,
                 min_poll=1,
                 max_inline=1024,
                 chunk_size=1048576,
                 verify=True):
        self.base_url = self.static_base_url = static_base_url
        self.http_host = http_host
        self.transaction_url = transaction_url
        self.timeout_start = timeout_start
        self.timeout_data = timeout_data

        self.remote_host_resolution = remote_host_resolution

        self.min_poll = min_poll
        self.max_inline = max_inline
        self.chunk_size = chunk_size

        self.client = Client(http2=True, verify=verify)

    def _maybe_qualify_url(self, url):
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return url
        return urljoin(self.base_url, url)

    def _start(self, tx : TransactionMetadata, req_json : Dict[Any,Any],
               timeout : Optional[float] = None):
        logging.debug('RestEndpoint._start %s', req_json)

        for remote_host in self.remote_host_resolution(tx.remote_host):
            logging.debug('RestEndpoint._start remote_host %s %s',
                          self.base_url, remote_host)
            # none if no remote_host/disco (above)
            if remote_host is not None:
                req_json['remote_host'] = remote_host.to_tuple()
                self.remote_host = remote_host

            req_headers = {}
            if self.http_host:
                req_headers['host'] = self.http_host
            self._set_request_timeout(req_headers, timeout)
            try:
                rest_resp = self.client.post(
                    urljoin(self.base_url, '/transactions'),
                    json=req_json,
                    headers=req_headers,
                    timeout=self.timeout_start)
            except RequestError:
                return None
            if rest_resp.status_code != 201:
                return rest_resp
            logging.info('RestEndpoint.start resp %s %s',
                         rest_resp, rest_resp.http_version)
            self.transaction_url = self._maybe_qualify_url(
                rest_resp.headers['location'])
            self.etag = rest_resp.headers.get('etag', None)
            return rest_resp

    def _update(self, req_json, timeout : Optional[float] = None
                ) -> HttpResponse:
        req_headers = {}
        if self.http_host:
            req_headers['host'] = self.http_host
        self._set_request_timeout(req_headers, timeout)
        if self.etag:
            req_headers['if-match'] = self.etag
        try:
            rest_resp = self.client.patch(self.transaction_url,
                                       json=req_json,
                                       headers=req_headers,
                                       timeout=self.timeout_start)
        except RequestError:
            return None
        logging.info('RestEndpoint._update resp %s %s',
                     rest_resp, rest_resp.http_version)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint._update resp_json %s', resp_json)
        if not resp_json:
            return None

        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
            # xxx err?
        return rest_resp

    # NOTE/TODO: RestEndpoint is used as a rest submission client in
    # some tests (at least router_service_test). Rest submission has
    # weaker preconditions i.e. can send data as long as any rcpt
    # hasn't permfailed. Contrast with:
    # SmtpService: relays through Exploder which masks upstream temp errors
    # OutputHandler: is structured to not send the data until at least
    # one upstream rcpt has succeeded.
    # It might be good for this code to enforce more of these
    # preconditions to detect bugs in calling code but it seesm like
    # it would have to have multiple modes (at least submission vs
    # relay) which may be more trouble than it's worth...
    def on_update(self, tx : TransactionMetadata,
                  timeout : Optional[float] = None):
        which_json = (WhichJson.REST_CREATE if not self.transaction_url
                      else WhichJson.REST_UPDATE)
        req_json = tx.to_json(which_json)

        self.data_last = tx.body_blob is not None and (
            tx.body_blob.len() == tx.body_blob.content_length())

        logging.debug('RestEndpoint.on_update '
                      ' self.data_last=%s', self.data_last)

        put_blob_resp = None
        if self.data_last:
            logging.debug('RestEndpoint._update_once body_blob')
            put_blob_resp = self._put_blob(tx.body_blob)
            logging.debug('RestEndpoint._update_once put_blob_resp %s',
                          put_blob_resp)
            if put_blob_resp.ok():
                req_json['body'] = self.blob_url
            else:
                tx.data_response = put_blob_resp

        logging.debug('RestEndpoint._update_once req_json %s', req_json)

        if not req_json:
            return False

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
        # xxx filter api needs to accomodate returning an error here
        # or else stuff the http error in the response for the first
        # inflight req field in tx?
        resp_json = get_resp_json(rest_resp) if rest_resp else None
        http_err = (rest_resp is None or rest_resp.status_code >= 300 or
                    resp_json is None)
        resp_json = resp_json if resp_json else {}
        self.get_tx_response(timeout, tx, resp_json, http_err)


    def _put_blob(self, blob) -> Response:
        offset = 0
        while offset < blob.len():
            chunk = blob.read(offset, self.chunk_size)
            chunk_last = (offset + len(chunk) >= blob.len())
            logging.debug('_put_blob chunk_offset %d chunk len %d '
                          'body_len %d chunk_last %s',
                          offset, len(chunk), self.body_len, chunk_last)

            resp, result_length = self._put_blob_chunk(
                offset=self.body_len,
                d=chunk,
                last=chunk_last)
            if resp is None:
                return Response(400, 'RestEndpoint blob upload error')
            elif not resp.ok():
                return resp
            # how many bytes from chunk were actually accepted?
            chunk_out = result_length - self.body_len
            logging.debug('_put_blob result_length %d chunk_out %d',
                          result_length, chunk_out)
            offset += chunk_out
            self.body_len += chunk_out
        logging.debug('_put_blob %s', resp)
        return Response()

    # -> (resp, len)
    def _put_blob_chunk(self, offset, d : bytes, last : bool
                        ) -> Tuple[Optional[Response], Optional[int]]:
        logging.info('RestEndpoint._put_blob_chunk %d %d %s',
                     offset, len(d), last)
        headers = {}
        if self.http_host:
            headers['host'] = self.http_host
        try:
            if self.blob_url is None:
                endpoint = '/blob'
                entity = d
                if not last:
                    endpoint += '?upload=chunked'
                    entity = None
                rest_resp = self.client.post(
                    urljoin(self.base_url, endpoint), headers=headers,
                    content=entity, timeout=self.timeout_data)
                logging.info('RestEndpoint._put_blob POST /blob %s %s %s',
                             rest_resp, rest_resp.headers,
                             rest_resp.http_version)
                if rest_resp.status_code != 201:
                    return None, None
                self.blob_url = rest_resp.headers.get('location', None)
                if not self.blob_url:
                    return None, None
                self.full_blob_url = self._maybe_qualify_url(self.blob_url)
                if not last:  # i.e. chunked upload
                    return Response(), 0
            else:
                range = ContentRange('bytes', offset, offset + len(d),
                                     offset + len(d) if last else None)
                headers['content-range'] = range.to_header()
                rest_resp = self.client.put(
                    self.full_blob_url, headers=headers, content=d,
                    timeout=self.timeout_data)
        except RequestError:
            return None, None
        logging.info('RestEndpoint._put_blob_chunk %s', rest_resp)

        # Most(all?) errors on blob put here means temp
        # transaction final status
        # IOW blob upload is not the place to reject the content, etc.

        if rest_resp.status_code > 299 and rest_resp.status_code != 416:
            return Response(
                400, 'RestEndpoint._put_blob_chunk PUT err'), None

        # cf RestTransactionHandler.build_resp() re content-range
        dlen = len(d)
        if 'content-range' in rest_resp.headers:
            range = werkzeug.http.parse_content_range_header(
                rest_resp.headers.get('content-range'))
            logging.debug('_put_blob_chunk resp range %s', range)
            # range.stop != offset + dlen is expected in some cases
            # e.g. after a 416 to report the current length to resync
            if range is None or range.start != 0:
                return Response(
                    400, 'RestEndpoint._put_blob_chunk bad range'), None
            dlen = range.stop

        return Response(), dlen


    def get_json(self, timeout : Optional[float] = None):
        try:
            req_headers = {}
            if self.http_host:
                req_headers['host'] = self.http_host
            self._set_request_timeout(req_headers, timeout)
            rest_resp = self.client.get(self.transaction_url,
                                     headers=req_headers,
                                     timeout=timeout)
            logging.debug('RestEndpoint.get_json %s', rest_resp)
        except RequestError:
            return None
        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
        return get_resp_json(rest_resp)


    # TODO the following can probably be considerably simplified by
    # the TransactionMetadata delta/merge strategy used by SyncFilterAdapter

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
