from typing import Any, Callable, Dict, Generator, Optional, Tuple
from threading import Lock, Condition
import logging
import time
import json.decoder
from urllib.parse import urljoin, urlparse

from httpx import Client, RequestError, Response as HttpResponse
from werkzeug.datastructures import ContentRange
import werkzeug.http

from deadline import Deadline
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

    last_tx : Optional[TransactionMetadata] = None

    def _set_request_timeout(self, headers, timeout : Optional[float] = None):
        if timeout and int(timeout) >= 2:
            # allow for propagation delay
            headers['request-timeout'] = str(int(timeout) - 1)

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

    def _start(self, tx : TransactionMetadata,
                deadline : Deadline) -> HttpResponse:
        logging.debug('RestEndpoint._start %s', tx)

        for remote_host in self.remote_host_resolution(tx.remote_host):
            logging.debug('RestEndpoint._start remote_host %s %s',
                          self.base_url, remote_host)
            # none if no remote_host/disco (above)
            if remote_host is not None:
                tx.remote_host = remote_host
                self.remote_host = remote_host

            req_headers = {}
            if self.http_host:
                req_headers['host'] = self.http_host
            deadline_left = deadline.deadline_left()
            self._set_request_timeout(req_headers, deadline_left)
            try:
                rest_resp = self.client.post(
                    urljoin(self.base_url, '/transactions'),
                    json=tx.to_json(WhichJson.REST_CREATE),
                    headers=req_headers,
                    timeout=deadline_left)
            except RequestError:
                return None
            if rest_resp.status_code != 201:
                return rest_resp
            logging.info('RestEndpoint._start resp %s %s',
                         rest_resp, rest_resp.http_version)
            self.transaction_url = self._maybe_qualify_url(
                rest_resp.headers['location'])
            self.etag = rest_resp.headers.get('etag', None)
            return rest_resp

    def _update(self, tx : TransactionMetadata,
                deadline : Deadline) -> HttpResponse:
        req_headers = {}
        if self.http_host:
            req_headers['host'] = self.http_host
        deadline_left = deadline.deadline_left()
        self._set_request_timeout(req_headers, deadline_left)
        if self.etag:
            req_headers['if-match'] = self.etag
        json = tx.to_json(WhichJson.REST_UPDATE)
        if json == {}:
            return HttpResponse(status_code=200)
        try:
            rest_resp = self.client.patch(
                self.transaction_url,
                json=tx.to_json(WhichJson.REST_UPDATE),
                headers=req_headers,
                timeout=deadline_left)
        except RequestError:
            return None
        logging.info('RestEndpoint._update resp %s %s',
                     rest_resp, rest_resp.http_version)
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint._update resp_json %s', resp_json)
        if resp_json is None:
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
        # xxx envelope vs data timeout
        if timeout is None:
            timeout = self.timeout_start
        deadline = Deadline(timeout)
        self.data_last = tx.body_blob is not None and (
            tx.body_blob.len() == tx.body_blob.content_length())

        logging.debug('RestEndpoint.on_update %s %s '
                      ' self.data_last=%s timeout=%s',
                      self.transaction_url, tx,
                      self.data_last, timeout)

        # We are going to send a slightly different delta upstream per
        # remote_host (discovery in _start()) and body_blob (below).
        # When we look at the delta of what we got back, these fields
        # should not appear so it will merge cleanly with the original input.
        upstream_tx = tx.copy()

        put_blob_resp = None
        if self.data_last:
            logging.debug('RestEndpoint.on_update body_blob')
            put_blob_resp = self._put_blob(tx.body_blob)
            logging.debug('RestEndpoint.on_update put_blob_resp %s',
                          put_blob_resp)
            if put_blob_resp.ok():
                upstream_tx.body = self.blob_url
            else:
                tx.data_response = put_blob_resp

        logging.debug('RestEndpoint.on_update req_json %s', upstream_tx)

        if not self.transaction_url:
            which_js = WhichJson.REST_CREATE
            rest_resp = self._start(upstream_tx, deadline)
            self.last_tx = upstream_tx.copy()
            if rest_resp is None or rest_resp.status_code != 201:
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint upstream err creating tx'))
                return
        else:
            assert self.last_tx.merge_from(upstream_tx) is not None
            which_js = WhichJson.REST_UPDATE
            rest_resp = self._update(upstream_tx, deadline)
            # TODO handle 412 failed precondition

        resp_json = get_resp_json(rest_resp) if rest_resp else None
        resp_json = resp_json if resp_json else {}

        tx_out = TransactionMetadata.from_json(resp_json, WhichJson.REST_READ)
        if tx_out is None:
            logging.debug('RestEndpoint.on_update bad resp_json %s', resp_json)
            tx.fill_inflight_responses(
                Response(450, 'RestEndpoint bad resp_json'))
            return

        logging.debug('RestEndpoint.on_update %s tx_out %s',
                      self.transaction_url, resp_json)
        resps = self.last_tx.delta(tx_out, WhichJson.REST_READ)
        logging.debug('RestEndpoint.on_update %s resps %s',
                      self.transaction_url, resps)
        if (resps is None or
            (self.last_tx.merge_from(resps) is None) or
            (tx.merge_from(resps) is None)):
            tx.fill_inflight_responses(
                Response(450,
                         'RestEndpoint upstream invalid resp/delta update'))
            return
        if not tx.req_inflight():
            return


        tx_out = self._get(deadline)
        logging.debug('RestEndpoint.on_update %s after _get() last_tx %s',
                      self.transaction_url, self.last_tx)
        logging.debug('RestEndpoint.on_update %s after _get() tx_out %s',
                      self.transaction_url, tx_out)

        if (tx_out is None or
            (resps := self.last_tx.delta(tx_out, WhichJson.REST_READ)) is None or
            (self.last_tx.merge_from(resps) is None) or
            (tx.merge_from(resps) is None)):
            tx.fill_inflight_responses(
                Response(450, 'RestEndpoint upstream invalid resp/delta get'))
            return

        logging.debug('RestEndpoint.on_update %s tx almost done %s',
                      self.transaction_url, tx)
        tx.fill_inflight_responses(
            Response(450, 'RestEndpoint upstream timeout'))


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
                return Response(450, 'RestEndpoint blob upload error')
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
                450, 'RestEndpoint._put_blob_chunk PUT err'), None

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
                    450, 'RestEndpoint._put_blob_chunk bad range'), None
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
        except RequestError as e:
            logging.debug('RestEndpoint.get_json() timeout %s',
                          self.transaction_url)
            return None
        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
        return get_resp_json(rest_resp)

    # does GET /tx at least once, polls as long as tx contains
    # inflight reqs, returns the last tx it successfully retrieved
    def _get(self, deadline : Deadline) -> Optional[TransactionMetadata]:
        tx_out = None
        while deadline.remaining():
            logging.debug('RestEndpoint._get() %s %s',
                          self.transaction_url, deadline.deadline_left())

            json = self.get_json(timeout=deadline.deadline_left())
            logging.debug('RestEndpoint._get() %s done %s',
                          self.transaction_url, json)

            if json is not None:
                tx_out = TransactionMetadata.from_json(
                    json, WhichJson.REST_READ)

            if (tx_out is not None) and (not self.last_tx.req_inflight(tx_out)):
                logging.debug('RestEndpoint._get() %s done %s',
                              self.transaction_url, tx_out)
                return tx_out
            logging.debug('RestEndpoint._get() %s inflight %s',
                          self.transaction_url, tx_out)

            # min delta
            # XXX configurable
            # XXX backoff?
            if not deadline.remaining(1):
                break
            time.sleep(1)
        return tx_out
