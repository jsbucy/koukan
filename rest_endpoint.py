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
from filter import (
    HostPort,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
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

class RestEndpoint(SyncFilter):
    transaction_path : Optional[str] = None
    transaction_url : Optional[str] = None
    static_base_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[HostPort] = None
    etag : Optional[str] = None
    max_inline : int
    chunk_size : int
    body_len : int = 0

    blob_path : Optional[str] = None
    blob_url : Optional[str] = None

    # PATCH sends rcpts to append but it sends back the responses for
    # all rcpts so far, need to remember how many we've sent to know
    # if we have all the responses.
    rcpts = 0

    client : Client

    upstream_tx : Optional[TransactionMetadata] = None

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
                deadline : Deadline) -> Optional[HttpResponse]:
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
            self.transaction_path = rest_resp.headers['location']
            self.transaction_url = self._maybe_qualify_url(
                rest_resp.headers['location'])
            self.etag = rest_resp.headers.get('etag', None)
            return rest_resp

    def _update(self, downstream_delta : TransactionMetadata,
                deadline : Deadline) -> Optional[HttpResponse]:
        req_headers = {}
        if self.http_host:
            req_headers['host'] = self.http_host
        deadline_left = deadline.deadline_left()
        self._set_request_timeout(req_headers, deadline_left)
        if self.etag:
            req_headers['if-match'] = self.etag
        json = downstream_delta.to_json(WhichJson.REST_UPDATE)
        if json == {}:
            return HttpResponse(status_code=200)
        try:
            rest_resp = self.client.patch(
                self.transaction_url,
                json=downstream_delta.to_json(WhichJson.REST_UPDATE),
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

    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata,
                  timeout : Optional[float] = None
                  ) -> Optional[TransactionMetadata]:
        # xxx envelope vs data timeout
        if timeout is None:
            timeout = self.timeout_start
        deadline = Deadline(timeout)
        data_last = tx_delta.body_blob is not None and (
            tx_delta.body_blob.finalized())

        logging.debug('RestEndpoint.on_update start %s '
                      ' data_last=%s timeout=%s downstream tx %s',
                      self.transaction_url, data_last, timeout, tx)

        downstream_delta = tx_delta.copy()
        if downstream_delta.body_blob:
            del downstream_delta.body_blob

        # We are going to send a slightly different delta upstream per
        # remote_host (discovery in _start()) and body_blob (below).
        # When we look at the delta of what we got back, these fields
        # should not appear so it will merge cleanly with the original input.
        if self.upstream_tx is None:
            self.upstream_tx = tx.copy_valid(WhichJson.REST_CREATE)
        else:
            assert self.upstream_tx.merge_from(downstream_delta) is not None
        upstream_tx = self.upstream_tx.copy()

        logging.debug('RestEndpoint.on_update merged tx %s', self.upstream_tx)

        tx_update = False
        if not self.transaction_url:
            which_js = WhichJson.REST_CREATE
            rest_resp = self._start(self.upstream_tx, deadline)
            if rest_resp is None or rest_resp.status_code != 201:
                # XXX maybe only needs to set mail_response?
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint upstream err creating tx'))
                return
            tx_update = True
        elif downstream_delta:
            which_js = WhichJson.REST_UPDATE
            rest_resp = self._update(downstream_delta, deadline)
            # TODO handle 412 failed precondition
            tx_update = True
        elif not data_last:
            return TransactionMetadata()

        upstream_delta = None
        if tx_update:
            resp_json = get_resp_json(rest_resp) if rest_resp else None
            resp_json = resp_json if resp_json else {}

            logging.debug('RestEndpoint.on_update %s tx from POST/PATCH %s',
                          self.transaction_url, resp_json)

            tx_out = TransactionMetadata.from_json(
                resp_json, WhichJson.REST_READ)

            if tx_out is None:
                logging.debug('RestEndpoint.on_update bad resp_json %s',
                              resp_json)
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint bad resp_json'))
                return

            if self.upstream_tx.req_inflight(tx_out):
                tx_out = self._get(deadline)
            err = None
            if tx_out is None:
                err = 'bad resp GET after POST/PUT'
            elif self.upstream_tx.req_inflight(tx_out):
                err = 'upstream timeout'
            if err:
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint bad resp_json'))
                return

            upstream_delta = self.upstream_tx.delta(tx_out, WhichJson.REST_READ)
            if (upstream_delta is None or
                (self.upstream_tx.merge_from(upstream_delta) is None) or
                (tx.merge_from(upstream_delta) is None)):
                errs = TransactionMetadata()
                tx.fill_inflight_responses(
                    Response(450,
                             'RestEndpoint upstream invalid resp/delta update'),
                    errs)
                assert tx.merge_from(errs) is not None
                return errs

        if tx.body_blob is None:
            return upstream_delta

        err = None
        if not any([r.ok() for r in tx.rcpt_response]):
            err = "all rcpts failed"

        if err is not None:
            # TODO cancel upstream
            # POST /tx/123/cancel etc.
            data_err = TransactionMetadata(
                data_response=Response(
                    400, "data failed precondition: " + err +
                    " (RestEndpoint)"))
            if upstream_delta is None:
                upstream_delta = data_err
            else:
                assert upstream_delta.merge_from(data_err) is not None
            assert tx.merge_from(data_err) is not None
            return upstream_delta

        put_blob_resp = None
        if data_last:
            put_blob_resp = self._put_blob(tx_delta.body_blob)
            logging.debug('RestEndpoint.on_update() put_blob_resp %s',
                          put_blob_resp)
            if not put_blob_resp.ok():
                upstream_delta = TransactionMetadata(
                    data_response = put_blob_resp)
                assert tx.merge_from(upstream_delta) is not None
                return upstream_delta

        tx_out = self._get(deadline)
        logging.debug('RestEndpoint.on_update %s tx from GET %s',
                      self.transaction_url, tx_out)

        if (tx_out is None or
            (blob_delta := self.upstream_tx.delta(
                tx_out, WhichJson.REST_READ)) is None or
            (self.upstream_tx.merge_from(blob_delta) is None) or
            (tx.merge_from(blob_delta) is None)):
            errs = TransactionMetadata()
            tx.fill_inflight_responses(
                Response(450, 'RestEndpoint upstream invalid resp/delta get'),
                errs)
            assert tx.merge_from(errs) is not None
            return errs


        errs = TransactionMetadata()
        tx.fill_inflight_responses(
            Response(450, 'RestEndpoint upstream timeout'), errs)
        assert tx.merge_from(errs) is not None
        del upstream_tx.remote_host
        del tx_out.remote_host
        upstream_delta = upstream_tx.delta(tx_out, WhichJson.REST_READ)
        assert upstream_delta is not None
        assert upstream_delta.merge_from(errs) is not None
        return upstream_delta

    def _put_blob(self, blob,
                  testonly_non_body_blob=False) -> Response:
        offset = 0
        while offset < blob.len():
            chunk = blob.read(offset, self.chunk_size)
            chunk_last = (offset + len(chunk) >= blob.len())
            logging.debug('RestEndpoint._put_blob() '
                          'chunk_offset %d chunk len %d '
                          'body_len %d chunk_last %s',
                          offset, len(chunk), self.body_len, chunk_last)

            resp, result_length = self._put_blob_chunk(
                offset=self.body_len,
                d=chunk,
                last=chunk_last,
                testonly_non_body_blob=testonly_non_body_blob)
            if resp is None:
                return Response(450, 'RestEndpoint blob upload error')
            elif not resp.ok():
                return resp
            # how many bytes from chunk were actually accepted?
            chunk_out = result_length - self.body_len
            logging.debug('RestEndpoint._put_blob() '
                          'result_length %d chunk_out %d',
                          result_length, chunk_out)
            offset += chunk_out
            self.body_len += chunk_out
        return Response()

    # -> (resp, len)
    def _put_blob_chunk(self, offset, d : bytes, last : bool,
                        testonly_non_body_blob=False
                        ) -> Tuple[Optional[Response], Optional[int]]:
        logging.info('RestEndpoint._put_blob_chunk %d %d %s',
                     offset, len(d), last)
        headers = {}
        if self.http_host:
            headers['host'] = self.http_host
        try:
            if self.blob_url is None:
                if not testonly_non_body_blob:
                    self.blob_path = self.transaction_path + '/body'
                    self.blob_url = self._maybe_qualify_url(self.blob_path)
                    endpoint = self.blob_url
                else:
                    endpoint = self.transaction_url + '/blob'

                entity = d
                if not last:
                    endpoint += '?upload=chunked'
                    entity = None

                logging.info('RestEndpoint._put_blob_chunk POST %s', endpoint)
                rest_resp = self.client.post(
                    urljoin(self.base_url, endpoint), headers=headers,
                    content=entity, timeout=self.timeout_data)
                logging.info('RestEndpoint._put_blob_chunk POST %s %s %s',
                             endpoint,
                             rest_resp, rest_resp.headers)
                if rest_resp.status_code != 201:
                    return None, None
                if testonly_non_body_blob:
                    self.blob_path = rest_resp.headers.get('location', None)
                    if not self.blob_path:
                        return None, None
                    self.blob_url = self._maybe_qualify_url(self.blob_path)
                if not last:  # i.e. chunked upload
                    return Response(), 0
            else:
                range = ContentRange('bytes', offset, offset + len(d),
                                     offset + len(d) if last else None)
                logging.info('RestEndpoint._put_blob_chunk() PUT %s %s',
                             self.blob_url, range)
                headers['content-range'] = range.to_header()
                logging.info('RestEndpoint._put_blob_chunk PUT %s', self.blob_url)
                rest_resp = self.client.put(
                    self.blob_url, headers=headers, content=d,
                    timeout=self.timeout_data)
                logging.info('RestEndpoint._put_blob_chunk PUT %s %s %s',
                             self.blob_url, rest_resp, rest_resp.headers)
        except RequestError as e:
            logging.info('RestEndpoint._put_blob_chunk RequestError %s', e)
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

            if (tx_out is not None) and (
                    not self.upstream_tx.req_inflight(tx_out)):
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
