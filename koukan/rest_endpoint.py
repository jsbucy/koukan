# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Generator, Optional, Tuple
from threading import Lock, Condition
import logging
import time
import json.decoder
from urllib.parse import urljoin, urlparse

from httpx import Client, Request, RequestError, Response as HttpResponse
from werkzeug.datastructures import ContentRange
import werkzeug.http

from koukan.deadline import Deadline
from koukan.filter import (
    HostPort,
    Resolution,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.response import Response, Esmtp
from koukan.blob import Blob, BlobReader

from koukan.rest_schema import BlobUri
from koukan.message_builder import MessageBuilderSpec

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

class RestEndpoint(SyncFilter):
    transaction_path : Optional[str] = None
    transaction_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[HostPort] = None
    etag : Optional[str] = None
    max_inline : int
    chunk_size : Optional[int]

    blob_path : Optional[str] = None
    blob_url : Optional[str] = None

    # PATCH sends rcpts to append but it sends back the responses for
    # all rcpts so far, need to remember how many we've sent to know
    # if we have all the responses.
    rcpts = 0

    client : Client

    upstream_tx : Optional[TransactionMetadata] = None
    sent_data_last : bool = False

    static_http_host : Optional[str] = None
    http_host : Optional[str] = None

    def _set_request_timeout(self, headers, timeout : Optional[float] = None):
        if timeout and int(timeout) >= 2:
            # allow for propagation delay
            headers['request-timeout'] = str(int(timeout) - 1)

    # pass base_url/http_host or transaction_url
    def __init__(self,
                 static_base_url=None,
                 static_http_host=None,
                 transaction_url=None,
                 timeout_start=TIMEOUT_START,
                 timeout_data=TIMEOUT_DATA,
                 min_poll=1,
                 max_inline=1024,
                 chunk_size : Optional[int] = None,
                 verify=True):
        self.base_url = static_base_url
        self.static_http_host = static_http_host
        self.transaction_url = transaction_url
        self.timeout_start = timeout_start
        self.timeout_data = timeout_data

        self.min_poll = min_poll
        self.max_inline = max_inline
        self.chunk_size = chunk_size

        self.client = Client(http2=True, verify=verify, follow_redirects=True)

    def __del__(self):
        if self.client:
            logging.debug('RestEndpoint.__del__() client')
            # close keepalive connections, setting Client(limits=)
            # doesn't seem to work? keepalive connections cause
            # hypercorn to take a long time to shut down which is a
            # problem in tests
            self.client.close()
            self.client = None

    # -> full-url, path
    def _maybe_qualify_url(self, url) -> Tuple[str, str]:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return url, parsed.path
        return urljoin(self.base_url, url), url

    def _create(self,
               resolution : Resolution,
               tx : TransactionMetadata,
               deadline : Deadline) -> Optional[HttpResponse]:
        logging.debug('RestEndpoint._create %s %s', resolution, tx)

        hosts = resolution.hosts if resolution is not None else [None]
        # TODO probably tx.rest_endpoint should also be repeated.  But
        # we probably don't want the cross product of endpoints and
        # remote hosts?  Iterate endpoint on http err, remote host on
        # tx err?
        rest_resp = None
        for remote_host in hosts:
            # TODO return last remote_host in tx "upstream_remote_host" etc
            if remote_host is not None:
                tx.remote_host = remote_host
                self.remote_host = remote_host

            json=tx.to_json(WhichJson.REST_CREATE)
            logging.debug('RestEndpoint._create remote_host %s %s %s',
                          self.base_url, remote_host, json)

            req_headers = {}
            if self.http_host:
                req_headers['host'] = self.http_host
            deadline_left = deadline.deadline_left()
            self._set_request_timeout(req_headers, deadline_left)
            try:
                rest_resp = self.client.post(
                    urljoin(self.base_url, '/transactions'),
                    json=json,
                    headers=req_headers,
                    timeout=deadline_left)
            except RequestError:
                logging.exception('RestEndpoint._create')
                continue
            logging.info('RestEndpoint._create req_headers %s resp %s %s',
                         req_headers, rest_resp, rest_resp.text)
            if rest_resp.status_code != 201:
                continue
            location = rest_resp.headers['location']
            self.transaction_url, self.transaction_path = self._maybe_qualify_url(location)
            self.etag = rest_resp.headers.get('etag', None)
            return rest_resp
        return rest_resp

    def _update(self, downstream_delta : TransactionMetadata,
                deadline : Deadline) -> Optional[HttpResponse]:
        body_json = downstream_delta.to_json(WhichJson.REST_UPDATE)
        # verify the not tx.empty() condition in the caller
        assert body_json

        rest_resp = self._post_tx(
            self.transaction_url, body_json, self.client.patch, deadline)
        if rest_resp is None:
            return None
        resp_json = get_resp_json(rest_resp)
        logging.info('RestEndpoint._update resp_json %s', resp_json)
        if resp_json is None:
            return None
        return rest_resp

    def _post_tx(self, uri, body_json : dict,
                 http_method,
                 deadline : Deadline) -> Optional[HttpResponse]:
        logging.debug('RestEndpoint._post_tx %s', uri)
        req_headers = {}
        if self.http_host:
            req_headers['host'] = self.http_host
        deadline_left = deadline.deadline_left()
        self._set_request_timeout(req_headers, deadline_left)
        if self.etag:
            req_headers['if-match'] = self.etag
        try:
            rest_resp = http_method(
                uri,
                json=body_json,
                headers=req_headers,
                timeout=deadline_left)
        except RequestError:
            return None
        logging.info('RestEndpoint._update resp %s %s',
                     rest_resp, rest_resp.http_version)

        if rest_resp.status_code < 300:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
            # xxx err?

        return rest_resp

    def _update_message_builder(self, delta : TransactionMetadata,
                                deadline : Deadline
                                ) -> Optional[Response]:
        assert isinstance(delta.body, MessageBuilderSpec)
        rest_resp = self._post_tx(
            self.transaction_url + '/message_builder',
            delta.body.json, self.client.post, deadline)
        if rest_resp is None or rest_resp.status_code != 200:
            return Response(400, 'RestEndpoint._update_message_builder err')
        return None

    def _cancel(self):
        logging.debug('RestEndpoint._cancel %s ', self.transaction_url)
        if not self.transaction_url:
            return TransactionMetadata(cancelled=True)
        try:
            rest_resp = self.client.post(self.transaction_url + '/cancel')
        except RequestError as e:
            logging.exception('RestEndpoint._cancel POST exception')
        else:
            logging.debug('RestEndpoint._cancel %s %s', self.transaction_url,
                          rest_resp)
        return TransactionMetadata(cancelled=True)

    def on_update(self,
                  tx : TransactionMetadata,
                  tx_delta : TransactionMetadata,
                  timeout : Optional[float] = None
                  ) -> Optional[TransactionMetadata]:
        if tx_delta.cancelled:
            return self._cancel()
        elif tx.cancelled:
            return TransactionMetadata()

        if self.http_host is None and self.transaction_url is None:
            if tx.upstream_http_host:
                self.http_host = tx.upstream_http_host
            elif self.static_http_host:
                self.http_host = self.static_http_host


        # xxx envelope vs data timeout
        if timeout is None:
            timeout = self.timeout_start
        deadline = Deadline(timeout)

        logging.debug('RestEndpoint.on_update start %s '
                      'timeout=%s downstream tx %s',
                      self.transaction_url, timeout, tx)

        downstream_delta = tx_delta.copy()
        if downstream_delta.body:
            del downstream_delta.body
        if downstream_delta.attempt_count:
            del downstream_delta.attempt_count

        # We are going to send a slightly different delta upstream per
        # remote_host (discovery in _create()) and body (below).
        # When we look at the delta of what we got back, these fields
        # should not appear so it will merge cleanly with the original input.
        if self.upstream_tx is None:
            if self.base_url is None:
                self.base_url = tx.rest_endpoint
            self.upstream_tx = tx.copy_valid(WhichJson.REST_CREATE)
        else:
            assert self.upstream_tx.merge_from(downstream_delta) is not None
        # xxx some tests send BlobUri here?
        # otherwise clear so it doesn't trip req_inflight() during get
        if not isinstance(self.upstream_tx.body, BlobUri) and not isinstance(self.upstream_tx.body, MessageBuilderSpec):
            self.upstream_tx.body = None
        upstream_tx = self.upstream_tx.copy()

        logging.debug('RestEndpoint.on_update merged tx %s', self.upstream_tx)

        tx_update = False
        if not self.transaction_url:
            rest_resp = self._create(tx.resolution, self.upstream_tx, deadline)
            if rest_resp is None or rest_resp.status_code != 201:
                # XXX maybe only needs to set mail_response?
                err = TransactionMetadata()
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint upstream err creating tx'), err)
                tx.merge_from(err)
                return err
            tx_update = True
        # the precondition is that the delta isn't empty but it might
        # only contain internal fields
        elif not downstream_delta.empty(WhichJson.REST_UPDATE):
            rest_resp = self._update(downstream_delta, deadline)
            # TODO handle 412 failed precondition
            if rest_resp is None or rest_resp.status_code != 200:
                err_delta = TransactionMetadata()
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint upstream http err'),
                    err_delta)
                tx.merge_from(err_delta)
                return err_delta

            tx_update = True
        elif (tx_delta.body is None or
              not tx_delta.body.finalized()):
            return TransactionMetadata()

        upstream_delta = None
        if tx_update:
            resp_json = get_resp_json(rest_resp) if rest_resp else None
            resp_json = resp_json if resp_json else {}

            logging.debug('RestEndpoint.on_update %s tx from POST/PATCH %s',
                          self.transaction_url, resp_json)

            tx_out = TransactionMetadata.from_json(
                resp_json, WhichJson.REST_READ)

            logging.debug('RestEndpoint.on_update %s tx_out %s',
                          self.transaction_url, tx_out)

            if tx_out is None:
                logging.debug('RestEndpoint.on_update bad resp_json %s',
                              resp_json)
                err_delta = TransactionMetadata()
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint bad resp_json'), err_delta)
                tx.merge_from(err_delta)
                return err_delta

            if self.upstream_tx.req_inflight(tx_out):
                tx_out = self._get(deadline)
            err = None
            if tx_out is None:
                err = 'bad resp GET after POST/PUT'
            elif self.upstream_tx.req_inflight(tx_out):
                err = 'upstream timeout'
            if err:
                err_delta = TransactionMetadata()
                tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint bad resp_json'), err_delta)
                tx.merge_from(err_delta)
                return err_delta

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

        if tx_delta.body is None:
            return upstream_delta

        message_builder = isinstance(tx_delta.body, MessageBuilderSpec)
        if message_builder:
            err = self._update_message_builder(tx_delta, deadline)
            if err is not None:
                delta = TransactionMetadata(data_response = err)
                tx.merge_from(delta)
                return  delta

            if not tx_delta.body.blobs:
                return upstream_delta

        err = None
        # delta/merge bugs in the chain downstream from here have been
        # known to drop response fields on subsequent calls so use
        # upstream_tx, not tx here
        if not any([r.ok() for r in self.upstream_tx.rcpt_response]):
            err = "all rcpts failed"

        if err is not None:
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

        blobs : List[Tuple(Blob, bool)]  # bool: non_body_blob
        if isinstance(tx_delta.body, Blob):
            blobs = [ (tx_delta.body, False) ]
        elif message_builder:
            blobs = [(b, True) for b in tx_delta.body.blobs]
            blobs.append((tx_delta.body.body_blob, False))
        elif isinstance(tx_delta.body, BlobUri):
            return upstream_delta
        else:
            raise ValueError()

        for blob,non_body_blob in blobs:
            put_blob_resp = self._put_blob(blob, non_body_blob=non_body_blob)
            if not put_blob_resp.ok():
                upstream_delta = TransactionMetadata(
                    data_response = put_blob_resp)
                assert tx.merge_from(upstream_delta) is not None
                return upstream_delta
            if non_body_blob:
                self.blob_url = None  # xxx wat?

        self.sent_data_last = True

        tx_out = self._get(deadline)
        logging.debug('RestEndpoint.on_update %s tx from GET %s',
                      self.transaction_url, tx_out)

        # NB this delta/merge is fragile and depends on dropping
        # fields we aren't going to send upstream (above)
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

    # Send a finalized blob with a single http request.
    def _put_blob_single(self, blob : Blob,
                         body : bool,
                         blob_rest_id : Optional[str] = None
                         ) -> Response:
        assert blob.finalized()
        assert not(body and blob_rest_id)
        assert body or blob_rest_id
        method, exp_code = ('PUT', 200) if blob_rest_id else ('POST', 201)
        if blob_rest_id is not None:
            self.blob_path = self.transaction_path + '/blob/' + blob_rest_id
        elif body:
            self.blob_path = self.transaction_path + '/body'
        self.blob_url, self.blob_path = self._maybe_qualify_url(self.blob_path)
        logging.debug('_put_blob_single %d %s',
                      blob.content_length(), self.blob_url)
        headers = {}
        if self.http_host:
            headers['host'] = self.http_host

        req = Request(method, self.blob_url, headers=headers,
                      content = BlobReader(blob))
        rest_resp = None
        try:
            rest_resp = self.client.send(req)
        except RequestError as e:
            logging.info('RestEndpoint._put_blob_single RequestError %s', e)
        if rest_resp is None or rest_resp.status_code != exp_code:
            logging.debug(rest_resp)
            return Response(450, 'RestEndpoint blob upload error')
        logging.info('RestEndpoint._put_blob_single %s', rest_resp)
        return Response()

    def _put_blob(self, blob : Blob, non_body_blob=False) -> Response:
        if self.chunk_size is None and blob.finalized():
            return self._put_blob_single(
                blob, not non_body_blob, blob.rest_id())

        offset = 0
        while offset < blob.len():
            chunk = blob.pread(offset, self.chunk_size)
            chunk_last = (offset + len(chunk) >= blob.len())
            logging.debug('RestEndpoint._put_blob() '
                          'chunk_offset %d chunk len %d '
                          'chunk_last %s',
                          offset, len(chunk), chunk_last)

            resp, result_length = self._put_blob_chunk(
                offset=offset,
                d=chunk,
                last=chunk_last,
                non_body_blob=non_body_blob,
                blob_rest_id=blob.rest_id())
            if resp is None:
                return Response(450, 'RestEndpoint blob upload error')
            elif not resp.ok():
                return resp
            # how many bytes from chunk were actually accepted?
            chunk_out = result_length - offset
            logging.debug('RestEndpoint._put_blob() '
                          'result_length %d chunk_out %d',
                          result_length, chunk_out)
            offset += chunk_out
        return Response()

    # -> (resp, len)
    def _put_blob_chunk(self, offset, d : bytes, last : bool,
                        non_body_blob=False,
                        blob_rest_id : Optional[str] = None
                        ) -> Tuple[Optional[Response], Optional[int]]:
        logging.info('RestEndpoint._put_blob_chunk %d %d %s',
                     offset, len(d), last)
        headers = {}
        if self.http_host:
            headers['host'] = self.http_host
        try:
            if self.blob_url is None:
                if blob_rest_id is not None:
                    endpoint = self.transaction_url + '/blob/' + blob_rest_id
                elif not non_body_blob:
                    self.blob_path = self.transaction_path + '/body'
                    self.blob_url, self.blob_path = self._maybe_qualify_url(self.blob_path)
                    endpoint = self.blob_url
                else:
                    endpoint = self.transaction_url + '/blob'

                entity = d
                if not last:
                    endpoint += '?upload=chunked'
                    entity = None

                logging.info('RestEndpoint._put_blob_chunk POST %s', endpoint)
                if non_body_blob:
                    method, exp_code = (self.client.put, 200)
                else:
                    method, exp_code = (self.client.post, 201)
                rest_resp = method(
                    urljoin(self.base_url, endpoint), headers=headers,
                    content=entity, timeout=self.timeout_data)
                logging.info('RestEndpoint._put_blob_chunk POST %s %s %s',
                             endpoint, rest_resp, rest_resp.headers)
                if rest_resp.status_code != exp_code:
                    return None, None
                if blob_rest_id is None and non_body_blob:
                    self.blob_path = rest_resp.headers.get('location', None)
                    if not self.blob_path:
                        return None, None
                    self.blob_url, self.blob_path = self._maybe_qualify_url(self.blob_path)
                if not last:  # i.e. chunked upload
                    return Response(), 0
            else:
                range = ContentRange('bytes', offset, offset + len(d),
                                     offset + len(d) if last else None)
                logging.info('RestEndpoint._put_blob_chunk() PUT %s %s',
                             self.blob_url, range)
                headers['content-range'] = range.to_header()
                logging.info('RestEndpoint._put_blob_chunk PUT %s',
                             self.blob_url)
                rest_resp = self.client.put(
                    self.blob_url, headers=headers, content=d,
                    timeout=self.timeout_data)
                logging.info('RestEndpoint._put_blob_chunk PUT %s %s %s',
                             self.blob_url, rest_resp, rest_resp.headers)
                if rest_resp.status_code not in [200, 416]:
                    return Response(
                        450, 'RestEndpoint._put_blob_chunk PUT err'), None
        except RequestError as e:
            logging.info('RestEndpoint._put_blob_chunk RequestError %s', e)
            return None, None
        logging.info('RestEndpoint._put_blob_chunk %s', rest_resp)

        # Most(all?) errors on blob put here means temp
        # transaction final status
        # IOW blob upload is not the place to reject the content, etc.

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

    def _get_json(self, timeout : Optional[float] = None,
                  testonly_point_read = False
                 ) -> Optional[dict]:
        try:
            req_headers = {}
            if self.http_host:
                req_headers['host'] = self.http_host
            if self.etag and not testonly_point_read:
                req_headers['if-none-match'] = self.etag
            self._set_request_timeout(req_headers, timeout)
            rest_resp = self.client.get(self.transaction_url,
                                        headers=req_headers,
                                        timeout=timeout)
            logging.debug('RestEndpoint.get_json %s %s', rest_resp, [r.headers for r in rest_resp.history])
        except RequestError as e:
            logging.debug('RestEndpoint.get_json() timeout %s',
                          self.transaction_url)
            return None
        if rest_resp.status_code in [200, 304]:
            self.etag = rest_resp.headers.get('etag', None)
        else:
            self.etag = None
        if rest_resp.status_code != 200:
            return None
        return get_resp_json(rest_resp)

    # test only
    def get_json(self, timeout : Optional[float] = None
                 ) -> Optional[dict]:
        return self._get_json(timeout, testonly_point_read=True)

    # does GET /tx at least once, polls as long as tx contains
    # inflight reqs, returns the last tx it successfully retrieved
    def _get(self, deadline : Deadline) -> Optional[TransactionMetadata]:
        tx_out = None
        while deadline.remaining():
            logging.debug('RestEndpoint._get() %s %s',
                          self.transaction_url, deadline.deadline_left())
            start = time.monotonic()
            prev_etag = self.etag
            tx_json = self._get_json(timeout=deadline.deadline_left())
            delta = time.monotonic() - start
            logging.debug('RestEndpoint._get() %s done %s',
                          self.transaction_url, tx_json)

            # TODO this should abort on some http errs? 4xx?

            if tx_json is not None:
                tx_out = TransactionMetadata.from_json(
                    tx_json, WhichJson.REST_READ)

            logging.debug('RestEndpoint._get() %s done tx_out %s',
                          self.transaction_url, tx_out)

            if (tx_out is not None) and (
                    not self.upstream_tx.req_inflight(tx_out) and not
                    (self.sent_data_last and tx_out.data_response is None)):
                return tx_out

            # min delta
            # XXX configurable
            # XXX backoff?
            if not deadline.remaining(1):
                break
            if (self.etag is None or self.etag == prev_etag) and (delta < 1):
                nospin = 1 - delta
                logging.debug('nospin %s %f', self.transaction_url, nospin)
                time.sleep(nospin)
        return tx_out
