# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union )
from threading import Lock, Condition
import logging
import time
import json.decoder
import copy
from urllib.parse import urljoin, urlparse
import enum

from httpx import Client, Request, RequestError, Response as HttpResponse
from werkzeug.datastructures import ContentRange
import werkzeug.http

from koukan.deadline import Deadline
from koukan.filter import (
    HostPort,
    Resolution,
    TransactionMetadata,
    WhichJson )
from koukan.filter_chain import FilterResult, Filter
from koukan.response import Response, Esmtp
from koukan.blob import Blob, BlobReader

from koukan.message_builder import MessageBuilderSpec
from koukan.storage_schema import BlobSpec
from koukan.rest_schema import FINALIZE_BLOB_HEADER, BlobUri

# these are artificially low for testing
TIMEOUT_START=5
TIMEOUT_DATA=5

class _Unchanged(enum.Enum):
    UNCHANGED = object()

# TODO maybe distinguish empty resp.content vs wrong content-type/invalid json?
def get_resp_json(resp):
    if (resp.headers.get('content-type', None) != 'application/json' or
        not resp.content):
        return None
    try:
        return resp.json()
    except json.decoder.JSONDecodeError:
        return None

class RestEndpointClientProvider:
    client : Optional[Client] = None
    def __init__(self, **kwargs):
        self.client_args = kwargs
    def get(self):
        if self.client is None:
            self.client = Client(
                http2=True, follow_redirects=True, **self.client_args)
        return self.client

    def close(self):
        if self.client:
            logging.debug('RestEndpointClientProvider.__del__() client')
            # close keepalive connections, setting Client(limits=)
            # doesn't seem to work? keepalive connections cause
            # the http server to take a long time to shut down which is a
            # problem in tests
            self.client.close()
            self.client = None

    def __del__(self):
        self.close()

class RestEndpoint(Filter):
    transaction_url : Optional[str] = None
    base_url : Optional[str] = None
    remote_host : Optional[HostPort] = None
    etag : Optional[str] = None
    max_inline : int
    chunk_size : Optional[int]

    client : Client

    rest_upstream_tx : Optional[TransactionMetadata] = None

    static_http_host : Optional[str] = None
    http_host : Optional[str] = None

    blob_readers : Dict[Blob, BlobReader]

    def _set_request_timeout(self, headers, timeout : Optional[float] = None):
        if timeout and int(timeout) >= 2:
            # allow for propagation delay
            headers['request-timeout'] = str(int(timeout) - 1)

    # pass base_url/http_host or transaction_url
    def __init__(self,
                 client_provider : RestEndpointClientProvider,
                 static_base_url=None,
                 static_http_host=None,
                 transaction_url=None,
                 timeout_start=TIMEOUT_START,
                 timeout_data=TIMEOUT_DATA,
                 min_poll=1,
                 max_inline=1024,
                 chunk_size : Optional[int] = None):
        self.base_url = static_base_url
        self.static_http_host = static_http_host
        self.transaction_url = transaction_url
        self.timeout_start = timeout_start
        self.timeout_data = timeout_data

        self.min_poll = min_poll
        self.max_inline = max_inline
        self.chunk_size = chunk_size

        self.client = client_provider.get()
        self.blob_readers = {}

    def _merge_upstream_tx(self, rest_resp : HttpResponse
                           ) -> Optional[TransactionMetadata]:
        assert self.rest_upstream_tx is not None
        assert self.downstream_tx is not None
        resp_json = get_resp_json(rest_resp)
        if resp_json is None:
            return None
        upstream_tx = TransactionMetadata.from_json(
            resp_json, WhichJson.REST_READ)
        if upstream_tx is None:
            return None
        upstream_delta = self.rest_upstream_tx.delta(upstream_tx, WhichJson.REST_READ)
        if upstream_delta is None:
            return None
        assert self.rest_upstream_tx.merge_from(upstream_delta) is not None
        self.upstream_body = self.rest_upstream_tx.body
        # if downstream_body is None:
        #   # so this doesn't spoof req_inflight()
        #   self.rest_upstream_tx.body = None

        delta = upstream_delta.copy()
        delta.body = None
        if self.downstream_tx.merge_from(delta) is None:
            logging.debug('bad')
            return None
        return upstream_delta

    def _create(self,
                resolution : Optional[Resolution],
                tx : TransactionMetadata,
                deadline : Deadline) -> Optional[TransactionMetadata]:
        assert self.base_url is not None
        assert self.downstream_tx is not None

        logging.debug('RestEndpoint._create %s %s', resolution, tx)

        hosts : Sequence[Optional[HostPort]] = [None]
        if resolution is not None and resolution.hosts is not None:
            hosts = resolution.hosts
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

            req_headers = {'content-type': 'application/json'}
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
            except RequestError as e:
                logging.debug('RestEndpoint._create http error %s', e)
                continue
            logging.info('RestEndpoint._create req_headers %s resp %s %s %s',
                         req_headers, rest_resp, rest_resp.headers, rest_resp.text)
            if rest_resp.status_code != 201:
                continue
            self.transaction_url = rest_resp.headers['location']
            if 'etag' not in rest_resp.headers:
                return None
            self.etag = rest_resp.headers['etag']
            resp_json = rest_resp.json()
            logging.debug(resp_json)
            if resp_json is None:
                return None
            self.rest_upstream_tx = tx.copy()
            return self._merge_upstream_tx(rest_resp)

        return None

    def _update(self, downstream_delta : TransactionMetadata,
                deadline : Deadline) -> Optional[TransactionMetadata]:
        assert self.downstream_tx is not None
        assert self.rest_upstream_tx is not None

        body_json = downstream_delta.to_json(WhichJson.REST_UPDATE)

        rest_resp = self._post_tx(
            self.transaction_url, body_json, self.client.patch, deadline)
        if rest_resp is None:
            return None
        if 'etag' not in rest_resp.headers:
            return None
        self.etag = rest_resp.headers['etag']
        return self._merge_upstream_tx(rest_resp)

    def _post_tx(self, url, body_json : dict,
                 http_method,
                 deadline : Deadline) -> Optional[HttpResponse]:
        logging.debug('RestEndpoint._post_tx %s', url)
        req_headers = {}
        if self.http_host:
            req_headers['host'] = self.http_host
        deadline_left = deadline.deadline_left()
        self._set_request_timeout(req_headers, deadline_left)
        assert self.etag
        req_headers['if-match'] = self.etag
        try:
            kwargs : Dict[Any, Any] = {}
            if body_json:
                req_headers['content-type'] = 'application/json'
                kwargs['json'] = body_json
            else:
                kwargs['data'] = b''
            rest_resp = http_method(
                url,
                **kwargs,
                headers=req_headers,
                timeout=deadline_left)
        except RequestError as e:
            logging.debug(e)
            return None
        logging.info('RestEndpoint._update resp %s %s',
                     rest_resp, rest_resp.http_version)

        if rest_resp.status_code != 200:
            return rest_resp
        return rest_resp

    def _cancel(self):
        logging.debug('RestEndpoint._cancel %s ', self.transaction_url)
        if not self.transaction_url:
            return
        try:
            rest_resp = self.client.post(self.transaction_url + '/cancel')
        except RequestError as e:
            logging.debug('RestEndpoint._cancel POST exception %s', e)
        else:
            logging.debug('RestEndpoint._cancel %s %s', self.transaction_url,
                          rest_resp)
        return

    def _maybe_update_message_builder(self, created, downstream_body, deadline
                                      ) -> Optional[FilterResult]:
        assert self.rest_upstream_tx is not None
        assert self.downstream_tx is not None

        # rest receiving with message parsing sends message builder
        # spec out the back. Without pipelining, this will always be
        # in a separate update from the initial creation. However rest
        # submission could send an inline body in which case you'll
        # get it all in the initial on_update() so don't send it again
        # here.
        if created or not isinstance(downstream_body, MessageBuilderSpec):
            return None

        upstream_body = self.rest_upstream_tx.body
        assert isinstance(upstream_body, MessageBuilderSpec), upstream_body

        assert isinstance(downstream_body, MessageBuilderSpec)
        rest_resp = self._post_tx(
            upstream_body.uri,
            downstream_body.json, self.client.post, deadline)
        if rest_resp is None or rest_resp.status_code != 200:
            logging.debug(rest_resp)
            self.downstream_tx.data_response = Response(
                400, 'RestEndpoint update_message_builder http err')
            return FilterResult()

        # xxx hack around full tree body delta
        #self.rest_upstream_tx.body = None
        if self._merge_upstream_tx(rest_resp) is None:
            return FilterResult()
        return None

    def _maybe_cancel(self, tx_delta) -> Optional[FilterResult]:
        assert self.downstream_tx is not None
        if tx_delta.cancelled:
            self._cancel()
            upstream_delta = TransactionMetadata()
            # TODO I'm not sure whether it's possible to get new
            # requests in delta along with cancellation. This response
            # should never get as far as smtp since cancel only occurs
            # after the smtp transaction aborted due to rset/quit/timeout.
            self.downstream_tx.fill_inflight_responses(
                Response(550, 'cancelled'), upstream_delta)
            return FilterResult()
        elif self.downstream_tx.cancelled:
            return FilterResult()
        return None

    def _update_body(self, tx_delta):
        downstream_delta = tx_delta.copy()
        downstream_delta.body = None
        # We always handle the body separately and indeed in the
        # absence of pipelining support in aiosmtpd, it will always
        # come in a separate update anyway.
        downstream_body = tx_delta.body
        upstream_body = self.rest_upstream_tx.body if self.rest_upstream_tx else None
        if isinstance(downstream_body, MessageBuilderSpec):
            # xxx this is sort of a 3-way merge when
            # rest receiver sent us back body_blob/message_builder uris
            downstream_body = downstream_body.clone()
            downstream_body.blobs = None  #{
            #     blob_id : None
            #     for blob_id in downstream_body.blobs.keys()
            # }
            if upstream_body:
                assert isinstance(upstream_body, MessageBuilderSpec)
                downstream_body.uri = upstream_body.uri
                downstream_body.body_blob = upstream_body.body_blob
        elif isinstance(downstream_body, Blob):
            downstream_body = None
        elif downstream_body is not None:
            assert False, downstream_body
        return downstream_delta, downstream_body

    def on_update(self, tx_delta : TransactionMetadata,
                  timeout : Optional[float] = None) -> FilterResult:
        assert self.downstream_tx is not None
        if r := self._maybe_cancel(tx_delta):
            return r

        # xxx envelope vs data timeout
        if timeout is None:
            timeout = self.timeout_start
        deadline = Deadline(timeout)

        logging.debug('RestEndpoint.on_update start %s '
                      'timeout=%s downstream tx %s',
                      self.transaction_url, timeout, self.downstream_tx)

        downstream_delta, downstream_body = self._update_body(tx_delta)

        tx_update = False
        created = False
        upstream_delta = None
        if not self.transaction_url:
            if self.http_host is None:
                if self.downstream_tx.upstream_http_host:
                    self.http_host = self.downstream_tx.upstream_http_host
                elif self.static_http_host:
                    self.http_host = self.static_http_host

            if self.base_url is None:
                self.base_url = self.downstream_tx.rest_endpoint
            self.rest_upstream_tx = self.downstream_tx.copy_valid(WhichJson.REST_CREATE)
            # cf _update_message_builder(), this is probably moot
            # because downstream_body is None on the first update
            self.rest_upstream_tx.body = downstream_body

            upstream_delta = self._create(self.downstream_tx.resolution,
                                          self.rest_upstream_tx, deadline)
            tx_update = True
            created = True
        else:
            assert self.rest_upstream_tx is not None
            self.rest_upstream_tx.merge_from(downstream_delta)

            # as long as the delta isn't just the body, send a patch even
            # if it's empty as a heartbeat
            # TODO maybe this should have nospin logic i.e. don't send
            # a heartbeat more often than every n secs?
            if (tx_delta.empty(WhichJson.REST_UPDATE) or
                not downstream_delta.empty(WhichJson.REST_UPDATE)):
               upstream_delta = self._update(downstream_delta, deadline)
               tx_update = True

        if tx_update:
            if upstream_delta is None:
                self.downstream_tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint upstream http err'))
                return FilterResult()
            logging.debug(self.rest_upstream_tx)
            err = None
            if self.rest_upstream_tx.req_inflight():
                upstream_delta = self._get(deadline)
                if upstream_delta is None:
                    err = 'bad resp GET after POST/PUT'
                    logging.debug(err)
                if self.rest_upstream_tx.req_inflight():
                    err = 'upstream timeout'
            if err is not None:
                logging.debug(err)
                self.downstream_tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint ' + err))
                return FilterResult()

        assert self.transaction_url is not None

        if tx_delta.body is None:
            return FilterResult()

        if (res := self._maybe_update_message_builder(
                created, downstream_body, deadline)) is not None:
            return res

        # delta/merge bugs in the chain downstream from here have been
        # known to drop response fields on subsequent calls so use
        # upstream_tx, not tx here
        if not any([isinstance(r, Response) and r.ok()
                    for r in self.rest_upstream_tx.rcpt_response]):
            # TODO this should be implemented centrally in FilterChain?
            self.downstream_tx.data_response = Response(
                    503, "5.1.1 data failed precondition: all rcpts failed"
                    " (RestEndpoint)")
            return FilterResult()

        blobs = self._update_blobs(tx_delta)

        # NOTE _get() will wait on tx version/etag once even if
        # not req_inflight()
        if not blobs:
            return FilterResult()

        # NOTE this assumes that message_builder includes all blobs on
        # the first call
        finalized = True
        for blob, url in blobs:
            put_blob_resp = self._put_blob(blob, url)
            if not put_blob_resp.ok():
                self.downstream_tx.data_response = put_blob_resp
                return FilterResult()
            if not blob.finalized():
                finalized = False
        if not finalized:
            return FilterResult()

        # TODO/NOTE:
        # _get() loops on rest_upstream_tx.req_inflight().
        # req_inflight() only returns true if body.finalized().
        # This is per the upstream blob specs/json in rest_upstream_tx.
        # We could do pedantic validation of that to make sure that it
        # is consistent with whether we think we've sent all the data
        # from blob_readers, etc.

        blob_delta = self._get(deadline)

        # NB this delta/merge is fragile and depends on dropping
        # fields we aren't going to send upstream (above)
        if blob_delta is None:
            self.downstream_tx.fill_inflight_responses(
                Response(450, 'RestEndpoint upstream invalid resp/delta get'))
            return FilterResult()

        # if there are any empty responses at this point then we timed out
        self.downstream_tx.fill_inflight_responses(
            Response(450, 'RestEndpoint upstream timeout'))
        return FilterResult()

    def _update_blobs(self, tx_delta):
        blobs : List[Tuple[Blob, str]] = []  # url
        def add_blob(blob, blob_spec):
            assert isinstance(blob, Blob)
            assert isinstance(blob_spec, BlobSpec), blob_spec
            assert isinstance(blob_spec.reuse_uri, BlobUri)
            blobs.append((blob, blob_spec.reuse_uri.parsed_uri))
        if isinstance(tx_delta.body, Blob):
            body_blob : Union[Blob, BlobSpec, MessageBuilderSpec, None] = None
            b = self.rest_upstream_tx.body
            if isinstance(b, BlobSpec):
                body_blob = b
            elif isinstance(b, MessageBuilderSpec):
                body_blob = b.body_blob
            else:
                assert False, b
            add_blob(tx_delta.body, body_blob)
        elif isinstance(tx_delta.body, MessageBuilderSpec):
            assert isinstance(self.rest_upstream_tx.body, MessageBuilderSpec)
            for blob_id, blob in tx_delta.body.blobs.items():
                add_blob(blob, self.rest_upstream_tx.body.blobs[blob_id])
            if tx_delta.body.body_blob is not None:
                add_blob(tx_delta.body.body_blob,
                         self.rest_upstream_tx.body.body_blob)
        else:
            raise ValueError()
        return blobs

    # Send a finalized blob with a single http request.
    def _put_blob_single(self, blob : Blob, blob_url : str) -> Response:
        assert blob.finalized()
        logging.debug('_put_blob_single %d %s',
                      blob.content_length(), blob_url)
        headers = {}
        if self.http_host:
            headers['host'] = self.http_host

        rest_resp = None
        try:
            rest_resp = self.client.put(
                blob_url, headers=headers, content = BlobReader(blob))
        except RequestError as e:
            logging.info('RestEndpoint._put_blob_single RequestError %s', e)
        if rest_resp is None or rest_resp.status_code != 200:
            logging.debug(rest_resp)
            return Response(450, 'RestEndpoint blob upload error')
        logging.info('RestEndpoint._put_blob_single %s', rest_resp)
        return Response()

    def _put_blob(self, blob : Blob, blob_url : str) -> Response:
        if blob not in self.blob_readers:
            self.blob_readers[blob] = BlobReader(blob)
        blob_reader = self.blob_readers[blob]
        if blob_reader is None and self.chunk_size is None and blob.finalized():
            return self._put_blob_single(blob, blob_url)

        empty_put = (blob.finalized() and
                     (blob.len() == blob.content_length()) and
                     (blob_reader.tell() == blob.content_length()))

        offset = blob_reader.tell()
        while (offset < blob.len()) or empty_put:
            chunk = blob_reader.read(self.chunk_size)
            chunk_last = False
            if blob.content_length() is not None:
                chunk_last = ((offset + len(chunk)) >= blob.content_length())
            logging.debug('RestEndpoint._put_blob() '
                          'chunk_offset %d chunk len %d '
                          'chunk_last %s',
                          offset, len(chunk), chunk_last)

            resp, result_length = self._put_blob_chunk(
                offset=offset, d=chunk, last=chunk_last, blob_url=blob_url)
            if resp is None:
                return Response(450, 'RestEndpoint blob upload error')
            elif not resp.ok():
                return resp
            # XXX does this actually need to handle short writes?
            # should that just 500?
            chunk_out = result_length - offset
            if chunk_out > len(chunk):
                resp = Response(450, 'invalid resp content-range')
                logging.debug(resp)
                return resp
            offset += chunk_out
            if chunk_out < len(chunk):
                blob_reader.seek(offset)
            logging.debug('RestEndpoint._put_blob() '
                          'result_length %d chunk_out %d',
                          result_length, chunk_out)
            if empty_put:
                break
        return Response()

    # -> (resp, len)
    def _put_blob_chunk(
            self, offset, d : bytes, last : bool, blob_url : str
    ) -> Tuple[Optional[Response], Optional[int]]:
        logging.info('RestEndpoint._put_blob_chunk %d %d %s',
                     offset, len(d), last)
        headers = {}
        if last and len(d) == 0:
            headers[FINALIZE_BLOB_HEADER] = str(offset)
        elif offset > 0 or not last:
            headers['content-range'] = ContentRange(
                'bytes', offset, offset + len(d),
                offset + len(d) if last else None).to_header()
        if self.http_host:
            headers['host'] = self.http_host
        try:
            logging.info('RestEndpoint._put_blob_chunk() PUT %s %s',
                         blob_url, headers)
            rest_resp = self.client.put(
                blob_url, headers=headers, content=d,
                timeout=self.timeout_data)
            logging.info('RestEndpoint._put_blob_chunk PUT %s %s %s',
                         blob_url, rest_resp, rest_resp.headers)
            if rest_resp.status_code not in [200, 416]:
                return Response(
                    450, 'RestEndpoint._put_blob_chunk PUT err'), None
        except RequestError as e:
            logging.info('RestEndpoint._put_blob_chunk RequestError %s', e)
            return None, None

        # Most(all?) errors on blob put here means temp
        # transaction final status
        # IOW blob upload is not the place to reject the content, etc.

        # cf RestTransactionHandler.build_resp() re content-range
        dlen : Optional[int] = len(d)
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
                  ) -> Union[None, HttpResponse, _Unchanged]:
        assert self.transaction_url is not None
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
            logging.debug('RestEndpoint.get_json %s %s',
                          rest_resp, [r.headers for r in rest_resp.history])
        except RequestError as e:
            logging.debug('RestEndpoint.get_json() %s error %s',
                          self.transaction_url, e)
            return None
        if rest_resp.status_code not in [200, 304]:
            logging.debug(rest_resp.text)
            return None
        if 'etag' not in rest_resp.headers:
            return None
        etag = rest_resp.headers['etag']
        if rest_resp.status_code == 304:
            if etag != self.etag:
                return None
            return _Unchanged.UNCHANGED
        else:  # 200
            self.etag = etag

        return rest_resp

    # test only
    def get_json(self, timeout : Optional[float] = None,
                 point_read = True
                 ) -> Optional[dict]:
        rest_resp = self._get_json(timeout, testonly_point_read=point_read)
        if rest_resp is None:
            return None
        resp_json = get_resp_json(rest_resp)
        if resp_json is not None:
            assert isinstance(resp_json, dict)
        return resp_json

    # does GET /tx at least once, polls as long as tx contains
    # inflight reqs, returns delta vs initial rest_upstream_tx
    def _get(self, deadline : Deadline, oneshot=False
             ) -> Optional[TransactionMetadata]:
        assert self.rest_upstream_tx is not None
        assert self.downstream_tx is not None

        prev = self.rest_upstream_tx.copy()
        while deadline.remaining():
            logging.debug('RestEndpoint._get() %s %s',
                          self.transaction_url, deadline.deadline_left())
            start = time.monotonic()
            prev_etag = self.etag
            rest_resp = self._get_json(timeout=deadline.deadline_left())
            dt = time.monotonic() - start
            if rest_resp is None:  # timeout or invalid json
                return None
            elif isinstance(rest_resp, HttpResponse):
                if (delta := self._merge_upstream_tx(rest_resp)) is None:
                    return None
            logging.debug(self.rest_upstream_tx)
            if oneshot:
                return prev.delta(self.rest_upstream_tx)

            if not self.rest_upstream_tx.req_inflight():
                return prev.delta(self.rest_upstream_tx)

            # min delta
            # XXX configurable
            # XXX backoff?
            if not deadline.remaining(1):
                break
            if (self.etag is None or self.etag == prev_etag) and (dt < 1):
                nospin = 1 - dt
                logging.debug('nospin %s %f', self.transaction_url, nospin)
                time.sleep(nospin)
            if oneshot and self.etag != prev_etag:
                break
        return prev.delta(self.rest_upstream_tx)

    # ~AsyncFilter
    # do hanging get, don't loop on req_inflight()
    def get(self, deadline : Deadline):
        return self._get(deadline, oneshot=True)
