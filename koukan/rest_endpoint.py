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

    rest_upstream_tx : Optional[TransactionMetadata] = None
    sent_data_last : bool = False

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

    # -> full-url, path
    def _maybe_qualify_url(self, url) -> Tuple[str, str]:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return url, parsed.path
        return urljoin(self.base_url, url), url

    def _create(self,
                resolution : Optional[Resolution],
                tx : TransactionMetadata,
                deadline : Deadline) -> Optional[HttpResponse]:
        assert self.base_url is not None
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
            logging.info('RestEndpoint._create req_headers %s resp %s %s',
                         req_headers, rest_resp, rest_resp.text)
            if rest_resp.status_code != 201:
                continue
            location = rest_resp.headers['location']
            self.transaction_url, self.transaction_path = self._maybe_qualify_url(location)
            if 'etag' not in rest_resp.headers:
                return None
            self.etag = rest_resp.headers['etag']
            return rest_resp
        return rest_resp

    def _update(self, downstream_delta : TransactionMetadata,
                deadline : Deadline) -> Optional[HttpResponse]:
        body_json = downstream_delta.to_json(WhichJson.REST_UPDATE)

        rest_resp = self._post_tx(
            self.transaction_url, body_json, self.client.patch, deadline)
        if rest_resp is None:
            return None
        if 'etag' not in rest_resp.headers:
            return None
        self.etag = rest_resp.headers['etag']
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
                uri,
                **kwargs,
                headers=req_headers,
                timeout=deadline_left)
        except RequestError:
            return None
        logging.info('RestEndpoint._update resp %s %s',
                     rest_resp, rest_resp.http_version)

        if rest_resp.status_code != 200:
            return rest_resp
        return rest_resp

    def _update_message_builder(self, delta : TransactionMetadata,
                                deadline : Deadline
                                ) -> Optional[Response]:
        assert self.transaction_url is not None
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
            return
        try:
            rest_resp = self.client.post(self.transaction_url + '/cancel')
        except RequestError as e:
            logging.debug('RestEndpoint._cancel POST exception %s', e)
        else:
            logging.debug('RestEndpoint._cancel %s %s', self.transaction_url,
                          rest_resp)
        return

    def on_update(self, tx_delta : TransactionMetadata,
                        timeout : Optional[float] = None) -> FilterResult:
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

        if self.http_host is None and self.transaction_url is None:
            if self.downstream_tx.upstream_http_host:
                self.http_host = self.downstream_tx.upstream_http_host
            elif self.static_http_host:
                self.http_host = self.static_http_host


        # xxx envelope vs data timeout
        if timeout is None:
            timeout = self.timeout_start
        deadline = Deadline(timeout)

        logging.debug('RestEndpoint.on_update start %s '
                      'timeout=%s downstream tx %s',
                      self.transaction_url, timeout, self.downstream_tx)

        downstream_delta = tx_delta.copy()
        if downstream_delta.body:
            del downstream_delta.body
        if downstream_delta.attempt_count:
            del downstream_delta.attempt_count

        # We are going to send a slightly different delta upstream per
        # remote_host (discovery in _create()) and body (below).
        # When we look at the delta of what we got back, these fields
        # should not appear so it will merge cleanly with the original input.
        if self.rest_upstream_tx is None:
            if self.base_url is None:
                self.base_url = self.downstream_tx.rest_endpoint
            self.rest_upstream_tx = self.downstream_tx.copy_valid(WhichJson.REST_CREATE)
        else:
            self.rest_upstream_tx.merge_from(downstream_delta)

        # XXX router_service_test test_reuse_body (apparently the only
        # one) uses this for submission and sends BlobSpec for body
        # reuse here. That test should really be using
        # examples/send_message instead?

        # Otherwise clear blobs so the _get() after POST doesn't wait on
        # data_response/req_inflight() (cf below)

        # XXX sort of weird the way rest_upstream_tx.body will change
        # from MessageBuilderSpec to BlobStatus or whatever
        # downstream body shouldn't go into rest_upstream_tx at all
        # since we get the blobs to send from the delta anyway?
        if isinstance(self.rest_upstream_tx.body, MessageBuilderSpec):
            self.rest_upstream_tx.body = copy.copy(self.rest_upstream_tx.body)
            # XXX
            self.rest_upstream_tx.body.blobs = {
                blob_id : None for blob_id in self.rest_upstream_tx.body.blobs.keys()
            }
        elif isinstance(self.rest_upstream_tx.body, Blob):
            self.rest_upstream_tx.body = None
        elif isinstance(self.rest_upstream_tx.body, BlobSpec):
            pass
        elif self.rest_upstream_tx.body is not None:
            raise ValueError()
        #xxx dead upstream_tx = self.rest_upstream_tx.copy()

        logging.debug('RestEndpoint.on_update merged tx %s', self.rest_upstream_tx)

        tx_update = False
        created = False
        if not self.transaction_url:
            rest_resp = self._create(self.downstream_tx.resolution,
                                     self.rest_upstream_tx, deadline)
            if rest_resp is not None and rest_resp.status_code != 201:
                rest_resp = None
            tx_update = True
            created = True
        else:
            # as long as the delta isn't just the body, send a patch even
            # if it's empty as a heartbeat
            # TODO maybe this should have nospin logic i.e. don't send
            # a heartbeat more often than every n secs?
            delta_no_body = tx_delta.copy()
            delta_no_body.body = None
            if (tx_delta.empty(WhichJson.REST_UPDATE) or
                not delta_no_body.empty(WhichJson.REST_UPDATE)):
               rest_resp = self._update(downstream_delta, deadline)
               # TODO handle 412 failed precondition
               if rest_resp is not None and rest_resp.status_code != 200:
                   rest_resp = None

               tx_update = True

        upstream_delta = TransactionMetadata()
        if tx_update:
            tx_out = None
            resp_json = None
            if rest_resp is not None:
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
                self.downstream_tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint upstream http err'))
                return FilterResult()

            # NOTE we cleared blobs from upstream_tx (above) to
            # prevent this for waiting for data_response since we
            # don't send the blobs until later.
            if self.rest_upstream_tx.req_inflight(tx_out):
                tx_out = self._get(deadline)
            err = None
            if tx_out is None:
                err = 'bad resp GET after POST/PUT'
                logging.debug(err)
            elif self.rest_upstream_tx.req_inflight(tx_out):
                err = 'upstream timeout'
            if err:
                self.downstream_tx.fill_inflight_responses(
                    Response(450, 'RestEndpoint ' + err))
                return FilterResult()

            upstream_delta = self.rest_upstream_tx.delta(tx_out, WhichJson.REST_READ)
            downstream_delta = upstream_delta.copy()
            downstream_delta.body = None
            if (upstream_delta is None or
                (self.rest_upstream_tx.merge_from(upstream_delta) is None) or
                (self.downstream_tx.merge_from(downstream_delta) is None)):
                self.downstream_tx.fill_inflight_responses(
                    Response(450,
                             'RestEndpoint upstream invalid resp/delta update'))
                return FilterResult()

        if tx_delta.body is None:
            return FilterResult()

        # rest receiving with message parsing sends message builder
        # spec out the back. Without pipelining, this will always be
        # in a separate update from the initial creation. However rest
        # submission could send an inline body in which case you'll
        # get it all in the initial on_update() so don't send it again
        # here.
        if not created and isinstance(tx_delta.body, MessageBuilderSpec):
            data_err = self._update_message_builder(tx_delta, deadline)
            if data_err is not None:
                self.downstream_tx.data_response = data_err
                return FilterResult()
            # xxx possibly this POST should return the spec decorated
            # with the blob uris but for the moment just GET the tx
            # again
            tx_out = self._get(deadline)
            d = self.rest_upstream_tx.delta(tx_out, WhichJson.REST_READ)
            self.rest_upstream_tx.merge_from(d)

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

        blobs : List[Tuple[Blob, str]]  # uri
        logging.debug(self.rest_upstream_tx)
        if isinstance(tx_delta.body, Blob):
            assert isinstance(self.rest_upstream_tx.body, BlobSpec)
            assert isinstance(self.rest_upstream_tx.body.reuse_uri, BlobUri)

            assert isinstance(self.rest_upstream_tx.body.reuse_uri.parsed_uri, str)
            blobs = [ (tx_delta.body, self.rest_upstream_tx.body.reuse_uri.parsed_uri) ]
        elif isinstance(tx_delta.body, MessageBuilderSpec):
            blobs = []
            # logging.debug(self.rest_upstream_tx.body.blob_specs)
            # logging.debug(self.rest_upstream_tx.body.body_blob)
            for blob_id, blob in tx_delta.body.blobs.items():
                assert isinstance(blob, Blob)
                logging.debug(blob.rest_id())
                assert isinstance(self.rest_upstream_tx.body, MessageBuilderSpec)
                # brid = blob.rest_id()
                # assert brid  is not None
                blob_spec = self.rest_upstream_tx.body.blobs[blob_id]
                assert isinstance(blob_spec, BlobSpec)
                assert blob_spec.reuse_uri is not None
                uri = blob_spec.reuse_uri.parsed_uri
                assert uri is not None
                blobs.append((blob, uri))
            if tx_delta.body.body_blob is not None:
                assert isinstance(self.rest_upstream_tx.body, MessageBuilderSpec)
                assert isinstance(self.rest_upstream_tx.body.body_blob, BlobSpec)
                logging.debug(self.rest_upstream_tx.body.body_blob)
                body_blob = tx_delta.body.body_blob
                assert isinstance(body_blob, Blob)
                blob_spec = self.rest_upstream_tx.body.body_blob
                assert isinstance(blob_spec.reuse_uri, BlobUri)
                assert isinstance(blob_spec.reuse_uri.parsed_uri, str)
                blobs.append((body_blob, blob_spec.reuse_uri.parsed_uri))
        elif isinstance(tx_delta.body, BlobSpec):
            return FilterResult()
        else:
            raise ValueError()

        logging.debug(blobs)

        # NOTE _get() will wait on tx version even if nothing inflight
        if not blobs:
            return FilterResult()

        # NOTE this assumes that message_builder includes all blobs on
        # the first call
        finalized = True
        for blob, uri in blobs:
            put_blob_resp = self._put_blob(blob, uri)
            if not put_blob_resp.ok():
                self.downstream_tx.data_response = put_blob_resp
                return FilterResult()
            if not blob.finalized():
                finalized = False
        if finalized:
            self.sent_data_last = True
        else:
            return FilterResult()


        tx_out = self._get(deadline)
        logging.debug('RestEndpoint.on_update %s tx from GET %s',
                      self.transaction_url, tx_out)

        # NB this delta/merge is fragile and depends on dropping
        # fields we aren't going to send upstream (above)
        if (tx_out is None or
            (blob_delta := self.rest_upstream_tx.delta(
                tx_out, WhichJson.REST_READ)) is None):
            self.downstream_tx.fill_inflight_responses(
                Response(450, 'RestEndpoint upstream invalid resp/delta get'))
            return FilterResult()

        downstream_delta = blob_delta.copy()
        downstream_delta.body = None
        if ((self.rest_upstream_tx.merge_from(blob_delta) is None) or
            (self.downstream_tx.merge_from(downstream_delta) is None)):
            self.downstream_tx.fill_inflight_responses(
                Response(450, 'RestEndpoint upstream invalid resp/delta get'))
            return FilterResult()

        self.downstream_tx.fill_inflight_responses(
            Response(450, 'RestEndpoint upstream timeout'))
        for t in [#upstream_tx,
                tx_out]:
            t.remote_host = None
        return FilterResult()

    # Send a finalized blob with a single http request.
    def _put_blob_single(self, blob : Blob, uri : str) -> Response:
        assert self.transaction_path is not None
        assert blob.finalized()
        self.blob_url = uri
        logging.debug('_put_blob_single %d %s',
                      blob.content_length(), uri)
        headers = {}
        if self.http_host:
            headers['host'] = self.http_host

        rest_resp = None
        try:
            rest_resp = self.client.put(
                uri, headers=headers, content = BlobReader(blob))
        except RequestError as e:
            logging.info('RestEndpoint._put_blob_single RequestError %s', e)
        if rest_resp is None or rest_resp.status_code != 200:
            logging.debug(rest_resp)
            return Response(450, 'RestEndpoint blob upload error')
        logging.info('RestEndpoint._put_blob_single %s', rest_resp)
        return Response()

    def _put_blob(self, blob : Blob, uri : str) -> Response:
        if blob not in self.blob_readers:
            self.blob_readers[blob] = BlobReader(blob)
        blob_reader = self.blob_readers[blob]
        if blob_reader is None and self.chunk_size is None and blob.finalized():
            return self._put_blob_single(blob, uri)

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
                offset=offset,
                d=chunk,
                last=chunk_last,
                uri=uri)
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
            self, offset, d : bytes, last : bool, uri : str
    ) -> Tuple[Optional[Response], Optional[int]]:
        assert self.transaction_path is not None

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
            self.blob_url = uri

            logging.info('RestEndpoint._put_blob_chunk() PUT %s %s',
                         self.blob_url, headers)
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
                  ) -> Union[None, dict, _Unchanged]:
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

        return get_resp_json(rest_resp)

    # test only
    def get_json(self, timeout : Optional[float] = None,
                 point_read = True
                 ) -> Optional[dict]:
        json = self._get_json(timeout, testonly_point_read=point_read)
        if json is not None:
            assert isinstance(json, dict)
        return json

    # does GET /tx at least once, polls as long as tx contains
    # inflight reqs, returns the last tx it successfully retrieved
    def _get(self, deadline : Deadline, oneshot=False
             ) -> Optional[TransactionMetadata]:
        tx_out = None
        while deadline.remaining():
            logging.debug('RestEndpoint._get() %s %s',
                          self.transaction_url, deadline.deadline_left())
            start = time.monotonic()
            prev_etag = self.etag
            tx_json = self._get_json(timeout=deadline.deadline_left())
            if tx_json is None:  # timeout or invalid json
                return None
            delta = time.monotonic() - start

            if isinstance(tx_json, dict):
                logging.debug('RestEndpoint._get() %s done %s',
                              self.transaction_url, tx_json)

                tx_out = TransactionMetadata.from_json(
                    tx_json, WhichJson.REST_READ)
                if tx_out is None:  # invalid json tx contents
                    return None

                logging.debug('RestEndpoint._get() %s done %s tx_out %s',
                              self.transaction_url, self.etag, tx_out)

                assert self.rest_upstream_tx is not None
                if (not oneshot and
                    not self.rest_upstream_tx.req_inflight(tx_out) and not
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
            if oneshot and self.etag != prev_etag:
                break
        return tx_out

    # ~AsyncFilter
    # do hanging get, don't loop on req_inflight()
    def get(self, deadline : Deadline):
        return self._get(deadline, oneshot=True)
