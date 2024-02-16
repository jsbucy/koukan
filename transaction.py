from typing import Dict, Optional, Any
import logging
import json
import secrets
import time

from flask import Request as FlaskRequest, Response as FlaskResponse

from werkzeug.datastructures import ContentRange

from rest_service_handler import Handler, HandlerFactory

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata, WhichJson

REST_ID_BYTES = 4  # XXX configurable, use more in prod

class RestServiceTransaction(Handler):
    storage : Storage
    _tx_rest_id : str
    http_host : Optional[str]
    tx_cursor : TransactionCursor
    blob_rest_id : Optional[str]
    blob_writer : Optional[BlobWriter]

    def __init__(self, storage,
                 tx_rest_id=None,
                 http_host=None,
                 tx_cursor=None,
                 blob_rest_id=None,
                 blob_writer=None):
        self.storage = storage
        self.http_host = http_host
        self._tx_rest_id = tx_rest_id
        self.tx_cursor = tx_cursor
        self.blob_rest_id = blob_rest_id
        self.blob_writer = blob_writer

    def etag(self):
        return self.tx_cursor.etag()

    @staticmethod
    def create_tx(storage, http_host) -> Optional["RestServiceTransaction"]:
        logging.debug('RestServiceTransaction.create_tx %s', http_host)

        cursor = storage.get_transaction_cursor()
        return RestServiceTransaction(storage,
                                      http_host=http_host,
                                      tx_cursor=cursor)

    @staticmethod
    def load_tx(storage, rest_id) -> Optional["RestServiceTransaction"]:
        cursor = storage.get_transaction_cursor()
        if not cursor.load(rest_id=rest_id):
            return None
        return RestServiceTransaction(
            storage, tx_rest_id=rest_id, tx_cursor=cursor)

    @staticmethod
    def load_blob(storage, blob_uri) -> Optional["RestServiceTransaction"]:
        blob_rest_id = blob_uri.removeprefix('/blob/')  # XXX uri prefix

        blob_writer = storage.get_blob_writer()
        if blob_writer.load(rest_id=blob_rest_id) is None:
            return None
        return RestServiceTransaction(
            storage, blob_rest_id=blob_rest_id, blob_writer=blob_writer)

    def tx_rest_id(self):
        return self._tx_rest_id

    def start(self, req_json, timeout : Optional[float] = None
              ) -> Optional[FlaskResponse]:
        logging.debug('RestServiceTransaction.start %s %s',
                      self.http_host, req_json)
        assert self._tx_rest_id is None
        self._tx_rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        tx = TransactionMetadata.from_json(req_json, WhichJson.REST_CREATE)
        tx.host = self.http_host
        if tx is None:
            return FlaskResponse(status=400,
                                 response=['invalid transaction json'])
        create_body_rest_id = None
        if tx.body:
            tx.body = tx.body.removeprefix('/blob/')
        elif 'body' in req_json and req_json['body'] is None:
            create_body_rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        self.tx_cursor.create(self._tx_rest_id, tx,
                              create_body_rest_id = create_body_rest_id)
        self.tx_cursor.load()
        return self._get_tx_json(timeout)

    def patch(self, req_json : Dict[str, Any],
              timeout : Optional[float] = None) -> FlaskResponse:
        logging.debug('RestServiceTransaction.patch %s %s',
                      self._tx_rest_id, req_json)
        resp_json = {}
        tx = TransactionMetadata.from_json(req_json, WhichJson.REST_UPDATE)
        if tx is None:
            return FlaskResponse(status=400,
                                 response=['invalid transaction delta'])

        create_body_rest_id = None
        if tx.body:
            tx.body = tx.body.removeprefix('/blob/')
        elif 'body' in req_json and req_json['body'] == '':
            create_body_rest_id = secrets.token_urlsafe(REST_ID_BYTES)

        try:
            self.tx_cursor.write_envelope(
                tx, create_body_rest_id=create_body_rest_id)
        except VersionConflictException:
            return FlaskResponse(status=412,
                                 response=['version conflict'])
        self.tx_cursor.load()
        return self._get_tx_json(timeout)

    def _get_tx_json(self, timeout : Optional[float] = None) -> FlaskResponse:
        start = time.monotonic()
        while True:
            deadline_left = None
            if timeout:
                deadline_left = timeout - (time.monotonic() - start)
                if deadline_left <= 0:
                    break
            else:
                break

            # only wait if something is inflight upstream and we think the
            # status might change soon
            logging.info('RestServiceTransaction.get rcpt_to=%s rcpt_resp=%s',
                         self.tx_cursor.tx.rcpt_to,
                         self.tx_cursor.tx.rcpt_response)
            wait_mail = (self.tx_cursor.tx.mail_from is not None and
                         self.tx_cursor.tx.mail_response is None)
            wait_rcpt = (len(self.tx_cursor.tx.rcpt_response) <
                         len(self.tx_cursor.tx.rcpt_to))
            wait_data = ((self.tx_cursor.last or self.tx_cursor.tx.body) and
                         self.tx_cursor.tx.data_response is None)
            if not (wait_mail or wait_rcpt or wait_data):
                break
            logging.info('RestServiceTransaction.get wait '
                         'mail=%s rcpt=%s data=%s deadline_left=%f',
                         wait_mail, wait_rcpt, wait_data, deadline_left)
            self.tx_cursor.wait(timeout=deadline_left)
            logging.info('RestServiceTransaction.get wait done')

        resp_json = {}
        resp_js = lambda r: r.to_json() if r is not None else {}
        if (self.tx_cursor.tx.mail_from is not None or
            self.tx_cursor.tx.mail_response is not None):
            resp_json['mail_response'] = resp_js(
                self.tx_cursor.tx.mail_response)
        if self.tx_cursor.tx.rcpt_to:
            rcpt_resp = self.tx_cursor.tx.rcpt_response
            rcpt_resp_json = resp_json['rcpt_response'] = []
            for i,r in enumerate(self.tx_cursor.tx.rcpt_to):
                if i < len(rcpt_resp):
                    rcpt_resp_json.append(rcpt_resp[i].to_json())
                else:
                    rcpt_resp_json.append({})


        # TODO surface more info about body here, finalized or not,
        # len, sha1, etc

        data_resp = False
        if self.tx_cursor.tx.data_response:
            data_resp = True
        if self.tx_cursor.tx.body:
            data_resp = True

        if data_resp:
            resp_json['data_response'] = resp_js(
                self.tx_cursor.tx.data_response)


        if self.tx_cursor.tx.body:
            resp_json['body'] = '/blob/' + self.tx_cursor.tx.body

        # xxx this needs the inflight -> {} logic
        #resp_json = self.tx_cursor.tx.to_json(WhichJson.REST_READ)

        logging.info('RestServiceTransaction.get done %s %s',
                     self._tx_rest_id, resp_json)

        # xxx flask jsonify() depends on app context which we may
        # not have in tests?
        rest_resp = FlaskResponse()
        rest_resp.set_data(json.dumps(resp_json))
        rest_resp.content_type = 'application/json'
        return rest_resp

    def get(self, req_json : Dict[str, Any], timeout : Optional[int] = None
            ) -> FlaskResponse:
        logging.info('RestServiceTransaction.get %s', self._tx_rest_id)

        logging.info('RestServiceTransaction.get %s %s', self._tx_rest_id,
                     self.tx_cursor.tx)
        return self._get_tx_json(timeout=timeout)


    @staticmethod
    def build_resp(code, msg, writer):
        resp = FlaskResponse(status=code, response=[msg] if msg else None)
        logging.info('build_resp offset=%s content_length=%s',
                     writer.length, writer.content_length)
        if writer.length > 0:
            resp.headers.set(
                'content-range',
                ContentRange('bytes', 0, writer.length, writer.content_length))
        return resp

    def put_blob(self, request : FlaskRequest,
                 content_range : ContentRange, range_in_headers : bool):
        logging.info('put_blob loaded %s len=%d content_length=%s',
                     self.blob_rest_id, self.blob_writer.length,
                     self.blob_writer.content_length)

        # being extremely persnickety: we would reject
        # !range_in_headers after a request with it populated even if
        # it's equivalent/noop but may not be worth storing that to
        # enforce?

        offset = content_range.start
        if offset > self.blob_writer.length:
            return RestServiceTransaction.build_resp(
                400, 'range start past the end', self.blob_writer)

        if (self.blob_writer.content_length is not None and
            content_range.length != self.blob_writer.content_length):
            return RestServiceTransaction.build_resp(
                400, 'append or !last after last', self.blob_writer)

        if self.blob_writer.length >= content_range.stop:
            return RestServiceTransaction.build_resp(
                200, 'noop (range)', self.blob_writer)

        # xxx on second thought I'm not sure why we would accept an
        # append that isn't exactly at the current end, just return a
        # 4xx with the current content-range
        d = request.data[self.blob_writer.length - offset:]
        assert len(d) > 0
        self.blob_writer.append_data(d, content_range.length)

        return RestServiceTransaction.build_resp(
            200, None, self.blob_writer)


    def abort(self):
        pass


# interface to top-level flask app
class RestServiceTransactionFactory(HandlerFactory):
    def __init__(self, storage : Storage):
        self.storage = storage

    def create_tx(self, http_host) -> Optional[RestServiceTransaction]:
        return RestServiceTransaction.create_tx(self.storage, http_host)

    def get_tx(self, tx_rest_id) -> Optional[RestServiceTransaction]:
        return RestServiceTransaction.load_tx(
            self.storage, tx_rest_id)

    def get_blob(self, blob_rest_id) -> Optional[RestServiceTransaction]:
        return RestServiceTransaction.load_blob(self.storage, blob_rest_id)


def output(cursor, endpoint) -> Optional[Response]:
    logging.debug('cursor_to_endpoint %s', cursor.rest_id)

    last_tx = TransactionMetadata()
    err = None
    ok_rcpt = False
    # TODO this needs to notice that the tx has aborted e.g. due to
    # timing out on input cf Storage._gc_non_durable_one()
    while True:
        delta = last_tx.delta(cursor.tx)
        # XXX delta() outputs response fields
        # but we don't really say if it's a precondition of Filter.on_update()
        # that those fields are unset
        delta.mail_response = None
        delta.rcpt_response = []
        assert delta is not None
        logging.info('cursor_to_endpoint %s '
                     'mail_from = %s '
                     'rcpt_to = %s ',
                     cursor.rest_id, delta.mail_from, delta.rcpt_to)
        if delta.mail_from is None and not delta.rcpt_to:
            if cursor.tx.body:
                logging.debug('cursor_to_endpoint %s -> body', cursor.rest_id)
                break
            else:
                cursor.wait()
                continue

        endpoint.on_update(delta)
        if delta.mail_from:
            logging.debug('cursor_to_endpoint %s mail_from: %s',
                          cursor.rest_id, delta.mail_from.mailbox)
            resp = delta.mail_response
            if not resp:
                # XXX fix upstream and assert
                logging.warning('tx out: output chain didn\'t set '
                                'mail response')
                resp = Response()
            else:
                logging.debug('cursor_to_endpoint %s mail_resp %s',
                              cursor.rest_id, resp)
            if resp.err():
                err = resp
            while True:
                try:
                    cursor.set_mail_response(resp)
                except VersionConflictException:
                    cursor.load()
                    continue
                break
            last_tx.mail_from = delta.mail_from

        if delta.rcpt_to:
            resp = delta.rcpt_response
            logging.debug('cursor_to_endpoint %s rcpt_to: %s resp %s',
                          cursor.rest_id, delta.rcpt_to, resp)
            assert len(resp) == len(delta.rcpt_to)
            logging.debug('cursor_to_endpoint %s rcpt_resp %s',
                          cursor.rest_id, resp)
            while True:
                try:
                    cursor.add_rcpt_response(resp)
                except VersionConflictException:
                    cursor.load()
                    continue
                break
            for r in resp:
                if r.ok():
                    ok_rcpt = True
                elif err is None:
                    # XXX revisit in the context of the exploder,
                    # this is used downstream for append_action,
                    # what does that mean in the context of
                    # multi-rcpt?
                    err = r
            last_tx.rcpt_to += delta.rcpt_to


    if not ok_rcpt:
        logging.debug('cursor_to_endpoint %s no rcpt', cursor.rest_id)
        return err

    blob_reader = cursor.parent.get_blob_reader()
    blob_reader.load(rest_id = cursor.tx.body)
    while blob_reader.content_length() is None or (
            blob_reader.length < blob_reader.content_length()):
        blob_reader.wait()
    body_tx = TransactionMetadata()
    body_tx.body_blob = blob_reader
    endpoint.on_update(body_tx)
    data_resp = body_tx.data_response
    assert data_resp is not None
    logging.info('cursor_to_endpoint %s body %s', cursor.rest_id, data_resp)
    while True:
        try:
            cursor.set_data_response(data_resp)
        except VersionConflictException:
            cursor.load()
            continue
        break
    return data_resp

def cursor_to_endpoint(cursor, endpoint):
    logging.debug('cursor_to_endpoint %s', cursor.rest_id)
    resp = output(cursor, endpoint)
    if resp is None:
        logging.warning('cursor_to_endpoint %s abort', cursor.rest_id)
        return
    logging.info('cursor_to_endpoint %s done %s', cursor.rest_id, resp)
    while True:
        try:
            cursor.finalize_attempt(not resp.temp())
            break
        except VersionConflictException:
            cursor.load()
