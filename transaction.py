from typing import Any, Dict, List, Optional, Tuple
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
from message_builder import MessageBuilder

REST_ID_BYTES = 4  # XXX configurable, use more in prod

class RestServiceTransaction(Handler):
    storage : Storage
    _tx_rest_id : str
    http_host : Optional[str]
    tx_cursor : TransactionCursor
    _blob_rest_id : Optional[str]
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
        self._blob_rest_id = blob_rest_id
        self.blob_writer = blob_writer

    def tx_rest_id(self):
        return self._tx_rest_id

    def blob_rest_id(self):
        return self._blob_rest_id

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
    def create_blob_handler(storage):
        blob_writer = storage.get_blob_writer()
        return RestServiceTransaction(storage, blob_writer=blob_writer)

    @staticmethod
    def load_blob(storage, blob_uri) -> Optional["RestServiceTransaction"]:
        blob_rest_id = RestServiceTransaction._blob_uri_to_id(blob_uri)

        blob_writer = storage.get_blob_writer()
        if blob_writer.load(rest_id=blob_rest_id) is None:
            return None
        return RestServiceTransaction(
            storage, blob_rest_id=blob_rest_id, blob_writer=blob_writer)

    # TODO pull out a schema thing to share with RestService
    # NOTE removeprefix(x) doesn't require startswith(x) so this will
    # accept a bare blob rest id
    @staticmethod
    def _blob_uri_to_id(uri):
        return uri.removeprefix('/blob/')

    @staticmethod
    def _blob_id_to_uri(blob_id):
        return '/blob/' + blob_id

    # -> reuse ids
    def _body_blob_id(self, tx, req_json) -> List[str]:
        reuse = []
        if tx.message_builder:
            reuse = MessageBuilder.get_blobs(
                tx.message_builder,
                RestServiceTransaction._blob_uri_to_id)
        elif tx.body:
            tx.body = RestServiceTransaction._blob_uri_to_id(tx.body)
            reuse = [ tx.body ]
        return reuse

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
        reuse_blob_rest_id = self._body_blob_id(tx, req_json)
        self.tx_cursor.create(self._tx_rest_id, tx,
                              reuse_blob_rest_id = reuse_blob_rest_id)
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

        reuse_blob_rest_id = self._body_blob_id(tx, req_json)

        logging.debug('RestServiceTransaction.patch reuse %s',
                      reuse_blob_rest_id)

        try:
            self.tx_cursor.write_envelope(
                tx, reuse_blob_rest_id = reuse_blob_rest_id)
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
            resp_json['body'] = RestServiceTransaction._blob_id_to_uri(self.tx_cursor.tx.body)

        if self.tx_cursor.message_builder:
            resp_json['message_builder'] = {}

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

    def get(self, req_json : Dict[str, Any], timeout : Optional[float] = None
            ) -> FlaskResponse:
        logging.info('RestServiceTransaction.get %s', self._tx_rest_id)

        logging.info('RestServiceTransaction.get %s %s', self._tx_rest_id,
                     self.tx_cursor.tx)
        return self._get_tx_json(timeout=timeout)


    @staticmethod
    def build_resp(code, msg, writer):
        resp = FlaskResponse(status=code, response=[msg] if msg else None)
        logging.info('build_resp code=%d msg=%s offset=%s content_length=%s',
                     code, msg, writer.length, writer.content_length)
        # NOTE chunked PUT with content-range is a ~custom protocol,
        # ours is inspired by gcp cloud storage resumable uploads.
        if writer.length > 0:
            resp.headers.set(
                'content-range',
                ContentRange('bytes', 0, writer.length, writer.content_length))
        return resp

    def create_blob(self, request : FlaskRequest) -> FlaskResponse:
        self._blob_rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        if self.blob_writer.create(self._blob_rest_id) is None:
            return FlaskResponse(status=500, response=['failed to create blob'])
        return FlaskResponse(status=201)

    def put_blob(self, request : FlaskRequest, content_range : ContentRange):
        logging.info('put_blob loaded %s len=%d content_length=%s range %s',
                     self._blob_rest_id, self.blob_writer.length,
                     self.blob_writer.content_length,
                     content_range)

        # this is a little on the persnickety side in that it will
        # fail requests that we could treat as a noop but those should
        # be uncommon, only in case of timeout/retry, etc.
        offset = content_range.start
        if offset != self.blob_writer.length:
            return RestServiceTransaction.build_resp(
                416, 'invalid range', self.blob_writer)

        self.blob_writer.append_data(request.data, content_range.length)

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

    def create_blob(self):
        return RestServiceTransaction.create_blob_handler(self.storage)

    def get_blob(self, blob_rest_id) -> Optional[RestServiceTransaction]:
        return RestServiceTransaction.load_blob(self.storage, blob_rest_id)

