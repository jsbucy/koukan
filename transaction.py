from typing import Dict, Optional, Any
import logging
import json
import secrets
import time

from flask import Request as FlaskRequest, Response as FlaskResponse

from werkzeug.datastructures import ContentRange

from rest_service_handler import Handler, HandlerFactory

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import Action, Status, InvalidActionException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata

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

    @staticmethod
    def create_tx(storage, http_host) -> Optional["RestServiceTransaction"]:
        logging.debug('RestServiceTransaction.create_tx %s', http_host)

        cursor = storage.get_transaction_cursor()
        rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        cursor.create(rest_id)
        return RestServiceTransaction(storage,
                                      tx_rest_id=rest_id,
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

    def start(self, req_json) -> Optional[FlaskResponse]:
        logging.debug('RestServiceTransaction.start %s', self.http_host)
        assert 'host' not in req_json
        js = req_json
        js['host'] = self.http_host
        tx = TransactionMetadata.from_json(js)
        self.tx_cursor.write_envelope(tx)
        # todo should return oneshot get()?
        return None  # Response only on error

    def patch(self, req_json : Dict[str, Any]) -> FlaskResponse:
        resp_json = {}
        tx = TransactionMetadata.from_json(req_json)
        self.tx_cursor.write_envelope(tx)
        # todo should return oneshot get()?
        return FlaskResponse()

    # TODO pass a timeout possibly from request-timeout header e.g.
    # https://datatracker.ietf.org/doc/id/draft-thomson-hybi-http-timeout-00.html
    def get(self, req_json : Dict[str, Any]) -> FlaskResponse:
        resp_json = {}

        start = time.monotonic()
        while (time.monotonic() - start) < 1:
            # only wait if something is inflight upstream and we think the
            # status might change soon
            logging.info('RestServiceTransaction.get %s %s',
                         self.tx_cursor.tx.rcpt_to,
                         self.tx_cursor.tx.rcpt_response)
            if self.tx_cursor.status not in [
                    Status.INSERT, Status.INFLIGHT, Status.ONESHOT_INFLIGHT ]:
                break
            wait_mail = (self.tx_cursor.tx.mail_from is not None and
                         self.tx_cursor.tx.mail_response is None)
            wait_rcpt = (self.tx_cursor.tx.rcpt_to is not None and
                         (not(self.tx_cursor.tx.rcpt_response) or len(self.tx_cursor.tx.rcpt_response) < len(self.tx_cursor.tx.rcpt_to)))
            wait_data = (self.tx_cursor.last and
                         self.tx_cursor.tx.data_response is None)
            if not (wait_mail or wait_rcpt or wait_data):
                break
            logging.info('RestServiceTransaction.get wait')
            self.tx_cursor.wait(timeout=1)
            logging.info('RestServiceTransaction.get wait done')

        # xxx ever expected to have the resp without the req?
        resp_js = lambda r: r.to_json() if r is not None else None
        if self.tx_cursor.tx.mail_from is not None:
            resp_json['mail_response'] = resp_js(
                self.tx_cursor.tx.mail_response)
        if self.tx_cursor.tx.rcpt_to:
            resp_json['rcpt_response'] = [
                r.to_json() for r in self.tx_cursor.tx.rcpt_response]
        if self.tx_cursor.max_i is not None:
            resp_json['data_response'] = resp_js(
                self.tx_cursor.tx.data_response)

        logging.info('RestServiceTransaction.get %s', resp_json)

        # xxx flask jsonify() depends on app context which we may
        # not have in tests?
        rest_resp = FlaskResponse()
        rest_resp.set_data(json.dumps(resp_json))
        rest_resp.content_type = 'application/json'
        return rest_resp


    def append_blob(self, uri : Optional[str], last) -> FlaskResponse:
        logging.info('append_blob %s %s last=%s', self._tx_rest_id, uri, last)
        if uri:
            uri = uri.removeprefix('/blob/')  # XXX uri prefix
            if (self.tx_cursor.append_blob(blob_rest_id=uri, last=last) ==
                TransactionCursor.APPEND_BLOB_OK):
                resp_json = {}
                rest_resp = FlaskResponse()
                rest_resp.set_data(json.dumps(resp_json))
                rest_resp.content_type = 'application/json'
                return rest_resp

        blob_rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        writer = self.storage.get_blob_writer()
        writer.start(blob_rest_id)
        if (self.tx_cursor.append_blob(blob_rest_id=blob_rest_id, last=last) !=
            TransactionCursor.APPEND_BLOB_OK):
            return FlaskResponse(500, 'internal error')
        # XXX move to rest service?
        resp_json = {'uri': '/blob/' + blob_rest_id}
        rest_resp = FlaskResponse()
        rest_resp.set_data(json.dumps(resp_json))
        rest_resp.content_type = 'application/json'
        return rest_resp

    # TODO this is not idempotent -> etags
    def append(self, req_json : Dict[str, Any]) -> FlaskResponse:
        # storage enforces universal preconditions:
        # insert/oneshot_inflight/oneshot_temp
        # i.e. start didn't already permfail and haven't already set_durable

        # public rest submission always async, mx always sync

        # smtp->rest gateway is a special/trusted/well-known client
        # that is allowed to async for mx because of multi-recipient
        # cases, if it can be trusted to set the mx_multi_rcpt bit,
        # it can be trusted to keep appending after tempfail or not
        # appropriately

        # TODO for now, this always allows append even in
        # oneshot_temp, at such time as we want to accept rest mx
        # directly, we need a flag whether this req is from the
        # gateway and 400, etc.

        logging.info('RestServiceTransaction.append %s %s',
                     self._tx_rest_id, req_json)

        last = False
        if 'last' in req_json:
            if not isinstance(req_json['last'], bool):
                return FlaskResponse(status=400)
            last = req_json['last']

        # (short) inline
        if 'd' in req_json:
            # TODO investigate this further, this may be effectively
            # round-tripping utf8 -> python str -> utf8
            d : bytes = req_json['d'].encode('utf-8')
            self.tx_cursor.append_blob(d=d, last=last)
            return FlaskResponse()  # XXX range?

        # if 'uri' in req_json:  # xxx this would be a protocol change
        uri = req_json.get('uri', None)
        if uri is not None and not isinstance(uri, str):
            return FlaskResponse(status=400)

        return self.append_blob(uri, last)


    @staticmethod
    def build_resp(code, msg, writer):
        resp = FlaskResponse(status=code, response=[msg] if msg else None)
        logging.info('build_resp offset=%s length=%s',
                     writer.offset, writer.length)
        if writer.offset > 0:
            resp.headers.set(
                'content-range',
                ContentRange('bytes', 0, writer.offset, writer.length))
        return resp

    def put_blob(self, request : FlaskRequest,
                 content_range : ContentRange, range_in_headers : bool):
        logging.info('put_blob loaded %s off=%d len=%s',
                     self.blob_rest_id, self.blob_writer.offset,
                     self.blob_writer.length)

        # being extremely persnickety: we would reject
        # !range_in_headers after a request with it populated even if
        # it's equivalent/noop but may not be worth storing that to
        # enforce?

        offset = content_range.start
        if offset > self.blob_writer.offset:
            return RestServiceTransaction.build_resp(
                400, 'range start past the end', self.blob_writer)

        if (self.blob_writer.length is not None and
            content_range.length != self.blob_writer.length):
            return RestServiceTransaction.build_resp(
                400, 'append or !last after last', self.blob_writer)

        if self.blob_writer.offset >= content_range.stop:
            return RestServiceTransaction.build_resp(
                200, 'noop (range)', self.blob_writer)

        d = request.data[self.blob_writer.offset - offset:]
        assert len(d) > 0
        self.blob_writer.append_data(d, content_range.length)

        return RestServiceTransaction.build_resp(
            200, None, self.blob_writer)


    def abort(self):
        pass

    def set_durable(self, req_json : Dict[str, Any]) -> FlaskResponse:
        # TODO public rest mx: similar to append, disallow this in ONESHOT_TEMP

        try:
            self.tx_cursor.append_action(Action.SET_DURABLE)
        except InvalidActionException:
            if self.tx_cursor.status != Status.DONE:
                return FlaskResponse(
                    status=400, response=['failed precondition'])

            # set durable in done state means that it raced with something
            # else that terminated the transaction either upstream
            # success/perm or idle gc/abort

            # set durable succeeding is us "taking responsibility for the
            # message" wrt rfc5321 so we can only treat this as a noop if
            # it already succeeded

            actions = self.tx_cursor.load_last_action(1)

            if actions[0][1] != Action.DELIVERED:
                return FlaskResponse(
                    status=400, response=['failed precondition'])

        return FlaskResponse()


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
    while True:
        delta = last_tx.delta(cursor.tx)
        assert delta is not None
        logging.info('output max_i %s', cursor.max_i)
        if delta:
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
                cursor.set_mail_response(resp)
            if delta.rcpt_to:
                logging.debug('cursor_to_endpoint %s rcpt_to: %s',
                              cursor.rest_id, delta.rcpt_to)
                resp = delta.rcpt_response
                logging.debug('cursor_to_endpoint %s rcpt_resp %s',
                              cursor.rest_id, resp)

                cursor.add_rcpt_response(resp)
                for r in resp:
                    if r.ok():
                        ok_rcpt = True
                    elif err is None:
                        # XXX revisit in the context of the exploder,
                        # this is used downstream for append_action,
                        # what does that mean in the context of
                        # multi-rcpt?
                        err = r

            last_tx = cursor.tx

        if cursor.max_i is not None:
            break

        cursor.wait()

    if not ok_rcpt:
        return err

    i = 0
    last = False
    while not last:
        # TODO this is working around some waiting bug
        while True:
            logging.info('cursor_to_endpoint %s cursor.last=%s max_i=%s',
                         cursor.rest_id, cursor.last, cursor.max_i)

            # xxx hide this wait in TransactionCursor.read_content() ?
            if cursor.wait_for(lambda: (cursor.max_i is not None) and
                               (cursor.max_i >= i), 1):
                break

        # XXX this should just be sequential?
        blob = cursor.read_content(i)

        # XXX probably move to BlobReader
        if isinstance(blob, BlobReader):
            logging.info('cursor_to_endpoint %s wait blob %d',
                         cursor.rest_id, i)
            blob.wait()

        last = cursor.last and (i == cursor.max_i)
        logging.info('cursor_to_endpoint %s i=%d blob_len=%d '
                     'cursor.max_i=%s last=%s',
                     cursor.rest_id,
                     i, blob.len(), cursor.max_i, last)
        i += 1
        resp = endpoint.append_data(last, blob)
        logging.info('cursor_to_endpoint %s %s', cursor.rest_id, resp)

        if not last:
            # non-last must only return error
            assert(resp is None or resp.err())
        else:
            # last must return a response
            assert resp is not None

        if resp is not None:
            cursor.set_data_response(resp)
            return resp
    assert False  # unreached

def cursor_to_endpoint(cursor, endpoint):
    logging.debug('cursor_to_endpoint %s', cursor.rest_id)
    resp = output(cursor, endpoint)
    if resp is None:
        logging.warning('cursor_to_endpoint %s abort', cursor.rest_id)
        return
    logging.info('cursor_to_endpoint %s done %s', cursor.rest_id, resp)
    action = Action.TEMP_FAIL
    if resp.ok():
        action = Action.DELIVERED
    elif resp.perm():
        action = Action.PERM_FAIL
    cursor.append_action(action, resp)
