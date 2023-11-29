
from typing import Dict, Optional, Any, List, Tuple

from storage import Storage, TransactionCursor, BlobReader
from storage_schema import Action, Status, InvalidActionException
from response import Response
from blob import Blob

from flask import Request as FlaskRequest, Response as FlaskResponse, jsonify

from werkzeug.datastructures import ContentRange
import werkzeug.http

from rest_service_handler import Handler, HandlerFactory

import json

import secrets

import logging

REST_ID_BYTES = 4  # XXX configurable, use more in prod

# def dequeue():
#     created_id = None
#     while True:
#         cursor = None
#         if storage.wait_created(created_id, timeout=1):
#             cursor = storage.load()
#             if cursor.status == Status.INSERT:
#                 created_id = cursor.id
#         else:
#             cursor = self.storage.load_one(min_age=5)
#         if cursor:
#             executor.enqueue(lambda: handle(cursor))

#         # 1: abort idle INSERT/INFLIGHT !last
#         #   - idle ~10s of minutes
#         #   - in particular, submission may be able to continue after
#         #     crash/restart
#         #   - triggers wakeup/err return for readers
#         # (req completion logic drops payload for final)
#         # 2: ttl all completed transactions after ~days
#         # 3: unref'd blobs
#         storage.maybe_gc()

class RestServiceTransaction(Handler):
    http_host : str = None

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
        if not cursor.load(rest_id=rest_id): return None
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

    @staticmethod
    def trim_actions(actions):
        # these are in descending order
        for i in range(0, len(actions)):
            time, action, resp = actions[i]
            if action == Action.LOAD:
                return actions[0:(i+1)]
        return actions

    def tx_rest_id(self): return self._tx_rest_id

    def start(self, req_json) -> FlaskResponse:
        logging.debug('RestServiceTransaction.start %s', self.http_host)
        self.tx_cursor.write_envelope(
            local_host = req_json.get('local_host', None),
            remote_host = req_json.get('remote_host', None),
            mail_from = req_json.get('mail_from', None),
            transaction_esmtp = None,
            rcpt_to = req_json.get('rcpt_to', None),
            rcpt_esmtp = None,
            host = self.http_host)
        return FlaskResponse()


    @staticmethod
    def have_action(actions : List[Tuple[int, int, Optional[Response]]],
                    pred_actions : List[int]) -> bool:
        return next((x for x in actions if x[1] in pred_actions),
                    None) is not None

    def get(self, req_json : Dict[str, Any]) -> FlaskResponse:
        wait = False
        while True:
            # the exact number just needs to be an upper bound on max seq of
            # LOAD, START, SET_DURABLE, DELIVERED, etc
            actions = self.tx_cursor.load_last_action(5)
            actions = RestServiceTransaction.trim_actions(actions)
            logging.info('RestServiceTransaction.get actions %s', actions)

            resp_json = {}
            for timestamp,action,resp in reversed(actions):
                if action == Action.START:
                    resp_json['start_response'] = resp.to_json()
                elif (action in [ Action.DELIVERED,
                                  Action.TEMP_FAIL,
                                  Action.PERM_FAIL,
                                  Action.ABORT ]):
                    resp_json['final_status'] = resp.to_json()

            # not inflight: state unlikely to change if we wait
            if self.tx_cursor.status not in [
                    Status.INFLIGHT, Status.ONESHOT_INFLIGHT ]:
                break
            # already done
            if 'final_status' in resp_json:
                break
            # already waited once
            if wait: break

            # if we have the whole envelope: wait for start or final
            if (self.tx_cursor.rcpt_to is not None and
                not RestServiceTransaction.have_action(
                    actions, [Action.START])):
                wait = True
            # if we have the whole payload, wait for the final resp
            # XXX last doesn't entail any blob uploads have finished
            # but it's good enough for now
            if (self.tx_cursor.last and
                not RestServiceTransaction.have_action(actions, [
                    Action.DELIVERED, Action.TEMP_FAIL, Action.PERM_FAIL,
                    Action.ABORT ])):
                wait = True

            if not wait: break
            logging.info('RestServiceTransaction.get wait')
            self.tx_cursor.wait(timeout=1)
            logging.info('RestServiceTransaction.get wait done')

        # xxx flask jsonify() depends on app context which we may
        # not have in tests?
        rest_resp = FlaskResponse()
        rest_resp.set_data(json.dumps(resp_json))
        rest_resp.content_type = 'application/json'
        return rest_resp
        #return jsonify(resp_json)

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

    # TODO this is not idempotent, need to pass an offset/chunk# in
    # the req and pass down into the db
    def append(self, req_json : Dict[str, Any]) -> FlaskResponse:
        # mx enforces that start succeeded, etc.
        # storage enforces preconditions that apply to all transactions:
        #   e.g. tx didn't already fail/abort

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
                 range : ContentRange, range_in_headers : bool):
        logging.info('put_blob loaded %s off=%d len=%s',
                     self.blob_rest_id, self.blob_writer.offset,
                     self.blob_writer.length)

        # being extremely persnickety: we would reject
        # !range_in_headers after a request with it populated even if
        # it's equivalent/noop but may not be worth storing that to
        # enforce?

        offset = range.start
        if offset > self.blob_writer.offset:
                return RestServiceTransaction.build_resp(
                    400, 'range start past the end', self.blob_writer)
        last = range.length is not None and range.stop == range.length

        if (self.blob_writer.length is not None and
            range.length != self.blob_writer.length):
            return RestServiceTransaction.build_resp(
                400, 'append or !last after last', self.blob_writer)

        if self.blob_writer.offset >= range.stop:
            return RestServiceTransaction.build_resp(
                200, 'noop (range)', self.blob_writer)

        d = request.data[self.blob_writer.offset - offset:]
        assert(len(d) > 0)
        self.blob_writer.append_data(d, range.length)

        return RestServiceTransaction.build_resp(
            200, None, self.blob_writer)


    def abort(self):
        pass

    def set_durable(self, req_json : Dict[str, Any]) -> FlaskResponse:
        try:
            self.tx_cursor.append_action(Action.SET_DURABLE)
        except InvalidActionException:
            if self.tx_cursor.status != Status.DONE:
                return FlaskResponse(status=400, response=['failed precondition'])

            # set durable in done state means that it raced with something
            # else that terminated the transaction either upstream
            # success/perm or idle gc/abort

            # set durable succeeding is us "taking responsibility for the
            # message" wrt rfc5321 so we can only treat this as a noop if
            # it already succeeded

            actions = self.tx_cursor.load_last_action(1)

            if actions[0][1] != Action.DELIVERED:
                return FlaskResponse(status=400, response=['failed precondition'])

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
    if not cursor.wait_attr_not_none('mail_from'):
        return None
    if not cursor.wait_attr_not_none('rcpt_to'):
        return None

    logging.debug('cursor_to_endpoint %s from=%s to=%s', cursor.rest_id,
                  cursor.mail_from, cursor.rcpt_to)

    resp = endpoint.start(
        cursor.remote_host, cursor.local_host,
        cursor.mail_from, cursor.transaction_esmtp,
        cursor.rcpt_to, cursor.rcpt_esmtp)
    logging.debug('cursor_to_endpoint %s start resp %s', cursor.rest_id, resp)
    if resp.err():
        return resp

    cursor.append_action(Action.START, resp)

    i = 0
    last = False
    while not last:
        logging.info('cursor_to_endpoint %s cursor.last=%s max_i=%s',
                     cursor.rest_id, cursor.last, cursor.max_i)

        # xxx hide this wait in TransactionCursor.read_content() ?
        cursor.wait_for(lambda: (cursor.max_i is not None) and
                        (cursor.max_i >= i))  # or abort

        # XXX this should just be sequential?
        blob = cursor.read_content(i)

        # XXX probably move to BlobReader
        if isinstance(blob, BlobReader):
            logging.info('cursor_to_endpoint %s wait blob %d',
                         cursor.rest_id, i)
            blob.wait()

        last = cursor.last and (i == cursor.max_i)
        logging.info('cursor_to_endpoint %s i=%d blob_len=%d '
                     'cursor.max_i=%d last=%s',
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
            assert(resp is not None)

        if resp is not None:
            return resp
    assert(False)  # unreached

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
