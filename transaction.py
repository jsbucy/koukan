
from typing import Dict,Optional

from storage import Storage, Action, Status
from response import Response
from blob import Blob

from flask import Request as FlaskRequest, Response as FlaskResponse, jsonify

from werkzeug.datastructures import ContentRange
import werkzeug.http


import json

import secrets

import logging

REST_ID_BYTES = 4  # XXX configurable, use more in prod

# interface to "outer" rest service
#class TransactionWriterFactory:
#    def create(self) -> TransactionWriter:
#        pass
#    def get(self, rest_id) -> TransactionWriter:
#        pass

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

class RestServiceTransaction:
    def __init__(self, storage, rest_id, cursor):
        self.storage = storage
        self.rest_id = rest_id
        self.cursor = cursor

    @staticmethod
    def create(storage, req_json):
        cursor = storage.get_transaction_cursor()
        rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        cursor.create(rest_id)

        cursor.write_envelope(
            local_host = req_json.get('local_host', None),
            remote_host = req_json.get('remote_host', None),
            mail_from = req_json.get('mail_from', None),
            transaction_esmtp = None,
            rcpt_to = req_json.get('rcpt_to', None),
            rcpt_esmtp = None,
            host = None)  # XXX

        return RestServiceTransaction(storage, rest_id, cursor)

    @staticmethod
    def trim_actions(actions):
        for i in range(len(actions) - 1, 0, -1):
            time, action, resp = actions[i]
            if action == Action.LOAD:
                actions = actions[i:]
                break

    def get(self, req_json) -> FlaskResponse:
        # wait/timeout: only if inflight?

        # fn is just version+1, i.e. just retry on every write, there
        # shouldn't be that many
        while True:
            actions = self.cursor.load_last_action(2)
            RestServiceTransaction.trim_actions(actions)
            if not actions:
                if not self.cursor.wait():
                    return FlaskResponse()
                continue
            resp_json = {}
            for timestamp,action,resp in actions:
                if action == Action.START:
                    resp_json['start_response'] = resp.to_json()
                elif (action == Action.DELIVERED or
                      action == Action.TEMP_FAIL or
                      action == Action.PERM_FAIL):
                    resp_json['final_status'] = resp.to_json()
            # xxx flask jsonify() depends on app context which we may
            # not have in tests?
            rest_resp = FlaskResponse()
            rest_resp.set_data(json.dumps(resp_json))
            rest_resp.content_type = 'application/json'
            return rest_resp
            #return jsonify(resp_json)

    def append_inline(self, d, last)  -> FlaskResponse:
        self.cursor.append_data(d)
        if last:
            self.cursor.finalize_payload()
        return FlaskResponse()

    def append_blob(self, uri : str) -> FlaskResponse:
        logging.info('append_blob %s %s', self.rest_id, uri)
        if uri:
            reader = self.storage.get_blob_reader()
            if reader.start(rest_id=uri):
                self.cursor.append_blob(uri, None)  # xxx length ignored
                resp_json = {}
                rest_resp = FlaskResponse()
                rest_resp.set_data(json.dumps(resp_json))
                rest_resp.content_type = 'application/json'
                return rest_resp

        blob_rest_id = secrets.token_urlsafe(REST_ID_BYTES)
        writer = self.storage.get_blob_writer()
        writer.start(blob_rest_id)
        self.cursor.append_blob(blob_rest_id, None)  # xxx length ignored
        resp_json = {'uri': blob_rest_id}
        rest_resp = FlaskResponse()
        rest_resp.set_data(json.dumps(resp_json))
        rest_resp.content_type = 'application/json'
        return rest_resp

    def append(self, req_json) -> FlaskResponse:
        # mx enforces that start succeeded, etc.
        # storage enforces preconditions that apply to all transactions:
        #   e.g. tx didn't already fail/abort

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
            return self.append_inline(d, last)
        if 'uri' in req_json:
            uri = req_json['uri']
            if uri is None or isinstance(uri, str):
                return self.append_blob(uri)

        return FlaskResponse(status=400)

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

    def put_blob(self, blob_id, request):
        logging.info('put_blob %s', blob_id)
        writer = self.storage.get_blob_writer()
        if writer.load(rest_id=blob_id) is None:
            return FlaskResponse(status=404)
        logging.info('put_blob loaded %s off=%d len=%s',
                     blob_id, writer.offset, writer.length)
        range = None
        if 'content-range' not in request.headers:
            logging.info('put_blob content-length=%d (no range)',
                         request.content_length)
            range = ContentRange('bytes', 0, request.content_length,
                                 request.content_length)
        else:
            range = werkzeug.http.parse_content_range_header(
                request.headers.get('content-range'))
            logging.info('put_blob content-range: %s', range)
            if not range or range.units != 'bytes':
                return RestServiceTransaction.build_resp(
                    400, 'bad range', writer)

        offset = range.start
        if offset > writer.offset:
                return RestServiceTransaction.build_resp(
                    400, 'range start past the end', writer)
        last = range.length is not None and range.stop == range.length

        if writer.length is not None and range.length != writer.length:
            return RestServiceTransaction.build_resp(
                400, 'append or !last after last', writer)

        if writer.offset >= range.stop:
            return RestServiceTransaction.build_resp(
                200, 'noop (range)', writer)

        d = request.data[writer.offset - offset:]
        assert(len(d) > 0)
        writer.append_data(d, range.length)

        return RestServiceTransaction.build_resp(
            200, None, writer)



def cursor_to_endpoint(cursor, endpoint):
    #if cursor.has_upstream_tx_url():
    #    endpoint.set_transaction_url(cursor.upstream_tx_url)
    #    resp = endpoint.get_status()
    #    if resp.err():
    #        endpoint = None
    #if endpoint is None:
    #    endpoint = endpoint()
    #    cursor.set_upstream_rest_id(endpoint.rest_id)

    cursor.wait_attr_not_none('mail_from')  # or abort
    cursor.wait_attr_not_none('rcpt_to')  # or abort

    resp = endpoint.start(
        cursor.remote_host, cursor.local_host,
        cursor.mail_from, cursor.transaction_esmtp,
        cursor.rcpt_to, cursor.rcpt_esmtp)
    # storage has business logic to know start + err resp -> DONE tx?
    cursor.append_action(Action.START, resp)
    if resp.err():
        return
    i = 0
    last = False
    while not last:
        cursor.wait_for(lambda: cursor.max_i >= i)  # or abort

        # TODO somewhere (here or BlobReader) needs to be some code to
        # wait for blob.last, etc.
        blob = cursor.read_content(i)

        logging.info('cursor_to_endpoint i=%d blob_len=%d '
                     'cursor.max_i=%d cursor.last=%s',
                     i, blob.len(), cursor.max_i, cursor.last)
        last = cursor.last and (i == cursor.max_i)
        i += 1
        logging.info('cursor_to_endpoint last=%s', last)
        resp = endpoint.append_data(last, blob)
        # blob.unref()

        if not last:
            # non-last must only return error
            assert(resp is None or resp.err())
        else:
            # last must return a response
            assert(resp is not None)

        if resp is not None:
            # XXX it seems like these fine-grained codes are redundant
            # with the resp, this is a vestige of not originally
            # storing the resp? does anything directly reading the db
            # distinguish those? or do you want it to capture things
            # like upstream temp + max retry -> perm? or maybe that
            # should be an additional action? conceptually that is
            # a separate attempt LOAD, TEMP, LOAD, PERM
            # though in practice the completion logic writes that?
            action = Action.TEMP_FAIL
            if resp.ok():
                action = Action.DELIVERED
            elif resp.perm():
                action = Action.PERM_FAIL
            cursor.append_action(action, resp)
            return
    assert(False)  # unreached
