from typing import Dict, Optional, Any
import logging
import json
import secrets
import time

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import Filter, HostPort, Mailbox, TransactionMetadata, WhichJson


class OutputHandler:
    cursor : TransactionCursor
    endpoint : Filter
    rest_id : str

    def __init__(self, cursor : TransactionCursor, endpoint : Filter,
                 downstream_env_timeout=None,
                 downstream_data_timeout=None):
        self.cursor = cursor
        self.endpoint = endpoint
        self.rest_id = self.cursor.rest_id
        self.env_timeout = downstream_env_timeout
        self.data_timeout = downstream_data_timeout

    def _output(self) -> Optional[Response]:
        logging.debug('OutputHandler._output() %s', self.rest_id)

        last_tx = TransactionMetadata()
        err = None
        ok_rcpt = False

        while True:
            delta = last_tx.delta(self.cursor.tx)
            # XXX delta() outputs response fields but we don't really
            # say if it's a precondition of Filter.on_update() that
            # those fields are unset
            delta.mail_response = None
            delta.rcpt_response = []
            assert delta is not None
            logging.info('OutputHandler._output() %s '
                         'mail_from = %s '
                         'rcpt_to = %s body = %s message_builder = %s '
                         'input_done = %s',
                         self.rest_id, delta.mail_from, delta.rcpt_to,
                         delta.body, delta.message_builder,
                         self.cursor.input_done)
            have_body = ((self.cursor.input_done) and
                         ((delta.body is not None) or
                          (delta.message_builder is not None)))
            if (not delta.rcpt_to) and (not ok_rcpt) and have_body:
                return
            if ((delta.mail_from is None) and (not delta.rcpt_to) and
                (not have_body)):
                # xxx env vs data timeout
                if not self.cursor.wait(self.env_timeout):
                    logging.debug(
                        'OutputHandler._output() %s timeout %d',
                        self.rest_id, self.env_timeout)

                    return Response(
                        400, 'OutputHandler downstream env timeout')
                continue

            body_blob = None
            if self.cursor.input_done and delta.body:
                blob_reader = self.cursor.parent.get_blob_reader()
                blob_reader.load(rest_id = self.cursor.tx.body)
                assert blob_reader.length == blob_reader.content_length()
                delta.body_blob = blob_reader

            # TODO possibly dead code, presence of message builder
            # entails input_done?
            if not self.cursor.input_done and delta.message_builder:
                del delta.message_builder

            assert delta.mail_from or delta.rcpt_to or (
                self.cursor.input_done and (
                    delta.body_blob or delta.message_builder))

            self.endpoint.on_update(delta)
            if delta.mail_from:
                logging.debug('OutputHandler._output() %s mail_from: %s',
                              self.rest_id, delta.mail_from.mailbox)
                resp = delta.mail_response
                if not resp:
                    # XXX fix upstream and assert
                    logging.warning(
                        'OutputHandler._output(): output chain didn\'t set '
                        'mail response')
                    resp = Response()
                else:
                    logging.debug('OutputHandler._output() %s mail_resp %s',
                                  self.rest_id, resp)
                if resp.err():
                    err = resp
                while True:
                    try:
                        self.cursor.set_mail_response(resp)
                    except VersionConflictException:
                        self.cursor.load()
                        continue
                    break
                last_tx.mail_from = delta.mail_from

            if delta.rcpt_to:
                resp = delta.rcpt_response
                logging.debug('OutputHandler._output() %s rcpt_to: %s resp %s',
                              self.rest_id, delta.rcpt_to, resp)
                assert len(resp) == len(delta.rcpt_to)
                logging.debug('OutputHandler._output() %s rcpt_resp %s',
                              self.rest_id, resp)
                while True:
                    try:
                        self.cursor.add_rcpt_response(resp)
                    except VersionConflictException:
                        self.cursor.load()
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

            # data response is ~undefined on update where all rcpts failed
            if self.cursor.input_done and (
                    delta.body_blob or delta.message_builder) and ok_rcpt:
                last_tx.body_blob = delta.body_blob
                last_tx.message_builder = delta.message_builder
                try:
                    self.cursor.set_data_response(delta.data_response)
                    return delta.data_response
                except VersionConflictException:
                    self.cursor.load()
                    continue

    def cursor_to_endpoint(self):
        logging.debug('OutputHandler.cursor_to_endpoint() %s', self.rest_id)
        resp = self._output()
        # TODO need another attempt column for internal errors?
        if resp is None:
            logging.warning('OutputHandler.cursor_to_endpoint() %s abort',
                            self.rest_id)
            resp = Response(400, 'output handler abort')
        logging.info('OutputHandler.cursor_to_endpoint() %s done %s',
                     self.rest_id, resp)
        while True:
            try:
                self.cursor.finalize_attempt(not resp.temp())
                break
            except VersionConflictException:
                self.cursor.load()
