from typing import Any, Callable, Dict, Optional, Tuple
import logging
import json
import secrets
import time

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import Filter, HostPort, Mailbox, TransactionMetadata, WhichJson
from dsn import read_headers, generate_dsn
from blob import InlineBlob

def default_notification_factory():
    raise NotImplementedError()

class OutputHandler:
    cursor : TransactionCursor
    endpoint : Filter
    rest_id : str
    notification_factory : Callable[[], Filter]
    mailer_daemon_mailbox : Optional[str] = None
    notifications_enabled : bool

    def __init__(self, cursor : TransactionCursor, endpoint : Filter,
                 downstream_env_timeout=None,
                 downstream_data_timeout=None,
                 notification_factory = default_notification_factory,
                 mailer_daemon_mailbox : Optional[str] = None,
                 notifications_enabled = True,
                 retry_params : Optional[dict] = {}):
        self.cursor = cursor
        self.endpoint = endpoint
        self.rest_id = self.cursor.rest_id
        self.env_timeout = downstream_env_timeout
        self.data_timeout = downstream_data_timeout
        self.notification_factory = notification_factory
        self.mailer_daemon_mailbox = mailer_daemon_mailbox
        self.notifications_enabled = notifications_enabled
        self.retry_params = retry_params

    def _output(self) -> Optional[Response]:
        logging.debug('OutputHandler._output() %s', self.rest_id)

        last_tx = TransactionMetadata()
        ok_rcpt = False
        rcpt0_resp = None

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
                # all recipients so far have failed and we aren't
                # waiting for any more (have_body) -> we're done
                if len(self.cursor.tx.rcpt_to) == 1:
                    return rcpt0_resp
                else:
                    # placeholder response, this is only used for
                    # notification logic for
                    # post-exploder/single-recipient tx
                    return Response(400, 'OutputHandler all recipients failed')

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
                while True:
                    try:
                        self.cursor.set_mail_response(resp)
                    except VersionConflictException:
                        self.cursor.load()
                        continue
                    break
                if resp.err():
                    return resp
                last_tx.mail_from = delta.mail_from

            if delta.rcpt_to:
                resp = delta.rcpt_response
                if rcpt0_resp is None:
                    rcpt0_resp = resp[0]
                logging.debug('OutputHandler._output() %s rcpt_to: %s resp %s',
                              self.rest_id, delta.rcpt_to,
                              [str(r) for r in resp])
                assert len(resp) == len(delta.rcpt_to)
                logging.debug('OutputHandler._output() %s rcpt_resp %s',
                              self.rest_id, [str(r) for r in resp])
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
                if delta.data_response.err():
                    return delta.data_response

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

        logging.debug('OutputHandler.cursor_to_endpoint() %s attempt %d max %s',
                      self.rest_id, self.cursor.attempt_id,
                      self.cursor.tx.max_attempts)

        next_attempt_time = None
        final_attempt_reason = None
        if resp.ok():
            final_attempt_reason = 'upstream response success'
        elif resp.perm():
            final_attempt_reason = 'upstream response permfail'
        else:
            final_attempt_reason, next_attempt_time = self._next_attempt_time(
                time.time())

        self._maybe_send_notification(resp, final_attempt_reason is not None)

        while True:
            try:
                delta = TransactionMetadata()
                delta.attempt_count = self.cursor.attempt_id
                self.cursor.write_envelope(
                    delta,
                    final_attempt_reason = final_attempt_reason,
                    next_attempt_time = next_attempt_time,
                    finalize_attempt = True)
                break
            except VersionConflictException:
                self.cursor.load()

    def _next_attempt_time(self, now) -> Tuple[Optional[str], Optional[int]]:
        max_attempts = self.cursor.tx.max_attempts
        if max_attempts is None:
            max_attempts = self.retry_params.get('max_attempts', 30)
        if max_attempts is not None and (
                self.cursor.attempt_id >= max_attempts):
            return 'retry policy max attempts', None
        dt = now - self.cursor.creation
        logging.debug('OutputHandler._next_attempt_time %s %d',
                      self.rest_id, int(dt))
        dt = dt * self.retry_params.get('backoff_factor', 1.5)
        dt = min(dt, self.retry_params.get('max_attempt_time', 3600))
        dt = max(dt, self.retry_params.get('min_attempt_time', 60))
        next = int(now + dt)
        # TODO jitter?
        logging.debug('OutputHandler._next_attempt_time %s %d %d',
                      self.rest_id, int(dt), next)
        deadline = self.cursor.tx.deadline
        if deadline is None:
            deadline = self.retry_params.get('deadline', 86400)
        if deadline is not None and (dt > deadline):
            return 'retry policy deadline', None
        return None, next

    def _maybe_send_notification(self, resp, last_attempt : bool):
        logging.debug('OutputHandler._maybe_send_notification '
                      '%s enabled %s last %s',
                      self.rest_id, self.notifications_enabled, last_attempt)

        if not self.notifications_enabled:
            return
        if self.cursor.tx.notifications is None:
            return
        if resp.ok():
            return
        if resp.temp() and not last_attempt:
            return

        mail_from = self.cursor.tx.mail_from
        if mail_from.mailbox == '':
            return

        orig_headers = b'\r\n\r\n'
        if self.cursor.tx.message_builder:
            # TODO save MessageBuilder-rendered headers
            msgid = self.cursor.tx.message_builder.get(
                'headers', {}).get('message-id', None)
            if msgid:
                orig_headers = b'Message-ID: <' + msgid + b'>\r\n\r\n'
        elif self.cursor.tx.body is not None:
            blob_reader = self.cursor.parent.get_blob_reader()
            blob_reader.load(rest_id = self.cursor.tx.body)
            orig_headers = read_headers(blob_reader)

        dsn = generate_dsn(self.mailer_daemon_mailbox,
                           mail_from.mailbox,
                           self.cursor.tx.rcpt_to[0].mailbox, orig_headers,
                           received_date=self.cursor.creation,
                           now=int(time.time()),
                           response=resp)

        # TODO link these transactions somehow
        # self.cursor.id should end up in this tx and this tx id
        # should get written back to self.cursor
        notify_json = self.cursor.tx.notifications
        notification_tx = TransactionMetadata(
            host=notify_json['host'],
            mail_from=Mailbox(''),
            # TODO may need to save some esmtp e.g. SMTPUTF8
            rcpt_to=[Mailbox(mail_from.mailbox)],
            body_blob = InlineBlob(dsn),
            max_attempts = notify_json.get('max_attempts', 10))
        notification_endpoint = self.notification_factory()
        # timeout=0 i.e. fire&forget, don't wait for upstream
        # but internal temp (e.g. db write fail, should be uncommon)
        # should result in the parent retrying even if it was
        # permfail, better to dupe than fail to emit the bounce
        notification_endpoint.on_update(notification_tx, timeout=0)
