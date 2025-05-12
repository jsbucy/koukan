# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Callable, Dict, Optional, Tuple, Union
import json
import logging
import secrets
import time

from koukan.backoff import backoff
from koukan.storage import Storage, TransactionCursor, BlobCursor
from koukan.storage_schema import VersionConflictException
from koukan.response import Response
from koukan.filter import (
    AsyncFilter,
    HostPort,
    Mailbox,
    SyncFilter,
    TransactionMetadata,
    WhichJson )
from koukan.dsn import read_headers, generate_dsn
from koukan.blob import Blob, InlineBlob
from koukan.rest_schema import BlobUri
from koukan.message_builder import MessageBuilder, MessageBuilderSpec

def default_notification_factory():
    raise NotImplementedError()

class OutputHandler:
    cursor : TransactionCursor
    endpoint : SyncFilter
    rest_id : str
    notification_factory : Callable[[], AsyncFilter]
    mailer_daemon_mailbox : Optional[str] = None
    retry_params : dict
    prev_tx : TransactionMetadata
    tx : TransactionMetadata

    def __init__(self,
                 cursor : TransactionCursor,
                 endpoint : SyncFilter,
                 downstream_env_timeout=None,
                 downstream_data_timeout=None,
                 notification_factory = default_notification_factory,
                 mailer_daemon_mailbox : Optional[str] = None,
                 retry_params : dict = {}):
        self.cursor = cursor
        self.endpoint = endpoint
        self.rest_id = self.cursor.rest_id
        self.env_timeout = downstream_env_timeout
        self.data_timeout = downstream_data_timeout
        self.notification_factory = notification_factory
        self.mailer_daemon_mailbox = mailer_daemon_mailbox
        self.retry_params = retry_params
        self.prev_tx = TransactionMetadata()
        self.tx = TransactionMetadata()

    def _wait_downstream(self, delta : TransactionMetadata,
                         ok_rcpt : bool
                         ) -> Tuple[Optional[bool], Optional[Response]]:
        # xxx req_inflight()?
        # xxx delta vs cursor.tx?
        body = self.cursor.tx.body
        assert isinstance(body, Union[None, Blob, MessageBuilderSpec])
        if ((delta.mail_from is not None) or
            bool(delta.rcpt_to) or
            ((body is not None) and body.finalized())):
            return False, None

        timeout = self.env_timeout
        msg = 'envelope'
        if ok_rcpt:
            timeout = self.data_timeout
            msg = 'body'

        if not self.cursor.wait(timeout):
            logging.debug(
                'OutputHandler._output() %s timeout %s=%d',
                self.rest_id, msg, timeout)
            return None, Response(
                400, 'OutputHandler downstream timeout (%s=%d)' % (
                    msg, timeout))
        self.cursor.load()
        return True, None

    def _output(self) -> Optional[Response]:
        ok_rcpt = False
        # full vector after last upstream update
        last_tx = None
        upstream_tx = None

        # TODO clean up control flow
        # while True:
        #   # first call or small chance write_envelope will return new reqs
        #   if not cursor.tx.req_inflight():
        #     prev = cursor.tx.copy()
        #     wait_downstream()
        #     delta = prev.delta(cursor.tx)
        #     precondition check: e.g. delta.body after all rcpts failed
        #       delta = cancel -> upstream
        #       return precondition error downstream

        #   upstream_delta = output_chain.on_update(tx, delta)
        #   done = mail_resp.err or data_response or req_inflight()  # bug
        #   if done:
        #     final attempt reason/notification logic
        #   write_envelope(upstream_delta)
        #   if done:
        #     break

        while True:
            logging.debug('OutputHandler._output() %s db tx %s input done %s',
                          self.rest_id, self.cursor.tx, self.cursor.input_done)

            cursor_tx = self.cursor.tx.copy()
            # NOTE: possible to read finalized body but input_done == False
            if last_tx is None:
                last_tx = cursor_tx.copy()
                upstream_tx = cursor_tx.copy()
                delta = last_tx.copy()
            else:
                delta = last_tx.delta(cursor_tx)
                last_tx = cursor_tx.copy()
                assert delta is not None
                assert upstream_tx.merge_from(delta) is not None

            assert len(last_tx.rcpt_to) >= len(last_tx.rcpt_response)
            assert len(upstream_tx.rcpt_to) >= len(upstream_tx.rcpt_response)

            # XXX drop some fields from upstream_tx:
            # notification, retry
            # these have REST_CREATE/... validity but we are handling it here
            for field in ['notification', 'retry', 'final_attempt_reason']:
                for obj in [last_tx, upstream_tx, delta]:
                    if getattr(obj, field) is not None:
                        delattr(obj, field)

            # have_body here is really a proxy for "envelope done"
            have_body = self.cursor.tx.body is not None
            if ((not delta.rcpt_to) and (not ok_rcpt) and have_body) or (
                    delta.cancelled):
                delta = TransactionMetadata(cancelled=True)
                assert upstream_tx.merge_from(delta) is not None
                self.endpoint.on_update(upstream_tx, delta)

                # all recipients so far have failed and we aren't
                # waiting for any more (have_body) -> we're done
                if len(self.cursor.tx.rcpt_to) == 1:
                    return self.cursor.tx.rcpt_response[0]
                else:
                    # placeholder response, this is only used for
                    # notification logic for
                    # post-exploder/single-recipient tx
                    return Response(400, 'OutputHandler all recipients failed')

            waited, err = self._wait_downstream(delta, ok_rcpt)
            if err is not None:
                return err
            elif waited:
                continue

            logging.info('OutputHandler._output() %s body %s',
                         self.rest_id, upstream_tx.body)

            # no new reqs in delta can happen e.g. if blob upload
            # ping'd last_update
            if delta.mail_from is None and not delta.rcpt_to and not delta.body:
                logging.info('OutputHandler._output() %s no reqs', self.rest_id)
                continue

            upstream_delta = self.endpoint.on_update(upstream_tx, delta)
            logging.info('OutputHandler._output() %s '
                         'tx after upstream update %s',
                         self.rest_id, upstream_tx)

            if upstream_delta is None or upstream_tx.req_inflight():
                logging.warning('OutputHandler._output() %s '
                                'BUG empty upstream_delta %s',
                                self.rest_id, upstream_delta)
                return None  # internal error
            assert len(upstream_tx.rcpt_response) <= len(upstream_tx.rcpt_to)

            # if we're short-circuiting to ourselves, these fields will clash
            for field in ['final_attempt_reason', 'attempt_count']:
                for t in [upstream_tx, upstream_delta]:
                    if getattr(t, field) is not None:
                        delattr(t, field)

            for i in range(0,5):
                try:
                    self.cursor.write_envelope(upstream_delta)
                    assert last_tx.merge_from(upstream_delta) is not None
                    break
                except VersionConflictException:
                    logging.info(
                        'OutputHandler._output() VersionConflictException %s ',
                        self.rest_id)
                    if i == 4:
                        raise
                    backoff(i)
                    self.cursor.load()

            if (upstream_delta.mail_response is not None and
                upstream_delta.mail_response.err()):
                return upstream_delta.mail_response
            if not ok_rcpt and any(
                    [r.ok() for r in upstream_delta.rcpt_response]):
                ok_rcpt = True
            if upstream_delta.data_response is not None:
                return upstream_delta.data_response

    def handle_old(self):
        logging.info('OutputHandler.handle() %s', self.rest_id)

        try:
            final_attempt_reason = None
            # _cursor_to_endpoint() could be terminated by an uncaught
            # exception due to a bug. We don't know whether this is
            # due to something in the tx deterministically tickling a
            # bug in the code or due to a bug handling a transient
            # failure (e.g. the executor overflow thing w/exploder
            # which we handled in 2eb7a4f0 but there are undoubtedly
            # others) that would succeed if you retry. To try to split
            # the difference on this, preload next_attempt_time with
            # 1h so that transient errors will eventually get retried
            # but we won't crashloop excessively on a bad tx. This is
            # a last resort against bugs.
            next_attempt_time = time.time() + self.retry_params.get(
                'bug_retry', 3600)
            delta = TransactionMetadata()
            notification_done = False

            if not self.cursor.no_final_notification:
                final_attempt_reason, next_attempt_time = (
                    self._cursor_to_endpoint())
                # xxx why is this here?
                delta.attempt_count = self.cursor.attempt_id
            else:
                # post facto notification
                # leave the existing value
                final_attempt_reason = None

            if self.cursor.tx.notification:
                self._maybe_send_notification(final_attempt_reason)
                notification_done = True
        except Exception as e:
            logging.exception('OutputHandler.handle(): _cursor_to_endpoint()')
        finally:
            for i in range(0,5):
                try:
                    # TODO it probably wouldn't be hard to merge this
                    # write with the one at the end of _output()

                    # postcondition of _cursor_to_endpoint() is that all
                    # reqs have responses. Populate with a temp err here
                    # in case of a bug, otherwise the downstream will hang
                    # up to some timeout waiting for a response that will
                    # never come.
                    err = TransactionMetadata()
                    err_resp = Response(
                        450, 'internal error: OutputHandler failed to populate response')
                    self.cursor.tx.fill_inflight_responses(err_resp, err)

                    if (self.cursor.tx.body and self.cursor.input_done and
                        self.cursor.tx.data_response is None):
                        self.cursor.tx.data_response = err_resp
                    if err:
                        logging.warning('missing responses %s', self.cursor.tx)
                        delta.merge_from(err)
                    self.cursor.write_envelope(
                        delta,
                        final_attempt_reason=final_attempt_reason,
                        next_attempt_time=next_attempt_time,
                        finalize_attempt=True,
                        # True clears no_final_notification
                        notification_done=notification_done)
                    break
                except VersionConflictException:
                    if i == 4:
                        raise
                    backoff(i)
                    self.cursor.load()

    def _cursor_to_endpoint(self) -> Tuple[Optional[str], Optional[int]]:
        logging.debug('OutputHandler._cursor_to_endpoint() %s', self.rest_id)

        resp = self._output()
        if resp is None:
            logging.warning('OutputHandler._cursor_to_endpoint() %s abort',
                            self.rest_id)
            resp = Response(400, 'output handler abort')
        logging.info('OutputHandler._cursor_to_endpoint() %s done %s',
                     self.rest_id, resp)

        next_attempt_time = None
        final_attempt_reason = None
        if self.cursor.tx.cancelled:
            pass  # leave existing value
        elif resp.ok():
            final_attempt_reason = 'upstream response success'
        elif resp.perm():
            final_attempt_reason = 'upstream response permfail'
        elif (not self.cursor.input_done) and (self.cursor.tx.retry is None):
            # if we early-returned due to upstream temp err before
            # receiving all of the body, don't set
            # final_attempt_reason if retries are enabled so it is
            # recoverable at such time as we get the rest of the
            # body/input_done
            final_attempt_reason = 'downstream timeout'
        else:
            # assert len(self.cursor.tx.rcpt_to) == 1
            # assert self.cursor.tx.body.finalized()
            final_attempt_reason, next_attempt_time = (
                self._next_attempt_time(time.time()))

        logging.debug(
            'OutputHandler._cursor_to_endpoint() %s attempt %d retry %s '
            'final_attempt_reason %s',
            self.rest_id, self.cursor.attempt_id, self.cursor.tx.retry,
            final_attempt_reason)

        return final_attempt_reason, next_attempt_time

    # one iteration of output
    # - wait for new downstream if necessary
    # - emit to output chain
    # - completion/next attempt/notification logic
    # -> delta, kwargs for write_envelope()
    def _handle_once(self) -> Tuple[TransactionMetadata, dict]:
        upstream_delta = None
        # cursor.no_final_notification: tx.notification was None when
        # final_attempt_reason was written iow notifications were enabled after
        # the tx already failed (Exploder)

        # very first call or there's a small chance
        # we may have picked up new downstream from the previous
        # write_envelope()
        downstream_timeout = False
        logging.debug(self.tx)
        if (not self.cursor.no_final_notification and
            not self.cursor.input_done and
            not self.prev_tx.delta(self.tx) and  # no new downstream
            not self.tx.cancelled):
            if not self.cursor.wait(self.env_timeout):  # xxx
                logging.debug('downstream timeout')
                downstream_timeout = True
            else:
                self.tx = self.cursor.load().copy()
        # drop some fields from the tx that's going upstream
        # OH consumes these fields but they should not propagate to the
        # output chain/upstream rest/gateway etc
        for field in ['notification', 'retry', 'final_attempt_reason']:
            if getattr(self.tx, field) is not None:
                delattr(self.tx, field)

        logging.debug('_handle_once %s', self.tx)
        delta = self.prev_tx.delta(self.tx)
        if (self.tx.body is not None and
            len(self.tx.rcpt_response) == len(self.tx.rcpt_to) and
            not any([r.ok() for r in self.tx.rcpt_response])):
            upstream_delta = TransactionMetadata(
                data_response=Response(450, 'precondition failed: '
                                       'no valid rcpt (OutputHandler)'))
            self.tx.merge_from(upstream_delta)
        elif self.tx.req_inflight() or self.tx.cancelled:
            upstream_delta = self.endpoint.on_update(self.tx, delta)
            logging.debug(self.cursor.tx)
            self.prev_tx = self.tx.copy()
            # postcondition check: !req_inflight() in handle() finally:
        else:
            upstream_delta = TransactionMetadata()

        done = False
        final_attempt_reason = None
        if self.cursor.no_final_notification:
            done = True
        if self.tx.cancelled:
            done = True
            final_attempt_reason = 'cancelled'
        if not done and downstream_timeout:
            done = True
            final_attempt_reason = 'downstream timeout'

        final_response = None
        if not done and (self.tx.mail_response is not None) and self.tx.mail_response.err():
            done = True
            final_response = self.tx.mail_response
        # for interactive smtp/downstream exploder, final rcpt error will be
        # followed by cancellation (or possibly timeout if the gateway crashed)
        if not done and self.tx.data_response is not None:
            done = True
            final_response = self.tx.data_response
        done = done or self.tx.req_inflight()  # bug
        if not done:
            return upstream_delta, {}

        kwargs = {}

        if final_attempt_reason is None and final_response is not None and (
                final_response.ok() or final_response.perm()):
            if final_response.ok():
                final_attempt_reason = 'upstream response success'
            elif final_response.perm():
                final_attempt_reason = 'upstream response permfail'
        if final_attempt_reason is None and not self.cursor.no_final_notification:
            final_attempt_reason, kwargs['next_attempt_time'] = (
                self._next_attempt_time(time.time()))
        kwargs['final_attempt_reason'] = final_attempt_reason

        if self.cursor.tx.notification is not None:
            self._maybe_send_notification(final_attempt_reason)
            kwargs['notification_done'] = True
        kwargs['finalize_attempt'] = True

        return upstream_delta, kwargs


    def handle(self):
        done = False
        while not done:
            try:
                # pre-load results as a last resort against bugs:
                # _handle_once() could be terminated by an uncaught
                # exception. We don't know whether this is
                # due to something in the tx deterministically tickling a
                # bug in the code or due to a bug handling a transient
                # failure (e.g. the executor overflow thing w/exploder
                # which we handled in 2eb7a4f0 but there are undoubtedly
                # others) that would succeed if you retry. To try to split
                # the difference on this, preload next_attempt_time with
                # 1h so that transient errors will eventually get retried
                # but we won't crashloop excessively on a bad tx.
                delta = TransactionMetadata()
                env_kwargs = {
                    'finalize_attempt': True,
                    'next_attempt_time': time.time() + self.retry_params.get(
                        'bug_retry', 3600) }
                self.tx = self.cursor.tx.copy()
                delta, env_kwargs = self._handle_once()
            except Exception as e:
                logging.exception('uncaught exception in OutputHandler')
            finally:
                done = env_kwargs.get('finalize_attempt', False)
                err_resp = Response(
                    450, 'internal error: OutputHandler failed to populate '
                    'response')
                self.tx.fill_inflight_responses(err_resp, delta)
                for i in range(0,5):
                    try:
                        logging.debug(self.cursor.tx)
                        self.cursor.write_envelope(delta, **env_kwargs)
                        break
                    except VersionConflictException:
                        logging.debug('VersionConflictException')
                        if i == 4:
                            raise
                        backoff(i)
                        self.cursor.load()
        logging.debug('done')

    # -> final attempt reason, next retry time
    def _next_attempt_time(self, now) -> Tuple[Optional[str], Optional[int]]:
        if self.cursor.tx.retry is None:
            # leave the existing value for final_attempt_reason
            final_attempt_reason = None
            next_attempt_time = None
            return final_attempt_reason, next_attempt_time

        max_attempts = self.cursor.tx.retry.get('max_attempts', None)
        # TODO separate function to merge tx.retry, self.retry_params, defaults
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
        deadline = self.cursor.tx.retry.get('deadline', None)
        if deadline is None:
            deadline = self.retry_params.get('deadline', 86400)
        if deadline is not None and ((next - self.cursor.creation) > deadline):
            return 'retry policy deadline', None
        return None, next

    def _maybe_send_notification(self, final_attempt_reason : Optional[str]):
        resp : Optional[Response] = None
        tx = self.cursor.tx
        # Note: this is not contingent on self.cursor.input_done. Thus
        # if the rest client enables notifications in the initial
        # POST, we will send a bounce if it permfailed at
        # RCPT. Exploder only enables notifications at the end and
        # will return an error downstream per its business logic if it
        # failed prior. If the rest client doesn't want notifications
        # for rcpt errors, don't enable it until after sending the
        # data, etc.

        # This does expose one shortcoming that appending body/blob
        # data will probably not fail even if the upstream transaction
        # has permfailed since:
        # - Exploder doesn't check for upstream errors after it it has
        # decided to store&forward
        # - OutputHandler buffers the entire body before sending it
        # upstream
        # - downstream StorageWriterFilter/RestHandler stack doesn't
        # have the logic to fail the PUT in that case
        # RestHandler should probably always fail body/blob PUT after
        # upstream perm error; it should do it after temp errors unless
        # retries are enabled?
        if tx.mail_response is None:
            pass
        elif tx.mail_response.err():
            resp = tx.mail_response
        elif not tx.rcpt_response:
            pass
        elif len(tx.rcpt_response) == 1 and tx.rcpt_response[0].err():
            resp = tx.rcpt_response[0]
        elif len(tx.rcpt_response) > 1:
            logging.warning('OutputHandler._maybe_send_notification %s '
                            'unexpected multi-rcpt', self.rest_id)
            return
        else:
            resp = tx.data_response

        if resp is None:
            logging.info('OutputHandler._maybe_send_notification %s '
                         'response is None, upstream timeout?', self.rest_id)
            return

        last_attempt = final_attempt_reason is not None

        logging.debug('OutputHandler._maybe_send_notification '
                      '%s last %s notify %s tx %s', self.rest_id, last_attempt,
                      self.cursor.tx.notification, self.cursor.tx)

        if resp.ok():
            return
        if resp.temp() and not last_attempt:
            return

        mail_from = self.cursor.tx.mail_from
        if mail_from.mailbox == '':
            return

        orig_headers : str
        body = self.cursor.tx.body
        if body:
            if isinstance(body, MessageBuilderSpec):
                # blobs not needed here
                builder = MessageBuilder(body.json, blobs={})
                orig_headers = builder.build_headers_for_notification().decode(
                    'utf-8')
            elif isinstance(body, Blob):
                h = read_headers(self.cursor.tx.body)
                orig_headers = h if h is not None else ''
            else:
                raise ValueError()
        else:
            logging.warning('OutputHandler._maybe_send_notification '
                            'no source for orig_headers %s', self.rest_id)
            orig_headers = ''

        assert bool(self.cursor.tx.rcpt_to)
        rcpt_to = self.cursor.tx.rcpt_to[0]
        assert rcpt_to is not None
        dsn = generate_dsn(self.mailer_daemon_mailbox,
                           mail_from.mailbox,
                           rcpt_to.mailbox, orig_headers,
                           received_date=self.cursor.creation,
                           now=int(time.time()),
                           response=resp)

        # TODO link these transactions somehow
        # self.cursor.id should end up in this tx and this tx id
        # should get written back to self.cursor
        notify_json = self.cursor.tx.notification
        # The endpoint used for notifications should go out directly,
        # *not* via the exploder which has the potential to enable
        # bounces on this bounce, etc.
        notification_tx = TransactionMetadata(
            host=notify_json['host'],
            mail_from=Mailbox(''),
            # TODO may need to save some esmtp e.g. SMTPUTF8
            rcpt_to=[Mailbox(mail_from.mailbox)],
            body = InlineBlob(dsn, last=True),
            retry={})
        notification_endpoint = self.notification_factory()
        # timeout=0 i.e. fire&forget, don't wait for upstream
        # but internal temp (e.g. db write fail, should be uncommon)
        # should result in the parent retrying even if it was
        # permfail, better to dupe than fail to emit the bounce

        notification_endpoint.update(notification_tx, notification_tx.copy())
        # no wait -> fire&forget
