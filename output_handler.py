from typing import Dict, Optional, Any
import logging
import json
import secrets
import time

from storage import Storage, TransactionCursor, BlobReader, BlobWriter
from storage_schema import InvalidActionException, VersionConflictException
from response import Response
from filter import HostPort, Mailbox, TransactionMetadata, WhichJson


class OutputHandler:
    def output(self, cursor, endpoint) -> Optional[Response]:
      logging.debug('cursor_to_endpoint output() %s', cursor.rest_id)

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

    def cursor_to_endpoint(self, cursor, endpoint):
        logging.debug('cursor_to_endpoint() %s', cursor.rest_id)
        resp = self.output(cursor, endpoint)
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
