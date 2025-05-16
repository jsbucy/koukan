# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# full-featured sender
# - message builder json or pre-formatted rfc822
# - hanging GET track transaction status
# - separate blob upload
# - reuses blobs across multiple rcpts

# python3 examples/send_message/send_message.py \
#   --router_url http://localhost:8000
#   --mail_from alice@example.com
#   --message_builder_filename examples/cli/message_builder.json \
#   bob@example.com carol@example.com dave@example.com


from typing import Any, Dict, Optional
import logging
from urllib.parse import urljoin
import time
import secrets
import socket
import copy
import json
import argparse
import threading

import time
import requests
import copy
from sys import argv
from contextlib import nullcontext

# Sends a message to one or more recipients. Does hanging GET to wait
# for upstream status. Reuses blobs across recipients.
class Sender:
    base_url : str
    host : str
    notification_host : Optional[str]
    mail_from : str

    message_builder : Optional[Dict[str, Any]]
    # has blobs uris referencing first first recipient transaction
    message_builder_blobs : Optional[dict] = None
    body_filename : Optional[str] = None
    body_path : Optional[str] = None

    def __init__(self,
                 base_url : str,
                 # in the default configs, this is a post-exploder
                 # chain that doesn't have default_notification...
                 host : str,
                 mail_from : str,
                 message_builder : Optional[dict] = None,
                 body_filename : Optional[str] = None,
                 # ... so if you want notifications, you need to
                 # enable it explicitly
                 notification_host : Optional[str] = None):
        self.session = requests.Session()
        self.session.verify = 'localhost.crt'
        self.mail_from = mail_from
        self.message_builder = message_builder
        self.body_filename = body_filename
        if self.message_builder:
            self.fixup_headers()
        self.base_url = base_url
        self.host = host
        self.notification_host = notification_host

    # -> url path (for reuse)
    def send_part(self,
                  tx_url,
                  blob_id : str,
                  filename : str) -> Optional[str]:
        path = tx_url + '/blob/' + blob_id
        uri = urljoin(self.base_url, path)
        logging.info('PUT %s', uri)

        with open(filename, 'rb') as file:
            resp = self.session.put(uri, data=file)

        logging.info('PUT %s %s', uri, resp)
        if resp.status_code >= 300:
            return None
        return path

    def strip_filenames(self, json : Dict[str, Any]):
        for multi in ['text_body', 'related_attachments', 'file_attachments']:
            if not (multipart := json.get(multi, [])):
                continue
            for part in multipart:
                if 'filename' in part['content']:
                    del part['content']['filename']
        return json


    def send_body(self, tx_url, json : dict):
        for multi in ['text_body', 'related_attachments', 'file_attachments']:
            if not (multipart := json.get(multi, [])):
                continue
            for part in multipart:
                logging.info('send_body %s', part)
                # cf MessageBuilderSpec._add_part_blob()
                content = part['content']
                if 'reuse_uri' in content:
                    continue
                if ('create_id' not in content or
                    'filename' not in content):
                    continue
                filename = content['filename']
                del content['filename']
                if (uri := self.send_part(
                        tx_url, content['create_id'],
                        filename=filename)) is None:
                    return False
                content['reuse_uri'] = uri

        return True

    def send(self, rcpt_to : str, retry : Dict[str, Any]={}, max_wait=30):
        logging.debug('main from=%s to=%s', self.mail_from, rcpt_to)

        tx_json={
            'mail_from': {'m': self.mail_from},
            'rcpt_to': [{'m': rcpt_to}],
        }
        if retry is not None:
            tx_json['retry'] = retry
        if self.notification_host:
            tx_json['notification'] = {'host': self.notification_host }
        if self.body_path is not None:
            tx_json['body'] = {'reuse_uri': self.body_path}
        elif self.body_filename is not None:
            pass
        elif (self.message_builder_blobs is None and
              self.message_builder is not None):
            tx_json['body'] = {'message_builder': self.strip_filenames(
                copy.deepcopy(self.message_builder))}
        else:
            tx_json['body'] = {'message_builder': self.message_builder_blobs}

        logging.debug(json.dumps(tx_json, indent=2))

        tx_start = time.monotonic()

        logging.debug('POST /transactions')
        start = time.monotonic()
        rest_resp = self.session.post(
            urljoin(self.base_url, '/transactions'),
            headers={'host': self.host,
                     'request-timeout': '5'},
            json=tx_json)
        logging.debug('POST /transactions %s %s', rest_resp, rest_resp.headers)
        if rest_resp.status_code != 201:
            return

        tx_json = rest_resp.json()
        tx_path = rest_resp.headers['location']
        tx_url = urljoin(self.base_url, tx_path)

        for resp_field in ['mail_response', 'rcpt_response']:
            if not (resp := tx_json.get(resp_field, None)):
                continue
            if resp_field == 'rcpt_response':
                resp = resp[0]
            if (code := resp.get('code', None)) is None:
                continue
            if code >= 500:
                logging.info('err %s %s %s',
                             resp_field, rest_resp, rest_resp.json())
                return

        if self.body_filename is not None and self.body_path is None:
            with open(self.body_filename, 'rb') as body_file:
                body_path = tx_path + '/body'
                resp = self.session.put(
                    urljoin(self.base_url, body_path),
                    data=body_file)
                logging.debug('PUT %s %s %s', body_path, resp, resp.text)
                if resp.status_code == 201:
                    self.body_path = body_path
        elif (self.message_builder is not None and
              self.message_builder_blobs is None):
            if not self.send_body(tx_path, self.message_builder):
                return
            logging.info('main message_builder spec %s',
                         json.dumps(self.message_builder, indent=2))
            message_builder_blobs = self.message_builder
            rest_resp = None

        done = False
        etag = None
        result = 'still inflight'
        while not done and ((time.monotonic() - tx_start) < max_wait):
            spin = False
            if rest_resp is None:
                spin = True
                start = time.monotonic()
                logging.info('GET %s etag=%s', tx_url, etag)
                headers = {'request-timeout': '5'}
                if etag:
                    headers['if-none-match'] = etag
                rest_resp = self.session.get(tx_url, headers=headers)
                logging.info('GET %s %d %s',
                             tx_url, rest_resp.status_code, rest_resp.text)
                if rest_resp.status_code in [200, 304] and (
                        'etag' in rest_resp.headers):
                    resp_etag = rest_resp.headers['etag']
                    if resp_etag != etag:
                        spin = False
                    etag = resp_etag
                    logging.debug('etag %s', etag)
                else:
                    etag = None

            if rest_resp.status_code == 304:
                continue

            tx_json = rest_resp.json()

            for resp in ['mail_response', 'rcpt_response', 'data_response']:
                if not (resp_json := tx_json.get(resp, None)):
                    continue
                if resp == 'rcpt_response':
                    resp_json = resp_json[0]
                if not (code := resp_json.get('code', None)):
                    continue
                logging.info('recipient %s resp field %s code %s',
                             rcpt_to, resp, code)
                if code >= 500:
                    result = 'failure'
                    done = True
                    break
                if (resp == 'data_response') and (code < 300):
                    result = 'success'
                    done = True
                    break

            if tx_json.get('final_attempt_reason', None):
                done = True

            rest_resp = None
            if done:
                break
            delta = time.monotonic() - start
            if spin and delta < 1:
                dt = 1 - delta
                logging.debug('nospin %f', dt)
                time.sleep(dt)
            tx_json = None

        logging.info('recipient %s result %s', rcpt_to, result)
        return result

    def _get_header(self, headers, name):
        for h in headers:
            if h[0].lower() == name.lower():
                return h
        return None

    def fixup_headers(self):
        headers = self.message_builder['headers']
        if not self._get_header(headers, 'date'):
            headers.append(["date", {"unix_secs": int(time.time()) }])
        if not self._get_header(headers, 'message-id'):
            headers.append(
                ["message-id",
                 [secrets.token_hex(16) + "@" + socket.gethostname()]])

        # TODO populate from, to/cc/bcc from envelope addrs


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--mail_from')
    parser.add_argument('--rfc822_filename')
    parser.add_argument('--message_builder_filename')
    parser.add_argument('--base_url', default='http://localhost:8000')
    parser.add_argument('--host', default='msa-output')
    parser.add_argument('--notification_host', default='msa-output')
    # {}: use system defaults for retries
    parser.add_argument('--retry', default='{}')
    parser.add_argument('--iters', default='1')
    parser.add_argument('--threads', default='1')
    parser.add_argument('rcpt_to', nargs='*')

    args = parser.parse_args()

    message_builder = None
    if args.message_builder_filename:
      with open(args.message_builder_filename, 'r') as f:
        message_builder = json.load(f)

    logging.debug(message_builder)

    logging.debug(args.rcpt_to)

    results = {}
    mu = threading.Lock()
    def send():
        sender = Sender(args.base_url,
                        args.host,
                        args.mail_from,
                        message_builder=message_builder,
                        body_filename=args.rfc822_filename,
                        notification_host=args.notification_host)

        for i in range(0, int(args.iters)):
            for rcpt in args.rcpt_to:
                result = sender.send(rcpt, retry)
                with mu:
                    if result not in results:
                        results[result] = 0
                    results[result] += 1

    retry = json.loads(args.retry) if args.retry else None
    threads = []
    start = time.monotonic()
    for t in range(0, int(args.threads)):
        t = threading.Thread(target = send)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    stop = time.monotonic()
    logging.info('done %f', stop - start)
    logging.info(results)
