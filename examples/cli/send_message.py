from typing import Dict, Optional
import logging
from urllib.parse import urljoin
import time
import secrets
import socket
import copy
import json
import argparse

import time
import requests
import copy
from sys import argv
from contextlib import nullcontext

class Sender:
    base_url : str
    mail_from : str

    message_builder : dict
    # has blobs uris referencing first first recipient transaction
    message_builder_blobs : Optional[dict] = None
    body_filename : Optional[str] = None
    body_path : Optional[str] = None

    def __init__(self, mail_from : str,
                 message_builder : Optional[dict] = None,
                 body_filename : Optional[str] = None,
                 base_url = 'https://localhost:8000'):
        self.session = requests.Session()
        self.session.verify = 'localhost.crt'
        self.mail_from = mail_from
        self.message_builder = message_builder
        self.body_filename = body_filename
        if self.message_builder:
            self.fixup_headers()
        self.base_url = base_url

    # -> url path (for reuse)
    def send_part(self,
                  tx_url,
                  blob_id : str,
                  inline : Optional[str] = None,
                  filename : Optional[str] = None) -> Optional[str]:
        assert inline or filename

        path = tx_url + '/blob/' + blob_id
        uri = urljoin(self.base_url, path)
        logging.info('PUT %s', uri)

        def put(content):
            return self.session.put(uri, data=content)

        if inline:
            resp = put(inline.encode('utf-8'))
        elif filename:
            with open(filename, 'rb') as file:
                resp = put(file)

        logging.info('PUT %s %s', uri, resp)
        if resp.status_code >= 300:
            return None
        return path

    def strip_filenames(self, json):
        for multi in ['text_body', 'related_attachments', 'file_attachments']:
            if not (multipart := json.get(multi, [])):
                continue
            for part in multipart:
                if 'file_content' in part:
                    del part['file_content']
                if 'put_content' in part:
                    del part['put_content']
        return json


    def send_body(self, tx_url, json : dict):
        for multi in ['text_body', 'related_attachments', 'file_attachments']:
            if not (multipart := json.get(multi, [])):
                continue
            for part in multipart:
                logging.info('send_body %s', part)
                if part['content_uri'].startswith('/'):
                    continue
                inline = part.get('put_content', None)
                filename = part.get('file_content', None)
                if filename:
                    del part['file_content']
                if not inline and not filename:
                    continue

                if (uri := self.send_part(
                        tx_url, part['content_uri'], inline=inline,
                        filename=filename)) is None:
                    return False

                for part_field in ['put_content', 'file_content']:
                    if part_field in part:
                        del part[part_field]
                part['content_uri'] = uri

        return True

    def send(self, rcpt_to, max_wait=30):
        logging.debug('main from=%s to=%s', self.mail_from, rcpt_to)

        tx_json={
            'mail_from': {'m': self.mail_from},
            'rcpt_to': [{'m': rcpt_to}],
            'retry': {},         # use system defaults for retries
            # msa-output won't have default_notification so explicitly
            # request to send it the same way we send the original message.
            'notification': {'host': 'msa-output' }
        }
        if self.body_path is not None:
            tx_json['body'] = self.body_path
        elif self.body_filename is not None:
            pass
        elif self.message_builder_blobs is None:
            tx_json['message_builder'] = self.strip_filenames(
                copy.deepcopy(self.message_builder))
        else:
            tx_json['message_builder']: self.message_builder_blobs

        logging.debug(json.dumps(tx_json, indent=2))

        tx_start = time.monotonic()

        logging.debug('POST /transactions')
        start = time.monotonic()
        rest_resp = self.session.post(
            urljoin(self.base_url, '/transactions'),
            headers={'host': 'msa-output',
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
                resp = self.session.post(
                    urljoin(self.base_url, body_path),
                    data=body_file)
                logging.debug('POST %s %s %s', body_path, resp, resp.text)
                if resp.status_code == 201:
                    self.body_path = body_path
        elif (self.message_builder is not None and
              self.message_builder_blobs is None):
            if not self.send_body(tx_path, self.message_builder):
                return
            logging.info('main message_builder spec %s',
                         json.dumps(self.message_builder, indent=2))
            get_tx_resp = self.session.get(tx_url)
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
                logging.info('GET %s', tx_url)
                headers = {'request-timeout': '5'}
                if etag:
                    headers['if-none-match'] = etag
                rest_resp = self.session.get(tx_url, headers=headers)
                logging.info('GET /%s %d %s',
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
            # xxx rest_resp.status_code?
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
                        format='%(asctime)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--mail_from')
    parser.add_argument('--rfc822_filename')
    parser.add_argument('--message_builder_filename')
    parser.add_argument('rcpt_to', nargs='*')

    args = parser.parse_args()

    message_builder = None
    if args.message_builder_filename:
      with open(args.message_builder_filename, 'r') as f:
        message_builder = json.load(f)

    logging.debug(message_builder)

    logging.debug(args.rcpt_to)

    sender = Sender(args.mail_from,
                    message_builder=message_builder,
                    body_filename=args.rfc822_filename)
    for rcpt in args.rcpt_to:
        sender.send(rcpt)
