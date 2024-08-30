from typing import Optional
import logging
from urllib.parse import urljoin
import time
import secrets
import socket
import copy
import json

import time
import requests
import copy
from sys import argv
from contextlib import nullcontext

base_url = 'https://localhost:8000'

session = requests.Session()
session.verify = 'localhost.crt'

# -> url path (for reuse)
def send_part(tx_url,
              blob_id : str,
              inline : Optional[str] = None,
              filename : Optional[str] = None) -> Optional[str]:
    assert inline or filename

    path = tx_url + '/blob/' + blob_id
    uri = base_url + path
    logging.info('PUT %s', uri)

    def put(content):
        return session.put(uri, data=content)

    if inline:
        resp = put(inline.encode('utf-8'))
    elif filename:
        with open(filename, 'rb') as file:
            resp = put(file)

    logging.info('PUT %s %s', uri, resp)
    if resp.status_code >= 300:
        return None
    return path

def strip_filenames(json):
    for multi in ['text_body', 'related_attachments', 'file_attachments']:
        if not (multipart := json.get(multi, [])):
            continue
        for part in multipart:
            if 'file_content' in part:
                del part['file_content']
            if 'put_content' in part:
                del part['put_content']
    return json


def send_body(tx_url, json):
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

            if (uri := send_part(
                    tx_url, part['content_uri'], inline=inline,
                    filename=filename)) is None:
                return False

            for part_field in ['put_content', 'file_content']:
                if part_field in part:
                    del part[part_field]
            part['content_uri'] = uri

    return True

message_builder = {
    "headers": [
        ["subject", "hello"],
        ["date", {"unix_secs": time.time() }],
        ["message-id", [secrets.token_hex(16) + "@" + socket.gethostname()]],
    ],

    "text_body": [
        {
            "content_type": "text/html",
            "put_content": "<b>hello</b>",
            "content_uri": "my_html_body",
        },
        {
            "content_type": "text/plain",
            "file_content": "/etc/lsb-release",
            "content_uri": "my_plain_body",
        }
    ]
}

message_builder_blobs = None
def main(mail_from, rcpt_to, max_wait=30):
    global message_builder_blobs
    logging.debug('main from=%s to=%s', mail_from, rcpt_to)

    tx_json={
        'mail_from': {'m': mail_from},
        'rcpt_to': [{'m': rcpt_to}],
        'message_builder': message_builder,
        'retry': {},         # use system defaults for retries
        # msa-output won't have default_notification so explicitly
        # request to send it the same way we send the original message.
        'notification': {'host': 'msa-output' }
    }
    if message_builder_blobs is None:
        tx_json['message_builder'] = strip_filenames(
            copy.deepcopy(message_builder))
    logging.debug(json.dumps(tx_json, indent=2))

    tx_start = time.monotonic()

    logging.debug('POST /transactions')
    start = time.monotonic()
    rest_resp = session.post(
        base_url + '/transactions',
        headers={'host': 'msa-output',
                 'request-timeout': '5'},
        json=tx_json)
    logging.debug('POST /transactions %s', rest_resp)
    if rest_resp.status_code != 201:
        return

    tx_json = rest_resp.json()
    tx_path = rest_resp.headers['location']
    tx_url = urljoin(base_url, tx_path)

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

    if message_builder_blobs is None:
        if not send_body(tx_path, message_builder):
            return
        logging.info('main message_builder spec %s', message_builder)
        get_tx_resp = session.get(tx_url)
        message_builder_blobs = message_builder
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
            rest_resp = session.get(tx_url, headers=headers)
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

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')
    mail_from = argv[1]
    rcpt_to = argv[2:]
    message_builder["headers"] += [
        ["from", [{"display_name": "alice a", "address": mail_from}]],
        ["to", [{"display_name": "bob b", "address": r} for r in rcpt_to]]]

    for rcpt in rcpt_to:
        main(mail_from, rcpt)
