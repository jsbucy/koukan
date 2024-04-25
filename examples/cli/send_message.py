from typing import Optional
import logging
from urllib.parse import urljoin
import time
import secrets
import socket

import time
import requests
import copy
from sys import argv

base_url = 'https://localhost:8000'

session = requests.Session()
session.verify = 'localhost.crt'

# -> uri
def send_part(inline : Optional[str] = None,
              filename : Optional[str] = None) -> Optional[str]:
    assert inline or filename

    resp = session.post(base_url + '/blob?upload=chunked')
    logging.info('POST /blob %s', resp)

    if resp.status_code != 201:
        return None
    if not (uri := resp.headers.get('location', None)):
        return None

    if inline:
        content = inline.encode('utf-8')
    elif filename:
        with open(filename, 'rb') as f:
            content = f.read()
    resp = session.put(base_url + uri, content)
    logging.info('PUT %s %s', uri, resp)
    if resp.status_code >= 300:
        return None
    return uri


def send_body(json):
    for multi in ['text_body', 'related_attachments', 'file_attachments']:
        if not (multipart := json.get(multi, [])):
            continue
        for part in multipart:
            logging.info('send_body %s', part)
            inline = part.get('put_content', None)
            filename = part.get('file_content', None)
            if not inline and not filename:
                continue

            if (uri := send_part(inline=inline, filename=filename)) is None:
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
            "put_content": "<b>hello</b>"
        },
        {
            "content_type": "text/plain",
            "file_content": "/etc/lsb-release"
        }
    ]
}

def main(mail_from, rcpt_to):
    message_builder["headers"] += [
        ["from", [{"display_name": "alice a", "address": mail_from}]],
        ["to", [{"display_name": "bob b", "address": rcpt_to}]]]

    resp = session.post(
        base_url + '/transactions',
        headers={'host': 'msa-output'},
        json={
            'mail_from': {'m': mail_from},
            'rcpt_to': [{'m': rcpt_to}],
        })

    if resp.status_code != 201:
        return

    tx_json = resp.json()
    tx_url = urljoin(base_url, resp.headers['location'])

    rest_resp = session.get(tx_url, headers={'request-timeout': '5'})
    for resp_field in ['mail_response', 'rcpt_response']:
        if not (resp := rest_resp.json().get(resp_field, None)):
            continue
        if resp_field == 'rcpt_response':
            resp = resp[0]
        if (code := resp.get('code', None)) is None:
            continue
        if code >= 500:
            logging.info('err %s %s %s',
                         resp_field, rest_resp, rest_resp.json())
            return

    if not send_body(message_builder):
        return
    logging.info('main message_builder spec %s', message_builder)

    resp = session.patch(tx_url, json={'message_builder': message_builder})
    if resp.status_code >= 300:
        return

    done = False
    while not done:
        logging.info('GET %s', tx_url)
        start = time.monotonic()
        resp = session.get(tx_url, headers={'request-timeout': '5'})
        logging.info('GET /%s %d %s', tx_url, resp.status_code, resp.text)
        tx_json = resp.json()

        for resp in ['mail_response', 'rcpt_response', 'data_response']:
            if not (resp_json := tx_json.get(resp, None)):
                continue
            if resp == 'rcpt_response':
                resp_json = resp_json[0]
            if not (code := resp_json.get('code', None)):
                continue
            logging.info('%s %s', resp, code)
            if code >= 500:
                break
            if (resp == 'data_response') and (code < 300):
                done = True
        if done:
            break
        delta = time.monotonic() - start
        if delta < 1:
            time.sleep(1 - delta)
        tx_json = None


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(message)s')
    main(argv[1], argv[2])
