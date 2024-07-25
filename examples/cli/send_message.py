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
def send_part(tx_url,
              inline : Optional[str] = None,
              filename : Optional[str] = None) -> Optional[str]:
    assert inline or filename

    resp = session.post(tx_url + '/blob?upload=chunked')
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


def send_body(tx_url, json):
    for multi in ['text_body', 'related_attachments', 'file_attachments']:
        if not (multipart := json.get(multi, [])):
            continue
        for part in multipart:
            logging.info('send_body %s', part)
            if 'content_uri' in part:
                continue
            inline = part.get('put_content', None)
            filename = part.get('file_content', None)
            if not inline and not filename:
                continue

            if (uri := send_part(
                    tx_url, inline=inline, filename=filename)) is None:
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

blobs_done = False
def main(mail_from, rcpt_to):
    global blobs_done
    logging.debug('main from=%s to=%s', mail_from, rcpt_to)

    json={
        'mail_from': {'m': mail_from},
        'rcpt_to': [{'m': rcpt_to}],
    }
    if blobs_done:
        json['message_builder'] = message_builder
    logging.debug('POST /transactions')
    start = time.monotonic()
    rest_resp = session.post(
        base_url + '/transactions',
        headers={'host': 'msa-output',
                 'request-timeout': '5'},
        json=json)
    logging.debug('POST /transactions %s', rest_resp)
    if rest_resp.status_code != 201:
        return

    tx_json = rest_resp.json()
    tx_url = urljoin(base_url, rest_resp.headers['location'])

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

    if not blobs_done:
        if not send_body(tx_url, message_builder):
            return
        logging.info('main message_builder spec %s', message_builder)

        get_tx_resp = session.get(tx_url)

        resp = session.post(
            tx_url + '/message_builder',
            json=message_builder,
            headers = {'if-match': get_tx_resp.headers['etag']})
        if resp.status_code >= 300:
            logging.info('patch message builder spec resp %s', resp)
            return
        blobs_done = True
        rest_resp = None

    done = False
    etag = None
    while not done:
        if rest_resp is None:
            start = time.monotonic()
            logging.info('GET %s', tx_url)
            headers = {'request-timeout': '5'}
            if etag:
                headers['if-none-match'] = etag
            rest_resp = session.get(tx_url, headers=headers)
            logging.info('GET /%s %d %s',
                         tx_url, rest_resp.status_code, rest_resp.text)
            if rest_resp.status_code in [200, 304] and 'etag' in rest_resp.headers:
                etag = rest_resp.headers['etag']
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
            logging.info('%s %s', resp, code)
            if code >= 500:
                break
            if (resp == 'data_response') and (code < 300):
                done = True
        rest_resp = None
        if done:
            break
        # etag didn't change and delta < 1?
        # delta = time.monotonic() - start
        # if delta < 1:
        #     dt = 1 - delta
        #     logging.debug('nospin %f', dt)
        #     time.sleep(dt)
        tx_json = None


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
