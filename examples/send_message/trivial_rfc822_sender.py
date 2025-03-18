# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# Simplest example of sending a pre-existing rfc822 message to a
# single recipient with fire&forget semantics. The message body will
# be sent inline in the JSON REST request and must be plain ascii or
# utf8.

# python3 examples/send_message/trivial_rfc822_sender.py \
#   --router_url http://localhost:8000
#   --mail_from alice@example.com --rfc822_filename testdata/trivial.msg \
#   --rcpt_to bob@example.com

from typing import Any, Dict, Optional
import logging
from urllib.parse import urljoin
import json
import argparse
import requests
from sys import argv


def send_message_rfc822(base_url : str,
                        host : str,
                        mail_from : str,
                        rfc822_message : str,
                        rcpt_to : str,
                        notification_host : str):
    logging.debug('Sender.send from=%s to=%s', mail_from, rcpt_to)
    tx_json = {
        'mail_from': {'m': mail_from},
        'rcpt_to': [{'m': rcpt_to}],
        'retry': {},         # use system defaults for retries
    }
    if notification_host:
        tx_json['notification'] = {'host': notification_host }

    tx_json['body'] = {'inline': rfc822_message}

    logging.debug(json.dumps(tx_json, indent=2))

    logging.debug('POST /transactions')
    rest_resp = requests.post(
        urljoin(base_url, '/transactions'),
        headers={ 'host': host },
        json=tx_json)
    logging.debug('POST /transactions %s %s', rest_resp, rest_resp.headers)
    if rest_resp.status_code != 201:
        return

    tx_json = rest_resp.json()
    tx_path = rest_resp.headers['location']
    tx_url = urljoin(base_url, tx_path)

    logging.info('sent, track progress at %s', tx_url)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--mail_from')
    parser.add_argument('--rfc822_filename')
    parser.add_argument('--router_url', default='http://localhost:8000')
    parser.add_argument('--host', default='msa-output')
    parser.add_argument('--notification_host', default='msa-output')
    parser.add_argument('--rcpt_to')

    args = parser.parse_args()

    rfc822_message = None
    if args.rfc822_filename:
        # open as text messes with line endings
        with open(args.rfc822_filename, 'rb') as f:
            rfc822_message = f.read().decode('utf-8')

    logging.debug(args.rcpt_to)

    send_message_rfc822(args.router_url,
                        args.host,
                        args.mail_from,
                        rfc822_message=rfc822_message,
                        rcpt_to=args.rcpt_to,
                        notification_host=args.notification_host)
