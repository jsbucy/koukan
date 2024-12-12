# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

# Simplest example of sending a message to a single recipient using
# json message builder, inline text only, fire&forget semantics.

# python3 examples/send_message/trivial_json_sender.py \
#   --router_url http://localhost:8000  # router rest_listener endpoint
#   --mail_from alice@example.com \
#   --message_builder_filename examples/send_message/trivial_message.json \
#   --rcpt_to bob@example.com

from typing import Any, Dict, Optional
import logging
from urllib.parse import urljoin
import json
import argparse
import requests
from sys import argv


def send_message_json(
        base_url : str,
        host : str,
        mail_from : str,
        message_builder_json : Dict[str, Any],
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

    tx_json['message_builder'] = message_builder_json

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

    logging.info('sent! track progress at %s', tx_url)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--mail_from')
    parser.add_argument('--message_builder_filename')
    parser.add_argument('--router_url', default='http://localhost:8000')
    parser.add_argument('--host', default='msa-output')
    parser.add_argument('--notification_host', default='msa-output')
    parser.add_argument('--rcpt_to')

    args = parser.parse_args()

    message_builder = None
    if args.message_builder_filename:
      with open(args.message_builder_filename, 'r') as f:
        message_builder = json.load(f)

    logging.debug(message_builder)

    logging.debug(args.rcpt_to)

    send_message_json(args.router_url,
                      args.host,
                      args.mail_from,
                      message_builder_json=message_builder,
                      rcpt_to=args.rcpt_to,
                      notification_host=args.notification_host)
