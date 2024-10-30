# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import smtplib
import sys
import secrets
import logging
import threading
from functools import partial
import argparse
import time

import email.message
import email.utils
from email import policy
from email.headerregistry import Address

def main(host, port, ehlo, mail_from, rcpt_to, data):
    with smtplib.SMTP(host=host, port=int(port), local_hostname=ehlo) as s:
        s.ehlo(ehlo)
        logging.info('remote server esmtp %s', s.esmtp_features)

        m = email.message.EmailMessage(policy=policy.SMTP)
        m['from'] = Address(addr_spec=mail_from)
        m['to'] = [Address(addr_spec=t) for t in rcpt_to]

        m.add_header(
            'Date', email.utils.format_datetime(email.utils.localtime()))
        m.add_header(
            'Message-ID', '<' + secrets.token_hex(16) + '@' + ehlo + '>')
        m.set_content(data)
        logging.info('sending smtp')
        try:
            resp = s.sendmail(mail_from, rcpt_to, m.as_bytes())
            logging.info('done %s', resp)
        except:
            logging.exception('sendmail exception')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(thread)d] %(filename)s:%(lineno)d %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default='1025')
    parser.add_argument('--ehlo', default='localhost')

    parser.add_argument('--mail_from')
    #parser.add_argument('--rfc822_filename')
    parser.add_argument('--notification_host', default='msa-output')
    # {}: use system defaults for retries
    parser.add_argument('--retry', default='{}')
    parser.add_argument('--iters', default='1')
    parser.add_argument('--threads', default='1')
    parser.add_argument('rcpt_to', nargs='*')

    args = parser.parse_args()
    host = args.host
    port = args.port
    ehlo = args.ehlo
    mail_from = args.mail_from
    rcpt_to = args.rcpt_to

    def send():
        for i in range(0, int(args.iters)):
            main(host, port, ehlo, mail_from, rcpt_to, msg)

    msg = sys.stdin.read()
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
