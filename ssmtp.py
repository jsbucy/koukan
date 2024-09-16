import smtplib
import sys
import secrets
import logging

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
        resp = s.sendmail(mail_from, rcpt_to, m.as_bytes())
        logging.info('done %s', resp)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s')

    host = sys.argv[1]
    port = sys.argv[2]
    ehlo = sys.argv[3]
    mail_from = sys.argv[4]
    rcpt_to = sys.argv[5:]

    main(host, port, ehlo, mail_from, rcpt_to, sys.stdin.read())
