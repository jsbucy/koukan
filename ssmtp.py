import smtplib
import sys
import secrets
import logging

import email.message
import email.utils

def main(host, port, ehlo, mail_from, rcpt_to, data):
    s = smtplib.SMTP(host=host, port=int(port), local_hostname=ehlo)
    s.ehlo(ehlo)
    print(s.esmtp_features)

    m = email.message.EmailMessage()
    m.add_header('From', '<' + mail_from + '>')

    # seems like email libs are capable of doing this but I can't figure out how
    to_header = '';
    for t in rcpt_to:
        to_header += '<' + t + '>'
        if t != rcpt_to[-1]:
            to_header += ', '
            m.add_header('To', to_header)

    m.add_header('Date', email.utils.format_datetime(email.utils.localtime()))
    m.add_header('Message-ID', '<' + secrets.token_hex(32) + '@' + ehlo + '>')
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
