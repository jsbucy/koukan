import smtplib
import sys
import secrets

import email.message
import email.utils

host = sys.argv[1]
port = sys.argv[2]
ehlo = sys.argv[3]
mail_from = sys.argv[4]
rcpt_to = sys.argv[5:]

s = smtplib.SMTP(host=host, port=port, local_hostname=ehlo)
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
m.set_content(sys.stdin.read())
print(m)
print(s.sendmail(mail_from, rcpt_to, m.as_bytes()))
