
from smtp_endpoint import SmtpEndpoint
import smtp_service
from fake_endpoints import PrintEndpoint


print_endpoint = lambda: PrintEndpoint()
smtp_service = smtp_service.service(print_endpoint, msa=False, port=9999)

smtp_endpoint = SmtpEndpoint(ehlo_hostname='example.com')

resp = smtp_endpoint.start(
    local_host=None,
    remote_host=('localhost', 9999),
    mail_from='alice@example.com',
    transaction_esmtp=None,
    rcpt_to='bob@destination.com',
    rcpt_esmtp=None)

smtp_endpoint.append_data(d=b'subject: hello\r\n\r\n', last=False)
smtp_endpoint.append_data(d=b'subject: world!', last=True)
print(smtp_endpoint.get_status())
