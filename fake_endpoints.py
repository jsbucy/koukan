from response import Response, Esmtp

class PrintEndpoint:
    def __init__(self):
        pass

    def start(self,
              local_host, remote_host,
              mail_from, transaction_esmtp,
              rcpt_to, rcpt_esmtp):

        print('PrintEndpoint.start_transaction ', mail_from, ' ',
              transaction_esmtp, ' ', rcpt_to, ' ', rcpt_esmtp)
        return Response(250)

    def append_data(self, last, d=None, blob_id=None):
        print('PrintEndpoint.append_data ', last, ' ')
        if blob_id:
            print('blob ', blob_id)
        else:
            print(d)
        return Response(), None

    def get_status(self):
        return Response(250, 'message successfully injected')
