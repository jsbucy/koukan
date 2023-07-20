from response import Response, Esmtp

class PrintEndpoint:
    def __init__(self):
        pass

    # return session id, greeting
    # -> (cx_id, resp)
    def on_connect(self, remote_host, local_host):
        print('on_connect ', remote_host, ' ', local_host)
        return Response()

    # return esmtp
    def on_ehlo(self, hostname):
        print('on_ehlo ', hostname)
        return Response(), Esmtp()

    # -> (resp, rcpt_status)
    def start_transaction(self,
                          reverse_path, esmtp_options=None,
                          forward_path = []):
        print('start_transaction ', reverse_path, ' ',
              esmtp_options, ' ', forward_path)
        rcpt_status = [Response() for _ in forward_path] if forward_path else []
        return Response(), rcpt_status

    # -> resp
    def add_rcpt(self, forward_path, esmtp_options=None):
        print('add_rcpt ', forward_path, ' ', esmtp_options)
        return Response()

    # -> (resp, chunk_id)
    def append_data(self, last : bool, chunk_id : int, d : bytes = None):
        print('append_data ', last, ' ', chunk_id)
        print(d)
        return Response()

    # -> (resp, len)
    def append_data_chunk(self, chunk_id : int, offset : int, d : bytes,
                          last : bool):
        print('append_data_chunk ', chunk_id, ' ', ' ', offset, ' ', last)
        print(d)
        return Response(200, 'append data chunk ok'), offset + len(d)


    # return final result
    def get_transaction_status(self):
        print('get_transaction_status ')
        return Response(250, 'message successfully injected')


    # does this need to have another hook to get the final status
    # since rest may need to GET that after the fact? Otherwise it
    # needs a separate cache for that.
