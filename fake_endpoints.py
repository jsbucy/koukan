from response import Response, Esmtp

class PrintEndpoint:
    chunk_id = None
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
        if forward_path:
            self.chunk_id = 0
        return Response(), [Response() for _ in forward_path]

    # -> resp
    def add_rcpt(self, forward_path, esmtp_options=None):
        print('add_rcpt ', forward_path, ' ', esmtp_options)
        self.chunk_id = 0
        return Response()

    # -> (resp, chunk_id)
    def append_data(self, last : bool, chunk_id : int, d : bytes = None):
        self.chunk_id += 1
        print('append_data ', last, ' ', chunk_id)
        return(Response(), str(self.chunk_id))

    # dotstuff: 1 or more times until eof
    # bdat: one or more per bdat
    # data uri: once per PUT
    # precondition: offset is <= the current end
    #   i.e. overlap is ok but cannot create holes
    # -> (resp, len)
    def append_data_chunk(self, chunk_id : int, offset : int, d : bytes,
                          last_chunk : bool):
        assert(chunk_id == str(self.chunk_id))
        print('append_data_chunk ', chunk_id, ' ', ' ', offset, ' ', last_chunk)
        return Response(200, 'append data chunk ok'), offset + len(d)


    # return final result
    def get_transaction_status(self):
        print('get_transaction_status ')
        return Response(250, 'message successfully injected')


    # does this need to have another hook to get the final status
    # since rest may need to GET that after the fact? Otherwise it
    # needs a separate cache for that.
