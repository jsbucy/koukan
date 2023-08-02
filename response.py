from typing import Dict
from typing import Tuple

def ok_smtp_code(code):
    return code >= 200 and code <= 299

class Esmtp:
    # from smtplib.SMTP.esmtp_features
    def __init__(self, esmtp : Dict[str,str] = None):
        self.esmtp = esmtp

class Response:
    code : int
    message : str

    def __init__(self, code=200, str="ok"):
        self.code = code
        self.message = str

    def __str__(self):
        return '%d %s' % (self.code, self.message)

    def from_smtp(t : Tuple[int, bytes]) -> "Response":
        return Response(t[0], t[1].decode('utf-8'))

    def ok(self):
        return ok_smtp_code(self.code)
    def err(self):
        return not self.ok()

    def perm(self):
        return self.code >= 500 and self.code <= 599

    def temp(self):
        return self.code >= 400 and self.code <= 499

    def to_json(self) -> Dict:
        return {'code': self.code, 'message': self.message}

    def from_json(d : Dict) -> "Response":
        return Response(d['code'], d['message'])

    def to_smtp_resp(self) -> bytes:
        return ('%d ' % self.code).encode('utf-8') + self.message.encode('utf-8')


def ok_resp(resp : Tuple[int, str]):
    return ok_smtp_code(resp[0])

def to_smtp_resp(resp : Tuple[int, str]):
    return ('%d %s' % resp).encode('ascii')
