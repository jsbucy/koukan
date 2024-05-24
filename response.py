from typing import Dict, Optional, Tuple


def ok_smtp_code(code):
    return code >= 200 and code <= 299


class Esmtp:
    # from smtplib.SMTP.esmtp_features
    def __init__(self, esmtp : Optional[Dict[str,str]] = None):
        self.esmtp = esmtp


class Response:
    code : int
    message : str

    INTERNAL=600

    @staticmethod
    def Internal(msg : str):
        return Response(Response.INTERNAL, msg)

    def __init__(self, code=200, str="ok"):
        self.code = code
        self.message = str

    def __str__(self):
        return '%d %s' % (self.code, self.message)

    def __repr__(self):
        return str(self)

    @staticmethod
    def from_smtp(t : Tuple[int, bytes]) -> "Response":
        return Response(t[0], t[1].decode('utf-8'))

    def ok(self):
        return ok_smtp_code(self.code)

    def internal(self):
        return self.code == Response.INTERNAL

    def err(self):
        return not self.ok()

    def perm(self):
        return self.code >= 500 and self.code <= 599

    def temp(self):
        return self.code >= 400 and self.code <= 499

    def to_json(self) -> Dict[object, object]:
        return {'code': self.code, 'message': self.message}

    def __eq__(self, r : 'Response'):
        return self.code == r.code and self.message == r.message

    @staticmethod
    def from_json(d : Dict[object, object]) -> Optional["Response"]:
        code = d.get('code', None)
        if not isinstance(code, int):
            return None
        msg = d.get('message', None)
        if msg is not None and not isinstance(msg, str):
            return None
        return Response(code, msg)

    def to_smtp_resp(self) -> bytes:
        assert(not self.internal())
        return ('%d ' % self.code).encode('utf-8') + self.message.encode('utf-8')


def ok_resp(resp : Tuple[int, str]):
    return ok_smtp_code(resp[0])

def to_smtp_resp(resp : Tuple[int, str]):
    return ('%d %s' % resp).encode('ascii')
