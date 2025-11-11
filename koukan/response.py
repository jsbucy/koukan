# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, Optional, Tuple
import logging

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

    def __init__(self, code=200, mess=None):
        self.code = code
        if mess is None:
            if self.ok():
                mess = 'ok'
            elif self.temp():
                mess = 'temporary error'
            elif self.perm():
                mess = 'permanent error'
            else:
                logging.warning('%s', code, stack_info=True)
                mess = 'internal error'
        self.message = mess

    def __str__(self):
        return '%d %s' % (self.code, self.message)

    def __repr__(self):
        return str(self)

    def major_code(self):
        return int(self.code/100)

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

    def to_json(self, which_json) -> Dict[object, object]:
        return {'code': self.code, 'message': self.message}

    def __eq__(self, r):
        if not isinstance(r, Response):
            return False
        return self.code == r.code and self.message == r.message

    @staticmethod
    def from_json(d : Dict[object, object], which_js) -> Optional["Response"]:
        code = d.get('code', None)
        if not isinstance(code, int):
            return None
        msg = d.get('message', None)
        if msg is not None and not isinstance(msg, str):
            return None
        return Response(code, msg)

    def to_smtp_resp(self) -> str:
        assert(not self.internal())
        assert self.code >= 200 and self.code <= 599
        # TODO it looks like aiosmtpd doesn't fold this if it's longer than
        # an smtp line (~1000B)
        return str(self.code) + ' ' + self.message
