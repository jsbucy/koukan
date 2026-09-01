# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, Optional, Tuple
import logging

from koukan.rest_schema import WhichJson

def ok_smtp_code(code):
    return code >= 200 and code <= 299

class Esmtp:
    # from smtplib.SMTP.esmtp_features
    def __init__(self, esmtp : Optional[Dict[str,str]] = None):
        self.esmtp = esmtp


class Response:
    code : int
    message : str
    group_reject = False

    def __init__(self, code=200, mess=None, group_reject=False):
        self.code = code
        if mess is None:
            if self.ok():
                mess = 'ok'
            elif self.temp():
                mess = 'temporary error'
            elif self.perm():
                mess = 'permanent error'
            else:
                assert False, 'invalid code'
        self.message = mess
        self.group_reject = group_reject

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

    def err(self):
        return not self.ok()

    def perm(self):
        return self.code >= 500 and self.code <= 599

    def temp(self):
        return self.code >= 400 and self.code <= 499

    def to_json(self, which_json : WhichJson) -> Dict[object, object]:
        out : Dict[object, object] = {
            'code': self.code,
            'message': self.message}
        if which_json == WhichJson.DB_ATTEMPT:
            out['group_reject'] = self.group_reject
        return out

    def __eq__(self, r):
        if not isinstance(r, Response):
            return False
        return self.code == r.code and self.message == r.message

    @staticmethod
    def from_json(d : Dict[object, object], which_js : WhichJson
                  ) -> Optional["Response"]:
        code = d.get('code', None)
        if not isinstance(code, int):
            return None
        msg = d.get('message', None)
        if msg is not None and not isinstance(msg, str):
            return None
        kwargs = {}
        if which_js == WhichJson.DB_ATTEMPT:
            gr = d.get('group_reject', None)
            if not isinstance(gr, bool):
                return None
            kwargs['group_reject'] = gr
        return Response(code, msg, **kwargs)

    def to_smtp_resp(self) -> str:
        assert self.code >= 200 and self.code <= 599
        # TODO it looks like aiosmtpd doesn't fold this if it's longer than
        # an smtp line (~1000B)
        return str(self.code) + ' ' + self.message
