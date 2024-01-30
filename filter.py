from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging

from response import Response

from blob import Blob

class HostPort:
    host : str
    port : int
    def __init__(self, host, port):
        self.host = host
        self.port = port
    @staticmethod
    def from_seq(seq):
        return HostPort(seq[0], seq[1])
    @staticmethod
    def from_yaml(yaml):
        return HostPort(yaml['host'], yaml['port'])
    def to_tuple(self):
        return (self.host, self.port)
    def to_json(self):
        return self.to_tuple()

class Esmtp:
    # from aiosmtpd
    # TODO parse out into keyword and key/value
    capabilities : List[str]

class Mailbox:
    mailbox : str  # i.e. rfc5321 4.1.2
    esmtp : Optional[Esmtp] = None
    def __init__(self, mailbox, esmtp=None):
        self.mailbox = mailbox
        self.esmtp = esmtp

    # NOTE this is mainly for invariant checking in merge/delta
    # (below), does not compare esmtp currently
    def __eq__(self, x : "Mailbox"):
        return self.mailbox == x.mailbox

    def __str__(self):
        return self.mailbox
    def __repr__(self):
        return self.mailbox

    #def local_part(self) -> Optional[str]:
    #    pass
    #def domain(self) -> Optional[str]:
    #    pass

    def to_json(self):
        out = {'m': self.mailbox}
        if self.esmtp:
            out['e'] = self.esmtp
        return out

    @staticmethod
    def from_json(json):
        return Mailbox(json['m'], json['e'] if 'e' in json else None)

def list_from_js(js, builder):
    return [builder(j) for j in js]

# NOTE in the Filter api/stack, this is usually interpreted as a delta where
# field == None means "not present in the delta". As such, there is
# currently no representation of "set this field to None".
class TransactionMetadata:
    fields = {
        'host': lambda x: x,
        'remote_host': HostPort.from_seq,
        'local_host': HostPort.from_seq,
        'mail_from': Mailbox.from_json,
        'mail_response': Response.from_json,
        'rcpt_to': lambda js: list_from_js(js, Mailbox.from_json),
        'rcpt_response': lambda js: list_from_js(js, Response.from_json),
        'data_response': Response.from_json,
        'max_attempts': lambda x: x,
        # XXX last?
    }

    host : Optional[str] = None
    rest_endpoint : Optional[str] = None
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None
    # tls mumble

    mail_from : Optional[Mailbox] = None
    mail_response : Optional[Response] = None
    rcpt_to : List[Mailbox]
    rcpt_response : List[Response]
    data_response : Optional[Response] = None

    max_attempts : Optional[int] = None

    def __init__(self, local_host : Optional[HostPort] = None,
                 remote_host : Optional[HostPort] = None,
                 mail_from : Optional[Mailbox] = None,
                 rcpt_to : Optional[List[Mailbox]] = None,
                 host : Optional[str] = None,
                 max_attempts : Optional[int] = None):
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.rcpt_to = rcpt_to if rcpt_to else []
        self.rcpt_response = []
        self.host = host
        if max_attempts is not None:
            self.max_attempts = max_attempts

#    def __bool__(self):
#        for f in TransactionMetadata.all_fields:
#            if hasattr(self, f) and bool(getattr(self, f)):
#                return True
#        return False

    def __repr__(self):
        out = ''
        out += 'mail_from=%s mail_response=%s ' % (
            self.mail_from, self.mail_response)
        out += 'rcpt_to=%s rcpt_response=%s ' % (
            self.rcpt_to, self.rcpt_response)
        out += 'data_response=%s' % self.data_response
        return out

    @staticmethod
    def from_json(json):
        tx = TransactionMetadata()
        for f in json.keys():
            builder = TransactionMetadata.fields.get(f, None)
            if not builder:
                return None  # invalid
            js_v = json[f]
            if js_v is None:
                # TODO for now setting a non-null field back to null
                # is not a valid operation so reject json with that
                return None
            if isinstance(js_v, list) and not js_v:
                return None
            v = builder(js_v)
            if not v:
                return None
            setattr(tx, f, v)
        return tx

    def to_json(self):
        json = {}
        for f in TransactionMetadata.fields.keys():
            if hasattr(self, f) and getattr(self, f) is not None:
                v = getattr(self, f)
                if v is None:
                    continue
                v_js = None
                if isinstance(v, str) or isinstance(v, int):
                    v_js = v
                elif isinstance(v, list):
                    if v:
                        v_js = [vv.to_json() for vv in v]
                else:
                    v_js = v.to_json()
                if v_js is not None:
                    json[f] = v_js
        return json

    # apply a delta to self -> next
    def merge(self, delta : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()

        for f in TransactionMetadata.fields.keys():
            old_v = getattr(self, f, None)
            new_v = getattr(delta, f, None)
            if old_v is None and new_v is not None:
                setattr(out, f, new_v)
                continue
            if old_v is not None and new_v is None:
                setattr(out, f, old_v)
                continue

            if isinstance(old_v, list) != isinstance(new_v, list):
                return None  # invalid
            if not isinstance(old_v, list):
                # could verify that old_v == new_v
                continue
            l = []
            l.extend(old_v)
            l.extend(new_v)
            setattr(out, f, l)

        return out

    # compute a delta from self to successor
    def delta(self, successor : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()
        for f in TransactionMetadata.fields.keys():
            old_v = getattr(self, f, None)
            new_v = getattr(successor, f, None)
            if (old_v is not None) and (new_v is None):
                return None  # invalid
            if (old_v is None) and (new_v is not None):
                setattr(out, f, new_v)
                continue
            if old_v == new_v:
                if isinstance(old_v, list):
                    setattr(out, f, [])
                else:
                    setattr(out, f, None)
                continue

            if isinstance(old_v, list) != isinstance(new_v, list):
                return None  # invalid
            if not isinstance(old_v, list):
                if old_v != new_v:
                    return None  # invalid
                continue

            old_len = len(old_v)
            if old_len > len(new_v):
                return None  # invalid
            if new_v[0:old_len] != old_v:
                return None
            setattr(out, f, new_v[old_len:])

        return out

class Filter(ABC):
    # XXX needs to return some errors e.g. http 412 directly instead of via tx
    @abstractmethod
    def on_update(self, transaction_metadata : TransactionMetadata,
                  timeout : Optional[float] = None):
        pass

    @abstractmethod
    def append_data(self, last : bool, blob : Blob,
                    timeout : Optional[float] = None) -> Response:
        pass

    @abstractmethod
    def abort(self):
        pass
