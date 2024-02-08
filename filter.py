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

class WhichJson(IntEnum):
    ALL = 0
    REST_READ = 1,
    REST_CREATE = 2,
    REST_UPDATE = 3,
    DB = 4

class TxField:
    json_field : str
    accept = List[WhichJson]
    emit = List[WhichJson]
    def __init__(self,
                 json_field : str,
                 accept,
                 emit,
                 from_json=None,
                 to_json=None):
        self.json_field = json_field
        self.accept = accept
        self.emit = emit
        # none here means identity i.e. plain old data/int/str
        self.from_json = from_json
        self.to_json = to_json

    def valid_accept(self, which_json):
        return which_json == WhichJson.ALL or which_json in self.accept
    def valid_emit(self, which_json):
        return which_json == WhichJson.ALL or which_json in self.emit

_tx_fields = [
    TxField('host',
            accept = [WhichJson.DB],
            emit = [WhichJson.DB]),
    TxField('remote_host',
            # xxx really only accept from gw
            accept = [WhichJson.REST_CREATE,
                      WhichJson.DB],
            emit = [WhichJson.REST_READ,
                    WhichJson.DB],
            from_json=HostPort.from_seq),
    TxField('local_host',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.DB],
            emit=[],
            from_json=HostPort.from_seq),
    TxField('mail_from',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE,
                    WhichJson.DB],
            emit=[WhichJson.DB],
            from_json=Mailbox.from_json),
    TxField('mail_response',
            accept=[],
            emit=[WhichJson.REST_READ],
            from_json=Response.from_json),
    TxField('rcpt_to',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_READ,
                  WhichJson.DB],
            from_json=lambda js: list_from_js(js, Mailbox.from_json)),
    TxField('rcpt_response',
            accept=[],
            emit=[WhichJson.REST_READ],
            from_json=lambda js: list_from_js(js, Response.from_json)),
    TxField('data_response',
            accept=[],
            emit=[WhichJson.REST_READ],
            from_json=Response.from_json),
    TxField('max_attempts',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE],
            emit=[WhichJson.REST_READ]),
    TxField('body',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE],
            emit=[WhichJson.REST_READ]),
]
tx_json_fields = { f.json_field : f for f in _tx_fields }

# NOTE in the Filter api/stack, this is usually interpreted as a delta where
# field == None means "not present in the delta". As such, there is
# currently no representation of "set this field to None".
class TransactionMetadata:
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

    # in rest, this is the url to the body blob, in-memory, it is the id
    # suffix of the blob url
    body : Optional[str] = None

    # filter chain only
    # this object will not change across successive calls to
    # Filter.on_update() but may grow
    body_blob : Optional[Blob] = None

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

    # err on resp fields
    # from_rest_req()
    # to_rest_resp()

    # drop resp fields
    # to_db_json()

    @staticmethod
    def from_json(json, which_js=WhichJson.ALL):
        tx = TransactionMetadata()
        for f in json.keys():
            field = tx_json_fields.get(f, None)
            if not field or not field.valid_accept(which_js):
                return None  # invalid
            js_v = json[f]
            if js_v is None:
                # TODO for now setting a non-null field back to null
                # is not a valid operation so reject json with that
                return None
            if isinstance(js_v, list) and not js_v:
                return None
            v = field.from_json(js_v) if field.from_json else js_v
            # xxx v is None?
            if v is None:
                return None
            setattr(tx, f, v)
        return tx

    def to_json(self, which_js=WhichJson.ALL):
        json = {}
        for name,field in tx_json_fields.items():
            if not field.valid_emit(which_js):
                continue
            if hasattr(self, name) and getattr(self, name) is not None:
                v = getattr(self, name)
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
                    json[name] = v_js
        return json

    # apply a delta to self -> next
    def merge(self, delta : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()

        for f in tx_json_fields.keys():
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
        for f in tx_json_fields.keys():
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
