from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging
import copy

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

    def __str__(self):
        return '%s:%d' % (self.host, self.port)
    def __repr__(self):
        return '%s:%d' % (self.host, self.port)
    def __eq__(self, h : 'HostPort'):
        return self.host == h.host and self.port == h.port

# NOTE the SMTP syntax for the capability list returned from EHLO
# isn't the same as that requested in MAIL/RCPT. This is for the latter.
class EsmtpParam:
    keyword : str
    value : Optional[str]
    def __init__(self, keyword, value = None):
        self.keyword = keyword
        self.value = value

    @staticmethod
    def from_str(s):
        eq = s.find('=')
        if eq == 0:
            return None
        if eq == -1:
            return EsmtpParam(s)
        return EsmtpParam(s[0:eq], s[eq+1:])

    @staticmethod
    def from_json(json : dict):
        if not (k := json.get('k', None)):
            return None
        p = json.get('p', None)
        return EsmtpParam(k, p)

    def to_str(self):
        out = self.keyword
        if self.value:
            out += '=' + self.value
        return out

    def to_json(self):
        json = { 'k': self.keyword }
        if self.value:
            json['p'] = self.value
        return json


class Mailbox:
    mailbox : str  # i.e. rfc5321 4.1.2
    esmtp : List[EsmtpParam]
    def __init__(self, mailbox : str, esmtp : List[EsmtpParam] = []):
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
            out['e'] = [e.to_json() for e in self.esmtp]
        return out

    @staticmethod
    def from_json(json):
        # XXX this should fail pytype since esmtp is arbitrary json
        # (in practice List[str]), Esmtp (above) isn't actually used?
        esmtp = json.get('e', [])
        params = []
        if isinstance(esmtp, list):  # xxx else fail?
            params = [EsmtpParam.from_json(j) for j in esmtp]
        return Mailbox(json['m'], params)

def list_from_js(js, builder):
    return [builder(j) for j in js]

class WhichJson(IntEnum):
    ALL = 0
    REST_READ = 1,
    REST_CREATE = 2,
    REST_UPDATE = 3,
    DB = 4
    DB_ATTEMPT = 5

class TxField:
    json_field : str
    accept = Optional[List[WhichJson]]
    emit = Optional[List[WhichJson]]
    def __init__(self,
                 json_field : str,
                 accept : Optional[List[WhichJson]],
                 emit : Optional[List[WhichJson]],
                 from_json=None,
                 to_json=None):
        self.json_field = json_field
        self.accept = accept
        self.emit = emit
        # none here means identity i.e. plain old data/int/str
        self.from_json = from_json
        self.to_json = to_json

    def valid_accept(self, which_json):
        if self.accept is None:  # internal-only
            return False
        return which_json == WhichJson.ALL or which_json in self.accept
    def valid_emit(self, which_json):
        if self.emit is None:  # internal-only
            return False
        return which_json == WhichJson.ALL or which_json in self.emit

_tx_fields = [
    TxField('host',
            accept = [WhichJson.DB],
            emit = [WhichJson.DB]),
    TxField('remote_host',
            # TODO these accept/emit criteria are more at the syntax
            # level, there also needs to be a policy level e.g. to
            # only accept remote_host from trusted/well-known peers
            # i.e. the smtp gateway
            accept = [WhichJson.REST_CREATE,
                      WhichJson.DB],
            emit = [WhichJson.REST_CREATE,
                    WhichJson.REST_READ,
                    WhichJson.DB],
            from_json=HostPort.from_seq),
    TxField('local_host',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_CREATE,
                  WhichJson.DB],
            from_json=HostPort.from_seq),
    TxField('mail_from',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_CREATE,
                  WhichJson.REST_UPDATE,
                  WhichJson.DB],
            from_json=Mailbox.from_json),
    TxField('mail_response',
            accept=[WhichJson.DB_ATTEMPT],
            emit=[WhichJson.DB_ATTEMPT,
                  WhichJson.REST_READ],
            from_json=Response.from_json),
    TxField('rcpt_to',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_CREATE,
                  WhichJson.REST_UPDATE,
                  WhichJson.DB],
            from_json=lambda js: list_from_js(js, Mailbox.from_json)),
    TxField('rcpt_response',
            accept=[WhichJson.DB_ATTEMPT],
            emit=[WhichJson.DB_ATTEMPT,
                  WhichJson.REST_READ],
            from_json=lambda js: list_from_js(js, Response.from_json)),
    TxField('data_response',
            accept=[WhichJson.DB_ATTEMPT],
            emit=[WhichJson.DB_ATTEMPT,
                  WhichJson.REST_READ],
            from_json=Response.from_json),
    TxField('attempt_count',
            accept=[WhichJson.DB],
            emit=[WhichJson.REST_READ,
                  WhichJson.DB]),
    TxField('body',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_CREATE,
                  WhichJson.REST_UPDATE,
                  WhichJson.REST_READ]),
    TxField('message_builder',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.REST_UPDATE],
            emit=[WhichJson.REST_READ]),
    TxField('notification',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_READ,
                  WhichJson.DB]),
    TxField('retry',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.DB],
            emit=[WhichJson.REST_READ,
                  WhichJson.DB]),
    TxField('smtp_meta',
            accept=[WhichJson.REST_CREATE,
                    WhichJson.DB],
            emit=[WhichJson.DB]),
    TxField('body_blob', accept=None, emit=None),
    TxField('rest_id', accept=None, emit=None),

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

    attempt_count : Optional[int] = None

    # in rest, this is the url to the body blob, in-memory, it is the id
    # suffix of the blob url
    body : Optional[str] = None

    # filter chain only
    # this object will not change across successive calls to
    # Filter.on_update() but may grow
    body_blob : Optional[Blob] = None

    message_builder : Optional[dict] = None

    # arbitrary json for now
    notification : Optional[dict] = None
    retry : Optional[dict] = None

    smtp_meta: Optional[dict] = None

    remote_hostname : Optional[str] = None
    fcrdns : Optional[bool] = None
    rest_id : Optional[str] = None

    def __init__(self, local_host : Optional[HostPort] = None,
                 remote_host : Optional[HostPort] = None,
                 mail_from : Optional[Mailbox] = None,
                 mail_response : Optional[Response] = None,
                 rcpt_to : Optional[List[Mailbox]] = None,
                 rcpt_response : Optional[List[Response]] = None,
                 host : Optional[str] = None,
                 body : Optional[str] = None,
                 body_blob : Optional[Blob] = None,
                 data_response : Optional[Response] = None,
                 notification : Optional[dict] = None,
                 retry : Optional[dict] = None,
                 smtp_meta : Optional[dict] = None):
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.mail_response = mail_response
        self.rcpt_to = rcpt_to if rcpt_to else []
        self.rcpt_response = rcpt_response if rcpt_response else []
        self.host = host
        self.body = body
        self.body_blob = body_blob
        self.data_response = data_response
        self.notification = notification
        self.retry = retry
        self.smtp_meta = smtp_meta

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
        out += 'body=%s ' % (self.body)
        out += 'body_blob=%s ' % (self.body_blob)
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


    # returns True if there is a request field (mail/rcpt/data)
    # without a corresponding response field in tx
    def req_inflight(self, tx : Optional['TransactionMetadata'] = None) -> bool:
        if tx is None:
            tx = self
        if self.mail_from and not tx.mail_response:
            return True
        if len(self.rcpt_to) > len(tx.rcpt_response):
            return True
        for i in range(0,len(self.rcpt_to)):
            if self.rcpt_to[i] is not None and (
                    i >= len(tx.rcpt_response) or
                    tx.rcpt_response[i] is None):
                return True
        body_blob_last = self.body_blob is not None and (
            self.body_blob.len() == self.body_blob.content_length())
        # xxx BEFORE SUBMIT
        if (self.body or body_blob_last) and tx.data_response is None:
            return True
        return False

    # Converts a response field (mail/rcpt/data) to json following the
    # convention that if the corresponding request field is populated,
    # return {} for the response to indicate that it is accepted and
    # inflight.
    # TODO in hindsight, this is perhaps not the best representation,
    # probably it should just return the req fields instead
    def _resp_field(self, name, field, which_js : WhichJson, json):
        if which_js != WhichJson.REST_READ or (
                name not in ["mail_response", "rcpt_response",
                             "data_response"]):
            return False
        if name in ["mail_response", "data_response"]:
            if ((name == 'mail_response' and self.mail_from and
                 not self.mail_response) or
                (name == 'data_response' and (self.body or self.body_blob) and
                 not self.data_response)):
                json[name] = {}
                return True
            return False

        assert name == 'rcpt_response'
        if not self.rcpt_to:
            return False

        out = [{}] * len(self.rcpt_to)
        for i in range(0, len(self.rcpt_to)):
            if (self.rcpt_response and i < len(self.rcpt_response) and
                self.rcpt_response[i] is not None):
                out[i] = self.rcpt_response[i].to_json()

        json[name] = out
        return True

    def _field_to_json(self, name, field, which_js : WhichJson, json):
        if not field.valid_emit(which_js):
            return

        v_js = None
        if self._resp_field(name, field, which_js, json):
            return
        if not hasattr(self, name):
            return
        if (v := getattr(self, name)) is None:
            return

        if isinstance(v, str) or isinstance(v, int) or isinstance(v, dict):
            v_js = v
        elif isinstance(v, list):
            if v:
                v_js = [vv.to_json() for vv in v]
        else:
            v_js = v.to_json()
        if v_js is not None:
            json[name] = v_js


    def to_json(self, which_js=WhichJson.ALL):
        json = {}
        for name,field in tx_json_fields.items():
            self._field_to_json(name, field, which_js, json)
        return json

    # self + delta -> out or new tx obj if out is None
    def merge(self, delta : "TransactionMetadata",
              out : Optional["TransactionMetadata"] = None
              ) -> Optional["TransactionMetadata"]:
        if out is None:
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
                # use the old value, assume the new one is the same
                # TODO could verify that old_v == new_v
                setattr(out, f, old_v)
                continue
            l = []
            l.extend(old_v)
            l.extend(new_v)
            setattr(out, f, l)

        return out

    # merge delta into self
    def merge_from(self, delta):
        return self.merge(delta, self)

    def replace_from(self, tx):
        for f in tx_json_fields.keys():
            setattr(self, f, getattr(tx, f, None))

    # compute a delta from self to successor
    def delta(self, successor : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()
        for f in tx_json_fields.keys():
            old_v = getattr(self, f, None)
            new_v = getattr(successor, f, None)
            if (old_v is not None) and (new_v is None):
                logging.debug('tx.delta invalid del %s', f)
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
                logging.debug('tx.delta is-list != is-list %s', f)
                return None  # invalid
            if not isinstance(old_v, list):
                logging.debug('tx.delta value change %s', f)
                if old_v != new_v:
                    return None  # invalid
                continue

            old_len = len(old_v)
            if old_len > len(new_v):
                logging.debug('tx.delta invalid list trunc %s', f)
                return None  # invalid
            if new_v[0:old_len] != old_v:
                logging.debug('tx.delta changed list prefix %s', f)
                return None
            setattr(out, f, new_v[old_len:])

        return out

    def copy(self):
        out = copy.copy(self)
        out.rcpt_to = list(self.rcpt_to)
        out.rcpt_response = list(self.rcpt_response)
        return out

class Filter(ABC):
    # XXX needs to return some errors e.g. http 412 directly instead of via tx

    # XXX we need to say a lot more about the protocol here
    # - resp for all req
    # - tx is a delta
    #   -- but tx.body_blob isn't a delta: grows across successive calls
    #   -- same object?
    # - is the tx object the same across multiple calls?
    # - mutation?

    # TODO remove this timeout -> AsyncFilter
    # only set by Exploder->StorageWriter and bounce injection
    @abstractmethod
    def on_update(self, transaction_metadata : TransactionMetadata,
                  timeout : Optional[float] = None):
        pass

    def abort(self):
        raise NotImplementedError()


class AsyncFilter(ABC):
    # may return None for reqs on timeout
    # returns full tx state
    @abstractmethod
    def update(self, transaction_metadata : TransactionMetadata,
               timeout : Optional[float] = None):
        pass

    @abstractmethod
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        pass

    # returns a "cached" value from the last get/update
    @abstractmethod
    def version(self) -> int:
        pass
