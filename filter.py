from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod
import logging
import copy

from response import Response

from blob import Blob, WritableBlob
from rest_schema import BlobUri

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
    def __eq__(self, x):
        if not isinstance(x, Mailbox):
            return False
        return self.mailbox == x.mailbox

    def __str__(self):
        return self.mailbox
    def __repr__(self):
        return self.mailbox

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

FromJson = Callable[[Dict[object, object]], object]
ToJson = Callable[[Any], Dict[object, object]]
class TxField:
    json_field : str

    validity = Optional[set[WhichJson]]

    rest_placeholder : bool
    from_json : Optional[FromJson] = None
    to_json : Optional[ToJson] = None
    is_list : bool = False

    def __init__(self,
                 json_field : str,
                 validity : Optional[set[WhichJson]] = None,
                 from_json : Optional[FromJson] = None,
                 to_json : Optional[ToJson] = None,
                 rest_placeholder : bool = False,
                 is_list : bool = False):
        self.json_field = json_field
        self.validity = validity

        # none here means identity i.e. plain old data/int/str
        self.from_json = from_json
        self.to_json = to_json
        self.rest_placeholder = rest_placeholder
        self.is_list = is_list

    def valid(self, which_json : WhichJson):
        if self.validity is None:  # internal-only
            return False
        return which_json == WhichJson.ALL or (which_json in self.validity)

    def emit_rest_placeholder(self, which_json):
        if which_json == WhichJson.REST_READ and self.rest_placeholder:
            return True
        return False

    def list_offset(self):
        assert self.is_list
        return self.json_field + '_list_offset'

_tx_fields = [
    TxField('host',
            validity = set([WhichJson.DB])),
    TxField('remote_host',
            # TODO these accept/emit criteria are more at the syntax
            # level, there also needs to be a policy level e.g. to
            # only accept remote_host from trusted/well-known peers
            # i.e. the smtp gateway
            validity = set([WhichJson.REST_CREATE,
                            WhichJson.REST_READ,
                            WhichJson.DB]),
            to_json=HostPort.to_json,
            from_json=HostPort.from_seq),
    TxField('local_host',
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.REST_READ,
                          WhichJson.DB]),
            to_json=HostPort.to_json,
            from_json=HostPort.from_seq),
    TxField('mail_from',
            rest_placeholder=True,
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.REST_UPDATE,
                          WhichJson.REST_READ,
                          WhichJson.DB]),
            from_json=Mailbox.from_json,
            to_json=Mailbox.to_json),
    TxField('mail_response',
            validity=set([WhichJson.DB_ATTEMPT,
                          WhichJson.REST_READ]),
            to_json=Response.to_json,
            from_json=Response.from_json),
    TxField('rcpt_to',
            rest_placeholder=True,
            is_list=True,
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.REST_UPDATE,
                          WhichJson.REST_READ,
                          WhichJson.DB]),
            to_json=Mailbox.to_json,
            from_json=Mailbox.from_json),
    TxField('rcpt_response',
            is_list=True,
            validity=set([WhichJson.DB_ATTEMPT,
                          WhichJson.REST_READ]),
            to_json=Response.to_json,
            from_json=Response.from_json),
    TxField('data_response',
            validity=set([WhichJson.DB_ATTEMPT,
                          WhichJson.REST_READ]),
            to_json=Response.to_json,
            from_json=Response.from_json),
    TxField('attempt_count',
            validity=set([WhichJson.DB,
                          WhichJson.REST_READ])),
    TxField('body',
            rest_placeholder=True,
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.REST_UPDATE,
                          WhichJson.REST_READ])),  # xxx WhichJson.DB],
    TxField('message_builder',
            rest_placeholder=True,
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.REST_UPDATE,
                          WhichJson.REST_READ ])),
    TxField('notification',
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.DB])),
    TxField('retry',
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.REST_READ,
                          WhichJson.DB])),
    TxField('smtp_meta',
            validity=set([WhichJson.REST_CREATE,
                          WhichJson.DB])),
    TxField('body_blob', validity=None),
    TxField('rest_id', validity=None),
    TxField('remote_hostname', validity=None),
    TxField('fcrdns', validity=None),
    TxField('tx_db_id', validity=None),
    TxField('inline_body', validity=set([WhichJson.REST_CREATE])),
]
tx_json_fields = { f.json_field : f for f in _tx_fields }

# NOTE in the Filter api/stack, this is usually interpreted as a delta
# where field == None means "not present in the delta" as opposed to
# "set field to None." In terms of json patch, it's a delta that
# contains only "add" operations.
class TransactionMetadata:
    host : Optional[str] = None
    rest_endpoint : Optional[str] = None
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None

    mail_from : Optional[Mailbox] = None
    mail_response : Optional[Response] = None
    # TODO more type-safe treatment of placeholder values, this should
    # only contain None in a delta
    rcpt_to : List[Optional[Mailbox]]
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
    tx_db_id : Optional[int] = None
    inline_body : Optional[str] = None

    def __init__(self, 
                 local_host : Optional[HostPort] = None,
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
                 smtp_meta : Optional[dict] = None,
                 message_builder : Optional[dict] = None,
                 inline_body : Optional[str] = None):
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
        self.message_builder = message_builder
        self.inline_body = inline_body

    def __repr__(self):
        out = ''
        out += 'mail_from=%s mail_response=%s ' % (
            self.mail_from, self.mail_response)
        out += 'rcpt_to=%s rcpt_response=%s ' % (
            self.rcpt_to, self.rcpt_response)
        out += 'body=%s ' % (self.body)
        out += 'body_blob=%s ' % (self.body_blob)
        out += 'message_builder=%s ' % (self.message_builder)
        out += 'data_response=%s' % self.data_response
        return out

    def __bool__(self):
        for name,field in tx_json_fields.items():
            if not hasattr(self, name):
                continue
            if (v:= getattr(self, name)) is None:
                continue
            if field.is_list:
                if bool(v):
                    return True
            else:
                return True

        return False


    @staticmethod
    def from_json(json, which_js=WhichJson.ALL):
        tx = TransactionMetadata()
        for f in json.keys():
            if which_js == WhichJson.REST_UPDATE and f.endswith('_list_offset'):  # XXX FIX BEFORE SUBMIT
                continue
            field = tx_json_fields.get(f, None)
            if not field or not field.valid(which_js):
                return None  # invalid
            js_v = json[f]
            if js_v is None:
                # TODO for now setting a non-null field back to null
                # is not a valid operation so reject json with that
                return None
            if isinstance(js_v, list) and not js_v:
                return None
            if field.is_list:
                if not isinstance(js_v, list):
                    return None
                if field.emit_rest_placeholder(which_js):
                    v = [None for v in js_v]
                else:
                    v = [field.from_json(v) for v in js_v]
                if field.list_offset() in json and which_js == WhichJson.REST_UPDATE:
                    offset = json.get(field.list_offset())
                    setattr(tx, field.list_offset(), offset)
            else:
                if field.emit_rest_placeholder(which_js):
                    if js_v != {}:
                        return None
                    else:
                        v = None
                else:
                    v = field.from_json(js_v) if field.from_json else js_v

            setattr(tx, f, v)
        return tx


    # returns True if there is a request field (mail/rcpt/data)
    # without a corresponding response field in tx
    # XXX depending on the context, the upstream will not populate
    # replies e.g. after upstream mail_resp err?
    def req_inflight(self, tx : Optional['TransactionMetadata'] = None) -> bool:
        if tx is None:
            tx = self
        if (self.mail_from is not None) and (tx.mail_response is None):
            return True
        # cannot make forward progress
        if tx.mail_response is not None and tx.mail_response.err():
            return False
        if len(self.rcpt_to) > len(tx.rcpt_response):
            return True
        for i in range(0,len(self.rcpt_to)):
            # XXX rcpt_response should never be None now?
            if self.rcpt_to[i] is not None and tx.rcpt_response[i] is None:
                return True
        body_blob_last = self.body_blob is not None and (
            self.body_blob.finalized())
        if (self.body or body_blob_last or self.message_builder
            ) and tx.data_response is None:
            return True
        return False

    # for sync filter api, e.g. if a rest call failed, fill resps for
    # all inflight reqs
    # TODO possibly this should populate the first of mail/rcpt/data
    # and either leave the rest unset or set them to "failed
    # precondition/bad sequence of commands"
    def fill_inflight_responses(self, resp : Response, dest=None):
        if dest is None:
            dest = self
        if self.mail_from and not self.mail_response:
            dest.mail_response = resp
        dest.rcpt_response.extend(
            [resp] * (len(self.rcpt_to) - len(self.rcpt_response)))
        body_blob_last = self.body_blob is not None and (
            self.body_blob.finalized())
        if body_blob_last and self.data_response is None:
            dest.data_response = resp

    def _field_to_json(self, name : str, field : TxField,
                       which_js : WhichJson, json):
        if not field.valid(which_js):
            return

        v_js = None
        if not hasattr(self, name):
            return
        if (v := getattr(self, name)) is None:
            return

        if isinstance(v, list):
            if v:
                if field.emit_rest_placeholder(which_js):
                    v_js = [{}] * len(v)
                else:
                    v_js = [vv.to_json() for vv in v]
                offset = getattr(self, field.list_offset(), None)
                if which_js == WhichJson.REST_UPDATE and offset:
                    json[field.list_offset()] = offset
        elif field.emit_rest_placeholder(which_js):
            v_js = {}
        elif field.to_json is not None:
            v_js = field.to_json(v)
        else:  # POD
            v_js = v

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
        assert delta is not None
        if out is None:
            out = TransactionMetadata()

        for f,field in tx_json_fields.items():
            old_v = getattr(self, f, None)
            new_v = getattr(delta, f, None)
            if old_v is None and new_v is not None:
                setattr(out, f, new_v)
                continue
            if old_v is not None and new_v is None:
                setattr(out, f, old_v)
                continue

            # XXX TxField.is_list?
            if isinstance(old_v, list) != isinstance(new_v, list):
                logging.debug('list-ness mismatch')
                return None  # invalid
            if not isinstance(old_v, list):
                # use the old value, assume the new one is the same
                # TODO could verify that old_v == new_v
                setattr(out, f, old_v)
                continue
            if not(new_v):
                setattr(out, f, old_v)
                continue
            offset = getattr(delta, field.list_offset(), 0)
            if offset != len(old_v):
                logging.debug('list offset mismatch %s old %d new %s',
                              f, len(old_v), offset)
                return None
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
    def delta(self, successor : "TransactionMetadata",
              which_json : Optional[WhichJson] = None
              ) -> Optional["TransactionMetadata"]:
        assert successor is not None
        out = TransactionMetadata()
        for (f,json_field) in tx_json_fields.items():
            old_v = getattr(self, f, None)
            new_v = getattr(successor, f, None)
            # logging.debug('tx.delta %s %s %s', f, old_v, new_v)
            if ((which_json is not None) and not json_field.valid(which_json)):
                continue  # ignore
            if ((which_json is not None) and (
                    json_field.emit_rest_placeholder(which_json)) and
                (old_v is not None and new_v is None)):
                continue
            if (old_v is not None) and (new_v is None):
                logging.debug('tx.delta invalid del %s', f)
                #raise ValueError()
                return None  # invalid
            if (old_v is None) and (new_v is not None):
                setattr(out, f, new_v)
                continue

            # XXX TxField.is_list?
            if isinstance(old_v, list) != isinstance(new_v, list):
                logging.debug('tx.delta is-list != is-list %s', f)
                return None  # invalid
            if not isinstance(old_v, list):
                if old_v != new_v:
                    logging.debug('tx.delta value change %s %s %s',
                                  f, old_v, new_v)
                    return None  # invalid
                setattr(out, f, None)
                continue

            if json_field.emit_rest_placeholder(which_json):
                if any([x != None for x in new_v]):
                    logging.debug('non-None placeholder')
                    return None
                if len(new_v) < len(old_v):
                    logging.debug('list shrink placeholder')
                    return None
                # XXX need this??
                # setattr(out, json_field.list_offset(), len(old_v))
                continue

            old_len = len(old_v)
            if old_len > len(new_v):
                logging.debug('tx.delta invalid list trunc %s', f)
                return None  # invalid
            for i in range(0, old_len):
                if old_v[i] is None and new_v[i] is not None:
                    pass  #ok   XXX why would old_v be None?
                if old_v[i] is not None and new_v[i] is None:
                    return None  # bad
                if old_v[i] != new_v[i]:
                    return None  # bad
            setattr(out, f, new_v[old_len:])
            setattr(out, json_field.list_offset(), old_len)

        return out

    # NOTE this copies the rcpt req/resp lists which we know we mutate
    # but not the underlying Mailbox/Response objects which shouldn't
    # be mutated.
    def copy(self):
        # TODO probably this should use tx_json_fields?
        out = copy.copy(self)
        out.rcpt_to = list(self.rcpt_to)
        out.rcpt_response = list(self.rcpt_response)
        return out

    def copy_valid(self, valid : WhichJson):
        out = TransactionMetadata()
        for name,field in tx_json_fields.items():
            if not field.valid(valid):
                continue
            v = getattr(self, name, None)
            if v is None:
                continue
            if field.is_list:
                v = list(v)
                if valid == WhichJson.REST_UPDATE and (
                        hasattr(self, field.list_offset())):
                    setattr(out, field.list_offset(),
                            getattr(self, field.list_offset()))
            setattr(out, name, v)
        return out


# NOTE Sync and Async here are with respect to the transaction
# responses, not program execution.

# state after previous call to sync_filter.on_update()
# prev_tx : TransactionMetadata
# delta : TransactionMetadata
# tx = prev_tx.merge(delta)
# new_tx = tx.copy()
# upstream_delta = sync_filter.on_update(new_tx, downstream_delta)
# assert tx.merge(upstream_delta) == new_tx

class SyncFilter(ABC):
    # tx is the full state vector
    # tx_delta is what's new since the last call
    # returns delta of what was added upstream
    # only returns None on invalid delta i.e. dropped fields
    @abstractmethod
    def on_update(self, tx : TransactionMetadata,
                  tx_delta : TransactionMetadata
                  ) -> Optional[TransactionMetadata]:
        pass

# interface for injecting new transactions
class AsyncFilter(ABC):
    # may return None for reqs on timeout/still inflight
    # may continue after this
    @abstractmethod
    def update(self,
               tx : TransactionMetadata,
               tx_delta : TransactionMetadata,
               timeout : Optional[float] = None
               ) -> Optional[TransactionMetadata]:
        pass

    @abstractmethod
    def get(self, timeout : Optional[float] = None
            ) -> Optional[TransactionMetadata]:
        pass

    # pass exactly one of blob_rest_id or tx_body=True
    @abstractmethod
    def get_blob_writer(
            self,
            create : bool,
            blob_rest_id : Optional[str] = None,
            tx_body : Optional[bool] = None,
            # rest id of a tx to copy the body from
            # requires create and tx_body
            copy_from_tx_body : Optional[str] = None
    ) -> Optional[WritableBlob]:
        pass


    # returns a "cached" value from the last get/update
    @abstractmethod
    def version(self) -> int:
        pass
