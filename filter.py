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

class TransactionMetadata:
    single_fields = ['host']
    host_port_fields = ['local_host', 'remote_host']
    mailbox_fields = ['mail_from']
    all_fields = single_fields + host_port_fields + mailbox_fields + ['rcpt_to']

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

    def __init__(self, local_host : Optional[HostPort] = None,
                 remote_host : Optional[HostPort] = None,
                 mail_from : Optional[Mailbox] = None,
                 rcpt_to : Optional[List[Mailbox]] = None,
                 host : Optional[str] = None):
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.rcpt_to = rcpt_to if rcpt_to else []
        self.rcpt_response = []
        self.host = host

    def __bool__(self):
        for f in TransactionMetadata.all_fields:
            if hasattr(self, f) and bool(getattr(self, f)):
                return True
        return False

    @staticmethod
    def from_json(json):
        tx =  TransactionMetadata()
        for f in TransactionMetadata.mailbox_fields:
            if f in json.keys():
                setattr(tx, f, Mailbox.from_json(json[f]))
        for f in TransactionMetadata.host_port_fields:
            if f in json.keys():
                h,p = json[f]
                setattr(tx, f, HostPort(h,p))
        if 'rcpt_to' in json and json['rcpt_to']:
            tx.rcpt_to = [Mailbox.from_json(r) for r in json['rcpt_to']]
        for f in TransactionMetadata.single_fields:
            if f in json.keys():
                setattr(tx, f, json[f])
        return tx

    def to_json(self):
        json = {}
        for f in TransactionMetadata.mailbox_fields:
            if hasattr(self, f) and getattr(self, f):
                json[f] = getattr(self, f).to_json()
        for f in TransactionMetadata.host_port_fields:
            if hasattr(self, f) and getattr(self, f):
                json[f] = getattr(self, f).to_tuple()
        if self.rcpt_to:
            json['rcpt_to'] = [r.to_json() for r in self.rcpt_to]
        for f in TransactionMetadata.single_fields:
            if hasattr(self, f) and getattr(self, f):
                json[f] = getattr(self, f)
        return json

    def merge(self, delta : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()
        for f in TransactionMetadata.single_fields + TransactionMetadata.host_port_fields + TransactionMetadata.mailbox_fields:
            if hasattr(delta, f) and getattr(delta, f):
                if hasattr(self, f) and getattr(self, f):
                    return None
                setattr(out, f, getattr(delta, f))
            elif hasattr(self, f) and getattr(self, f):
                setattr(out, f, getattr(self, f))
        out.rcpt_to.extend(self.rcpt_to)
        out.rcpt_to.extend(delta.rcpt_to)
        return out

    def delta(self, next : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()
        for f in TransactionMetadata.single_fields + TransactionMetadata.host_port_fields + TransactionMetadata.mailbox_fields:
            if hasattr(next, f) and getattr(next, f):
                if not hasattr(self, f) or not getattr(self, f):
                    setattr(out, f, getattr(next, f))
            elif hasattr(self, f) and getattr(self, f):
                logging.info('invalid delta %s', f)
                return None

        # rcpts must be a prefix
        old_len = len(self.rcpt_to)
        if len(next.rcpt_to) < old_len or self.rcpt_to != next.rcpt_to[0:old_len]:
            logging.info('invalid delta rcpt %s %s', self.rcpt_to, next.rcpt_to)
            return None
        out.rcpt_to = next.rcpt_to[old_len:]
        return out

class Filter(ABC):
    @abstractmethod
    def on_update(self, transaction_metadata : TransactionMetadata):
        pass

    @abstractmethod
    def append_data(self, last : bool, blob : Blob) -> Response:
        pass

    @abstractmethod
    def abort(self):
        pass
