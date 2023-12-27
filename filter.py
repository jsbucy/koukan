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
    single_fields = ['local_host', 'remote_host', 'host']
    mailbox_fields = ['mail_from', 'rcpt_to']

    rest_endpoint : Optional[str] = None
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None
    # tls mumble

    mail_from : Optional[Mailbox] = None
    mail_response : Optional[Response] = None
    rcpt_to : Optional[Mailbox] = None
    rcpt_response : Optional[Response] = None
    data_response : Optional[Response] = None

    def __init__(self, local_host : Optional[str] = None,
                 remote_host : Optional[str] = None,
                 mail_from : Optional[Mailbox] = None,
                 rcpt_to : Optional[Mailbox] = None,
                 host : Optional[str] = None):
        self.local_host = local_host
        self.remote_host = remote_host
        self.mail_from = mail_from
        self.rcpt_to = rcpt_to
        self.host = host

    @staticmethod
    def from_json(json):
        tx =  TransactionMetadata()
        for f in TransactionMetadata.mailbox_fields:
            if f in json.keys():
                setattr(tx, f, Mailbox.from_json(json[f]))
        for f in TransactionMetadata.single_fields:
            if f in json.keys():
                setattr(tx, f, json[f])
        return tx

    def to_json(self):
        json = {}
        for f in TransactionMetadata.mailbox_fields:
            if hasattr(self, f) and getattr(self, f):
                json[f] = getattr(self, f).to_json()
        for f in TransactionMetadata.single_fields:
            if hasattr(self, f) and getattr(self, f):
                json[f] = getattr(self, f)
        return json

    def merge(self, delta : "TransactionMetadata"
              ) -> Optional["TransactionMetadata"]:
        out = TransactionMetadata()
        for f in TransactionMetadata.single_fields + TransactionMetadata.mailbox_fields:
            if hasattr(delta, f) and getattr(delta, f):
                if hasattr(self, f) and getattr(self, f):
                    return None
                setattr(out, f, getattr(delta, f))
            elif hasattr(self, f) and getattr(self, f):
                setattr(out, f, getattr(self, f))
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
