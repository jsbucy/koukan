
from enum import IntEnum
from typing import Any, List, Optional, Tuple, TypeAlias
from abc import ABC, abstractmethod

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

class TransactionMetadata:
    rest_endpoint : Optional[str] = None
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None
    # tls mumble

    mail_from : Optional[Mailbox] = None
    mail_response : Optional[Response] = None
    rcpt_to : Optional[Mailbox] = None
    rcpt_response : Optional[Response] = None

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
