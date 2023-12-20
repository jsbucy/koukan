
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

class TransactionMetadata:
    rest_endpoint : Optional[str] = None
    remote_host : Optional[HostPort] = None
    local_host : Optional[HostPort] = None
    # tls mumble

class Esmtp:
    # from aiosmtpd
    # TODO parse out into keyword and key/value
    capabilities : List[str]

class Mailbox:
    mailbox : str  # i.e. rfc5321 4.1.2
    esmtp : Esmtp

    def local_part(self) -> Optional[str]:
        pass
    def domain(self) -> Optional[str]:
        pass

class Filter(ABC):
    @abstractmethod
    def start(self,
              transaction_metadata : TransactionMetadata,
              mail_from : str, transaction_esmtp : Optional[Any],
              rcpt_to : str, rcpt_esmtp : Optional[Any]):
        pass

    @abstractmethod
    def append_data(self, last : bool, blob : Blob):
        pass

    @abstractmethod
    def abort(self):
        pass
