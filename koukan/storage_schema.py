# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
import logging

from enum import IntEnum
from typing import Dict, Optional
from koukan.rest_schema import BlobUri
from koukan.blob import Blob

class VersionConflictException(Exception):
    pass

def internal_blob_prefix(b):
    return b.startswith('__internal')

TX_BODY='__internal_tx_body'

def body_blob_uri(uri : BlobUri) -> BlobUri:
    if uri.blob and internal_blob_prefix(uri.blob):
        raise ValueError()
    if not uri.tx_body:
        return uri
    return BlobUri(tx_id = uri.tx_id, tx_body=True, blob=TX_BODY)

class BlobSpec:
    blob : Optional[Blob] = None
    create_id : Optional[str] = None
    reuse_uri : Optional[BlobUri] = None
    create_tx_body : Optional[bool] = None
    finalized : bool = False

    def __init__(self, blob : Optional[Blob] = None,
                 create_id : Optional[str] = None,
                 reuse_uri : Optional[BlobUri] = None,
                 create_tx_body : Optional[bool] = None):
        # TODO put back assert, blob and create_id can both be present if inline
        self.blob = blob
        self.create_id = create_id
        self.reuse_uri = reuse_uri
        self.create_tx_body = create_tx_body

    # xxx hack
    def delta(self, rhs, which_json) -> Optional[bool]:
        if not isinstance(rhs, BlobSpec):
            raise ValueError()
            return None
        if self.reuse_uri is not None and rhs.reuse_uri is None:
            raise ValueError()
        # elif self.reuse_uri is None and rhs.reuse_uri is not None:
        #     pass
        elif self.reuse_uri is not None and rhs.reuse_uri is not None:
            if self.reuse_uri.parsed_uri and (self.reuse_uri.parsed_uri != rhs.reuse_uri.parsed_uri):
                logging.debug(self.reuse_uri)
                logging.debug(rhs.reuse_uri)
                raise ValueError()
                return None
        if self.finalized and not rhs.finalized:
            raise ValueError()
            return None
        return self.finalized != rhs.finalized

    def __repr__(self):
        return '%s %s %s %s %s' % (
            self.blob,
            self.create_id,
            self.reuse_uri,
            self.create_tx_body,
            self.finalized)
