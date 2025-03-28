# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
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

    def __init__(self, blob : Optional[Blob] = None,
                 create_id : Optional[str] = None,
                 reuse_uri : Optional[BlobUri] = None,
                 create_tx_body : Optional[bool] = None):
        assert len([x for x in [blob, create_id, reuse_uri, create_tx_body]
                    if x is not None]) == 1
        self.blob = blob
        self.create_id = create_id
        self.reuse_uri = reuse_uri
        self.create_tx_body = create_tx_body
