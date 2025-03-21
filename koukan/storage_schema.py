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
    # TODO maybe split uri into create vs reuse, currently implicit by
    # whether uri has tx_id
    uri : Optional[BlobUri] = None
    def __init__(self, blob : Optional[Blob] = None,
                 uri : Optional[BlobUri] = None):
        self.blob = blob
        self.uri = uri
