# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from enum import IntEnum
from typing import Dict, Optional
from koukan.rest_schema import BlobUri

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
