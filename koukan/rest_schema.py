# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Tuple

from urllib.parse import urljoin, urlparse

FINALIZE_BLOB_HEADER = 'x-finalize-blob-length'

def make_tx_uri(tx):
    return '/transactions/' + tx

def make_blob_uri(tx, blob : Optional[str] = None,
                  tx_body : Optional[bool] = None,
                  base_uri : Optional[str] = None) -> str:
    assert not (blob and tx_body)
    assert blob or tx_body
    uri = make_tx_uri(tx)
    if base_uri is not None:
        uri = urljoin(base_uri, uri)
    if blob:
        uri += ('/blob/' + blob)
    else:
        uri += '/body'
    return uri

class BlobUri:
    tx_id : str
    tx_body : bool = False
    blob : Optional[str] = None
    base_uri : Optional[str] = None
    def __init__(self, tx_id : str, tx_body : bool = False,
                 blob : Optional[str] = None):
        assert tx_body or blob
        # TODO storage instantiates this with tx_body and __internal_tx_body
        # but if RestHandler instantiates with both, it's a bug
        #assert not (tx_body and blob)
        self.tx_id = tx_id
        self.tx_body = tx_body
        self.blob = blob
    def __repr__(self):
        out = 'tx_id=' + self.tx_id + ' tx_body=' + str(self.tx_body)
        if self.blob:
            out += ' blob=' + self.blob
        return out
    def __eq__(self, rhs):
        if not isinstance(rhs, BlobUri):
            return False
        return self.tx_id == rhs.tx_id and self.tx_body == rhs.tx_body and self.blob == rhs.blob


def parse_blob_uri(uri) -> Optional[BlobUri]:
    result = urlparse(uri)
    if result is None:
        return None
    if not result.path.startswith('/transactions/'):
        return None
    u = result.path.removeprefix('/transactions/')
    slash = u.find('/')
    if slash == -1:
        return None
    tx = u[0:slash]
    u = u[slash+1:]
    if u == 'body':
        return BlobUri(tx_id=tx, tx_body=True)
    if not u.startswith('blob/'):
        return None
    u = u.removeprefix('blob/')
    if not u:
        return None
    return BlobUri(tx_id=tx, blob=u)
