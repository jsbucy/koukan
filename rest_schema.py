from typing import Optional, Tuple

def make_tx_uri(tx):
    return '/transactions/' + tx

def make_blob_uri(tx, blob : Optional[str] = None,
                  tx_body : Optional[bool] = None) -> str:
    assert not (blob and tx_body)
    assert blob or tx_body
    uri = make_tx_uri(tx)
    if blob:
        uri += ('/blob/' + blob)
    else:
        uri += '/body'
    return uri

class BlobUri:
    tx_id : str
    tx_body : bool = False
    blob : Optional[str] = None
    def __init__(self, tx_id, tx_body = False, blob = None):
        self.tx_id = tx_id
        self.tx_body = tx_body
        self.blob = blob

def parse_blob_uri(uri) -> Optional[BlobUri]:
    if not uri.startswith('/transactions/'):
        return None
    u = uri.removeprefix('/transactions/')
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
