from typing import Optional, Tuple

def make_blob_uri(tx, blob : Optional[str] = None,
                  tx_body : Optional[bool] = None) -> str:
    assert not (blob and tx_body)
    assert blob or tx_body
    uri = '/transactions/' + tx
    if blob:
        uri += ('/blob/' + blob)
    else:
        uri += '/body'
    return uri

def parse_blob_uri(uri) -> Optional[Tuple[str,str]]:
    if not uri.startswith('/transactions/'):
        return None
    u = uri.removeprefix('/transactions/')
    slash = u.find('/')
    if slash == -1:
        return None
    tx = u[0:slash]
    u = u[slash+1:]
    if not u.startswith('blob/'):
        return None
    u = u.removeprefix('blob/')
    if not u:
        return None
    return tx, u
