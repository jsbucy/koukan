from typing import List, Tuple
import logging

# the asgiref implementation seems to run the app in a single
# thread. Further down the sync_to_async stack it supports passing in
# an executor but this isn't propagated all the way to this entry
# point.
#from asgiref.wsgi import WsgiToAsgi
from a2wsgi import WSGIMiddleware

from hypercorn.config import Config
import asyncio
from hypercorn.asyncio import serve


def run(bind : List[Tuple[str,int]], cert, key, app):
    config = Config()
    config.bind = [('%s:%d' % (h,p)) for h,p in bind]
    if cert and key:
        logging.debug('cert %s key %s', cert, key)
        config.certfile = cert
        config.keyfile = key
    asyncio.run(serve(WSGIMiddleware(app), config))
