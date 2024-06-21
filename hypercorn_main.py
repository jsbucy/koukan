from typing import List, Tuple
import logging

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
    asyncio.run(serve(app,
                      config,
                      shutdown_trigger=lambda: asyncio.Future()))
