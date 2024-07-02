from typing import List, Tuple
import logging

from hypercorn.config import Config
import asyncio
from hypercorn.asyncio import serve



def run(bind : List[Tuple[str,int]], cert, key, app, shutdown):
    config = Config()
    config.bind = [('%s:%d' % (h,p)) for h,p in bind]
    if cert and key:
        logging.debug('cert %s key %s', cert, key)
        config.certfile = cert
        config.keyfile = key
    async def _run():
        fut = asyncio.get_running_loop().create_future()
        shutdown[0] = fut
        await serve(app, config, shutdown_trigger=lambda: fut)
    asyncio.run(_run())
