from typing import Callable, List, Optional, Tuple
import logging
from functools import partial
import asyncio
import logging

from hypercorn.config import Config
from hypercorn.asyncio import serve


def run(bind : List[Tuple[str,int]], cert, key, app, shutdown,
        alive : Optional[Callable] = None):
    config = Config()
    config.bind = [('%s:%d' % (h,p)) for h,p in bind]
    if cert and key:
        logging.debug('cert %s key %s', cert, key)
        config.certfile = cert
        config.keyfile = key
    async def _ping_alive():
        while True:
            logging.debug('_ping_alive')
            alive()
            await asyncio.sleep(1)

    async def _run():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        task = None
        if alive:
            task = loop.create_task(_ping_alive())
        shutdown.append(fut)
        await serve(app, config, shutdown_trigger=lambda: fut)
        if task:
            task.cancel()
    asyncio.run(_run())
