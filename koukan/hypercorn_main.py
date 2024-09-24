# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional, Tuple
import logging
from functools import partial
import asyncio
import logging

from hypercorn.config import Config
from hypercorn.asyncio import serve


def run(bind : List[Tuple[str,int]], cert, key, app,
        shutdown : asyncio.Event,
        alive : Optional[Callable] = None):
    config = Config()
    config.bind = [('%s:%d' % (h,p)) for h,p in bind]
    if cert and key:
        logging.debug('cert %s key %s', cert, key)
        config.certfile = cert
        config.keyfile = key
    async def _ping_alive():
        while alive():
            try:
                await asyncio.wait_for(asyncio.shield(shutdown.wait()), 1)
                break
            except TimeoutError:
                pass

    loop = asyncio.new_event_loop()
    logging.debug('hypercorn_main._run shutdown %s', shutdown)
    task = None
    if alive:
        task = loop.create_task(_ping_alive())
    loop.run_until_complete(serve(app, config, shutdown_trigger=shutdown.wait))
