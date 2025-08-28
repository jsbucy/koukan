# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, List, Optional, Tuple
import logging
from functools import partial
import asyncio
import logging

import uvicorn

class Server:
    loop : asyncio.AbstractEventLoop
    server : uvicorn.Server
    shutdown_event : asyncio.Event
    alive : Optional[Callable]
    serve_task : asyncio.Task

    def __init__(self, app : Callable,
                 bind : Tuple[str,int],
                 cert : Optional[str] = None, key : Optional[str] = None,
                 alive : Optional[Callable] = None):
        config = uvicorn.Config(
            host = bind[0],
            port = bind[1],
            app = app,
            timeout_graceful_shutdown = 0,
            workers = 0,
            log_config = {'version': 1})

        if cert and key:
            logging.debug('cert %s key %s', cert, key)
            config.ssl_certfile = cert
            config.ssl_keyfile = key

        self.server = uvicorn.Server(config)
        self.loop = asyncio.new_event_loop()
        self.serve_task = self.loop.create_task(self.server.serve())
        self.alive = alive
        self.shutdown_event = asyncio.Event()

    async def _stop(self):
        logging.debug('stop')
        await self.server.shutdown()
        self.serve_task.cancel()
        try:
            await self.serve_task
        except:
            pass
        self.loop.stop()

    def shutdown(self):
        logging.debug('shutdown %s %s', self.shutdown, self.loop)
        self.shutdown_event.set()
        self.loop.create_task(self._stop())

    async def _ping_alive(self):
        while self.alive():
            try:
                await asyncio.wait_for(
                    asyncio.shield(self.shutdown_event.wait()), 1)
                break
            except TimeoutError:
                pass

    def run(self):
        logging.debug('run')
        task = None
        if self.alive:
            task = self.loop.create_task(self._ping_alive())
        self.loop.run_forever()
