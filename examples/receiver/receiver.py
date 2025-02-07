# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Dict, List, Optional, Tuple
import logging
import json
from io import IOBase
import os
import time

import secrets
import copy

class Transaction:
    CHUNK_SIZE=1048576
    tx_id : str
    tx_json : dict
    tx_json_path : Optional[str] = None
    body_path : Optional[str] = None
    message_json : Optional[dict] = None
    message_json_path : Optional[str] = None

    blobs : Dict[str, IOBase]
    blob_paths = Dict[str, str]
    cancelled = False
    dir : str

    last_update : int

    def __init__(self, tx_id : str, tx_json,
                 dir : str):
        self.blobs = {}
        self.blob_paths = {}
        self.tx_id = tx_id
        logging.debug('Tx.init %s', tx_json)
        self.tx_json = copy.copy(tx_json)
        if 'smtp_meta' in self.tx_json:
            del self.tx_json['smtp_meta']
        self.tx_json['mail_response'] = {'code': 250, 'message': 'ok'}
        self.tx_json['rcpt_response'] = [{'code': 250, 'message': 'ok'}]
        self.dir = dir
        self.ping()

    def ping(self):
        self.last_update = time.monotonic()

    def get_json(self):
        json_out = copy.copy(self.tx_json)
        if 'mail_from' in json_out:
            json_out['mail_from'] = {}
        if 'rcpt_to' in json_out:
            json_out['rcpt_to'] = [{} for x in self.tx_json['rcpt_to']]
        if self.body_path:
            json_out['body'] = {}
        logging.debug('Tx.get %s', json_out)
        return json_out

    def _create_file(self, suffix : str, content='b'):
        path = self.dir + '/' + self.tx_id + '.' + suffix
        return open(path, mode='x' + content)

    def _check_blobs(self, part_json) -> bool:
        logging.debug('check_blobs %s', part_json)
        if 'parts' not in part_json:
            if 'blob_rest_id' not in part_json:
                return True
            blob_id = part_json['blob_rest_id']
            if blob_id.startswith('/'):
                return False  # this doesn't support reuse
            logging.debug('check_blobs %s', blob_id)
            if blob_id in self.blobs:
                return None  # bad
            file = self._create_file(blob_id)
            # a la send_message.py
            part_json['file_content'] = file.name
            self.blobs[blob_id] = file
            return True

        for part_i in part_json['parts']:
            if not self._check_blobs(part_i):
                return False
        return True


    def update_message_builder(self, message_json) -> Tuple[int, str]:
        assert not self.cancelled
        logging.debug('Tx.update_message_json %s', message_json)
        self.message_json = message_json
        if not self._check_blobs(self.message_json['parts']):
            return 400, 'bad blob in message builder json'
        with self._create_file('msg.json', 't') as f:
            json.dump(message_json, f)
            self.message_json_path = f.name

    # wsgi/flask-only
    def create_tx_body(self, stream : IOBase):
        assert not self.cancelled
        assert self.body_path is None

        with self._create_file('msg') as file:
            assert isinstance(file, IOBase)
            while b := stream.read(self.CHUNK_SIZE):
                file.write(b)
            self.body_path = file.name

        self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
        self.log()

    # asgi/fastapi-only
    async def create_tx_body_async(self, stream : AsyncGenerator[bytes, None]):
        assert not self.cancelled
        assert self.body_path is None

        with self._create_file('msg') as file:
            assert isinstance(file, IOBase)
            async for b in stream:
                file.write(b)
            self.body_path = file.name

        self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
        self.log()

    # wsgi/flask-only
    def put_blob(self, blob_id : str, stream : IOBase) -> int:
        assert not self.cancelled
        assert blob_id in self.blobs
        logging.debug('put_blob %s', blob_id)
        blob_file = self.blobs[blob_id]
        while b := stream.read(self.CHUNK_SIZE):
            blob_file.write(b)
        self.blob_paths[blob_id] = blob_file.name
        del self.blobs[blob_id]
        blob_file.close()
        return 200

    # asgi/fastapi-only
    async def put_blob_async(
            self, blob_id : str, stream : AsyncGenerator[bytes, None]) -> int:
        assert not self.cancelled
        assert blob_id in self.blobs
        logging.debug('put_blob %s', blob_id)
        blob_file = self.blobs[blob_id]
        async for b in stream:
            blob_file.write(b)
        self.blob_paths[blob_id] = blob_file.name
        del self.blobs[blob_id]
        blob_file.close()
        return 200

    def _file_size(self, path):
        with open(path, 'rb') as file:
            file.seek(-1, os.SEEK_END)
            return file.tell() + 1

    def log(self):
        logging.debug('received tx %s', self.tx_json)
        logging.debug('received parsed %s',
                      self.message_json if self.message_json else None)
        logging.debug('received body %d bytes', self._file_size(self.body_path))

        logging.debug('received blobs %s', self.blobs.keys())
        for blob_id, blob_path in self.blob_paths.items():
            logging.debug('received blob %s %d bytes',
                          blob_id, self._file_size(blob_path))

        # don't populate these internal fields in the json we send
        # back to the router
        output_json = copy.copy(self.tx_json)
        if self.message_json_path:
            output_json['message_builder_json_path'] = self.message_json_path
        if self.body_path:
            output_json['body_path'] = self.body_path

        with self._create_file('tx.json', 't') as f:
            json.dump(output_json, f)
            self.tx_json_path = f.name

    def cancel(self):
        if self.cancelled:
            return
        for blob_id,blob_file in self.blobs.items():
            blob_file.close()
        self.blobs = {}

        self.cancelled = True


class Receiver:
    transactions : dict[str, Transaction]
    dir : str
    gc_ttl : int
    gc_interval : int
    last_gc : int

    def __init__(self, dir = '/tmp',
                 gc_ttl = 300,
                 gc_interval = 30):
        self.transactions = {}
        self.dir = dir
        self.last_gc = time.monotonic()
        self.gc_ttl = gc_ttl
        self.gc_interval = gc_interval

    def _get_tx(self, tx_rest_id : str) -> Transaction:
        tx = self.transactions.get(tx_rest_id, None)
        assert tx is not None
        tx.ping()
        self._gc()
        return tx

    def _gc(self):
        now = time.monotonic()
        if (now - self.last_gc) <= self.gc_interval:
            return
        self.last_gc = now
        del_tx_id = []
        for tx_id, tx in self.transactions.items():
            age = now - tx.last_update
            if age > self.gc_ttl:
                tx.cancel()
                logging.debug('_gc %s %f', tx_id, age)
                del_tx_id.append(tx_id)
        for tx_id in del_tx_id:
            del self.transactions[tx_id]
        logging.debug('_gc %d live', len(self.transactions) - len(del_tx_id))

    def create_tx(self, tx_json) -> Tuple[str,dict]:
        tx_id = secrets.token_urlsafe()
        tx = Transaction(tx_id, tx_json, self.dir)
        self.transactions[tx_id] = tx
        return tx_id, tx.get_json()

    def get_tx(self, tx_rest_id : str):
        tx = self._get_tx(tx_rest_id)
        return tx.get_json()

    def update_tx_message_builder(self, tx_rest_id : str, message_json):
        tx = self._get_tx(tx_rest_id)
        return tx.update_message_builder(message_json)

    # wsgi/flask-only
    def create_tx_body(self,
                       tx_rest_id : str,
                       stream : IOBase):
        tx = self._get_tx(tx_rest_id)
        return tx.create_tx_body(stream)

    # asgi/fastapi-only
    def put_blob(self, tx_rest_id: str, blob_id : str, stream : IOBase) -> int:
        tx = self._get_tx(tx_rest_id)
        return tx.put_blob(blob_id, stream)

    # wsgi/flask-only
    async def create_tx_body_async(self,
                       tx_rest_id : str,
                       stream : AsyncGenerator[bytes, None]):
        tx = self._get_tx(tx_rest_id)
        return await tx.create_tx_body_async(stream)

    # asgi/fastapi-only
    async def put_blob_async(self, tx_rest_id: str, blob_id : str,
                             stream : AsyncGenerator[bytes, None]
                             ) -> int:
        tx = self._get_tx(tx_rest_id)
        return await tx.put_blob_async(blob_id, stream)

    def cancel_tx(self, tx_rest_id : str):
        tx = self._get_tx(tx_rest_id)
        tx.cancel()
