# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Dict, List, Optional, Set, Tuple
import logging
import json
from io import IOBase, TextIOWrapper
import os
import time
from contextlib import nullcontext

from urllib.parse import urljoin

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

    # create_id (or auto-generated for inline) -> fs path
    # lifecycle: update_message_builder() inserts placeholder None
    # put_blob() writes and replaces with actual filename
    blob_paths : Dict[str, Optional[str]]
    cancelled = False
    dir : str

    last_update : int
    version = 1
    base_url : str

    def __init__(self, tx_id : str, tx_json,
                 dir : str,
                 base_url):
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
        self.next_inline = 0
        self.base_url = base_url

    def ping(self):
        self.last_update = int(time.monotonic())

    def get_json(self):
        json_out = copy.copy(self.tx_json)
        if 'mail_from' in json_out:
            json_out['mail_from'] = {}
        if 'rcpt_to' in json_out:
            json_out['rcpt_to'] = [{} for x in self.tx_json['rcpt_to']]

        json_out['body'] = {}
        json_out['body']['blob_status'] = {
            'uri': urljoin(self.base_url, '/transactions/' + self.tx_id + '/body'),
            'finalized': self.body_path is not None
        }

        message_builder = json_out['body']['message_builder'] = {}
        if self.message_json is None:
            message_builder['uri'] = urljoin(self.base_url, '/transactions/' + self.tx_id + '/message_builder')
        else:
            blob_status = {}
            for bid,bp in self.blob_paths.items():
                blob_status[bid] = {
                    'uri': urljoin(
                        self.base_url,
                        '/transactions/' + self.tx_id + '/blob/' + bid),
                    'finalized': bp is not None
                }
            message_builder['blob_status'] = blob_status

        logging.debug('Tx.get %s', json_out)
        return json_out

    def _create_file(self, suffix : str, content='b'):
        path = self.dir + '/' + self.tx_id + '.' + suffix
        return open(path, mode='x' + content)

    # walks mime part tree
    # creates/writes files for inline
    # inserts placeholder values for create_id in blob_paths
    def _check_blobs(self, part_json) -> bool:
        logging.debug('check_blobs %s', part_json)
        if 'parts' in part_json:
            for part_i in part_json['parts']:
                if not self._check_blobs(part_i):
                    return False
            return True

        content = part_json['content']
        if inline := content.get('inline', None):
            blob_id = 'inline%d' % self.next_inline
            self.next_inline += 1
            with self._create_file(blob_id) as file:
                file.write(inline.encode('utf-8'))
                self.blob_paths[blob_id] = file.name
                del content['inline']
                content['filename'] = file.name

        elif blob_id := content.get('create_id', None):
            if blob_id in self.blob_paths:
                # this is now expected as the same id will usually
                # appear in both the original mime tree and the
                # simplified "text_body" fields
                return True
            self.blob_paths[blob_id] = None
        else:
            return False  # this doesn't support reuse

        logging.debug('check_blobs %s', blob_id)

        return True

    # walks mime part tree
    # removes create_id and replaces with filename from blob_paths
    def _finalize_blobrefs(self, part_json, blob_id, filename):
        logging.debug(blob_id)
        logging.debug(json.dumps(part_json, indent=2))
        if 'parts' in part_json:
            for part_i in part_json['parts']:
                if not self._finalize_blobrefs(part_i, blob_id, filename):
                    return False
            return True

        content = part_json['content']
        if (bid := content.get('create_id', None)) is None or (bid != blob_id):
            logging.debug('%s %s', bid, blob_id)
            return True
        del content['create_id']
        content['filename'] = filename
        logging.debug(content)
        return True

    def update_message_builder(self, message_json) -> Optional[Tuple[int, str]]:
        assert not self.cancelled
        logging.debug('Tx.update_message_json %s', message_json)
        self.message_json = message_json
        if not self._check_blobs(self.message_json['parts']):
            return 400, 'bad blob in message builder json'
        for p in ["text_body", "related_attachments", "file_attachments"]:
            if parts := message_json.get(p, None):
                for part in parts:
                    if not self._check_blobs(part):
                        return 400, 'bad blob in message builder json'

        with self._create_file('msg.json', 't') as f:
            assert isinstance(f, TextIOWrapper)
            json.dump(message_json, f)
            self.message_json_path = f.name
        self.version += 1
        return None

    def _validate_put_req(self, req_headers,
                          tx_body : Optional[bool] = None,
                          blob_id : Optional[str] = None,
                          ) -> Optional[Tuple[int, str]]:
        if self.cancelled:
            return 400, 'transaction cancelled'
        if tx_body and self.body_path is not None:
            return 400, 'body already exists'
        if blob_id is not None:
            if blob_id not in self.blob_paths:
                return 404, 'blob not found'
            elif self.blob_paths[blob_id] is not None:
                return 400, 'blob already exists'
        if 'content-range' in req_headers:
            return 400, 'receiver does not accept chunked PUT'
        return None

    def _finalize_blob(self, filename,
                       tx_body : Optional[bool] = None,
                       blob_id : Optional[str] = None):
        logging.debug('%s %s %s', filename, tx_body, blob_id)
        if blob_id:
            self.blob_paths[blob_id] = filename
            assert self.message_json is not None
            self._finalize_blobrefs(
                self.message_json['parts'], blob_id, filename)
            for p in ["text_body", "related_attachments", "file_attachments"]:
                if parts := self.message_json.get(p, None):
                    for part in parts:
                        self._finalize_blobrefs(part, blob_id, filename)

            logging.debug(self.message_json)
        if tx_body:
            self.body_path = filename
            self.tx_json['data_response'] = {'code': 250, 'message': 'ok'}
            self.log()

    # wsgi/flask-only
    def put_blob(self, req_headers, stream : IOBase,
                 tx_body : Optional[bool] = None,
                 blob_id : Optional[str] = None,
                 ) -> Tuple[int, str]:
        if err := self._validate_put_req(req_headers, tx_body, blob_id):
            return err
        logging.debug('put_blob %s', blob_id)
        dname = 'msg' if tx_body else blob_id
        assert dname is not None
        with self._create_file(dname) as file:
            while b := stream.read(self.CHUNK_SIZE):
                file.write(b)
            self._finalize_blob(file.name, tx_body, blob_id)
        self.version += 1
        return 200, 'ok'

    # asgi/fastapi-only
    async def put_blob_async(
            self, req_headers,
            stream : AsyncGenerator[bytes, None],
            tx_body : Optional[bool] = None,
            blob_id : Optional[str] = None
    ) -> Tuple[int, str]:
        if err := self._validate_put_req(req_headers, tx_body, blob_id):
            return err
        logging.debug('put_blob %s', blob_id)
        dname = 'msg' if tx_body else blob_id
        assert dname is not None
        with self._create_file(dname) as file:
            async for b in stream:
                file.write(b)
            self._finalize_blob(file.name, tx_body, blob_id)
        return 200, 'ok'

    def _file_size(self, path):
        with open(path, 'rb') as file:
            file.seek(0, os.SEEK_END)
            return file.tell()

    def log(self):
        logging.debug('received tx %s', self.tx_json)
        logging.debug('received parsed %s',
                      self.message_json if self.message_json else None)
        logging.debug('received body %d bytes', self._file_size(self.body_path))

        logging.debug('received blobs %s', self.blob_paths.keys())
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
            assert isinstance(f, TextIOWrapper)
            json.dump(output_json, f)
            self.tx_json_path = f.name
            logging.debug(self.tx_json_path)

    def cancel(self):
        if self.cancelled:
            return
        self.cancelled = True

class Receiver:
    transactions : dict[str, Transaction]
    dir : str
    gc_ttl : int
    gc_interval : int
    last_gc : int
    base_url = None

    def __init__(self, dir = '/tmp',
                 gc_ttl = 300,
                 gc_interval = 30,
                 base_url = None):
        self.transactions = {}
        self.dir = dir
        self.last_gc = int(time.monotonic())
        self.gc_ttl = gc_ttl
        self.gc_interval = gc_interval
        self.base_url = base_url

    def _get_tx(self, tx_rest_id : str) -> Transaction:
        tx = self.transactions.get(tx_rest_id, None)
        assert tx is not None
        tx.ping()
        self._gc()
        return tx

    def _gc(self):
        now = int(time.monotonic())
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

    def create_tx(self, tx_json) -> Tuple[str,dict,str]:
        tx_id = secrets.token_urlsafe()
        tx = Transaction(tx_id, tx_json, self.dir, self.base_url)
        self.transactions[tx_id] = tx
        return tx_id, tx.get_json(), str(tx.version)

    def get_tx(self, tx_rest_id : str):
        tx = self._get_tx(tx_rest_id)
        return tx.get_json(), str(tx.version)

    def update_tx_message_builder(self, tx_rest_id : str, message_json):
        tx = self._get_tx(tx_rest_id)
        err = tx.update_message_builder(message_json)
        if err:
            return err, None
        return None, self.get_tx(tx_rest_id)

    # asgi/fastapi-only
    def put_blob(self, tx_rest_id: str, req_headers,
                 stream : IOBase,
                 tx_body : Optional[bool] = None,
                 blob_id : Optional[str] = None
                 ) -> Tuple[int, str]:
        tx = self._get_tx(tx_rest_id)
        return tx.put_blob(
            req_headers, stream, tx_body=tx_body, blob_id=blob_id)

    # asgi/fastapi-only
    async def put_blob_async(self, tx_rest_id: str,
                             req_headers,
                             stream : AsyncGenerator[bytes, None],
                             tx_body : Optional[bool] = None,
                             blob_id : Optional[str] = None
                             ) -> Tuple[int, str]:
        tx = self._get_tx(tx_rest_id)
        return await tx.put_blob_async(
            req_headers, stream, tx_body=tx_body, blob_id=blob_id)

    def cancel_tx(self, tx_rest_id : str):
        tx = self._get_tx(tx_rest_id)
        tx.cancel()
