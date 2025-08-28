# Copyright The Koukan Authors
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Optional, Tuple
import logging
import psycopg
import psycopg.errors
import testing.postgresql

pg_factory : Optional[Callable] = None

def setUpModule():
    global pg_factory
    pg_factory = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

def tearDownModule():
    global pg_factory
    pg_factory.clear_cache()

def postgres_url(unix_socket_dir, port, db, engine='postgresql'):
    # SA URL can't seem to create this form of url with no host or
    # password for peer auth to unix socket
    url = engine + '://postgres@/' + db + '?'
    url += ('host=' + unix_socket_dir)
    url += ('&port=%d' % port)
    return url

def setup_postgres() -> Tuple[object, str]:
    global pg_factory
    assert pg_factory is not None
    pg = pg_factory()
    unix_socket_dir = pg.base_dir + '/tmp'
    port = pg.dsn()['port']
    url = postgres_url(unix_socket_dir, port, 'postgres')
    logging.info('postgres_test_utils.setup_postgres %s', url)

    with psycopg.connect(url) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            try:
                cursor.execute('drop database storage_test;')
            except psycopg.errors.InvalidCatalogName:
                pass
            cursor.execute('create database storage_test;')
            conn.commit()

    url = postgres_url(unix_socket_dir, port, 'storage_test')
    with psycopg.connect(url) as conn:
        with open('koukan/init_storage_postgres.sql', 'r') as f:
            with conn.cursor() as cursor:
                cursor.execute(f.read())
    return pg, postgres_url(unix_socket_dir, port, 'storage_test',
                            engine='postgresql+psycopg')
