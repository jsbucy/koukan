import logging
import psycopg
import psycopg.errors
import testing.postgresql

def setUpModule():
    global pg_factory
    pg_factory = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

def tearDownModule():
    global pg_factory
    pg_factory.clear_cache()

def postgres_url(unix_socket_dir, port, db):
    url = 'postgresql://postgres@/' + db + '?'
    url += ('host=' + unix_socket_dir)
    url += ('&port=%d' % port)
    return url

# mutates storage yaml
def setup_postgres(storage_yaml):
    global pg_factory
    pg = pg_factory()
    unix_socket_dir = pg.base_dir + '/tmp'
    port = pg.dsn()['port']
    storage_yaml['unix_socket_dir'] = unix_socket_dir
    storage_yaml['port'] = port
    storage_yaml['postgres_user'] = 'postgres'
    storage_yaml['postgres_db_name'] = 'storage_test'
    url = postgres_url(unix_socket_dir, port, 'postgres')
    logging.info('StorageTest setup_postgres %s', url)

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
        with open('init_storage_postgres.sql', 'r') as f:
            with conn.cursor() as cursor:
                cursor.execute(f.read())
    return pg
