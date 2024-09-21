from typing import Tuple
from tempfile import TemporaryDirectory
import sqlite3

def create_temp_sqlite_for_test() -> Tuple[TemporaryDirectory, str]:
    dir = TemporaryDirectory()
    filename = dir.name + 'db'
    conn = sqlite3.connect(filename)
    with open("koukan/init_storage.sql", "r") as f:
        conn.executescript(f.read())
    return dir, 'sqlite+pysqlite:///' + filename
