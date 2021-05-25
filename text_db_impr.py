# -*- coding: UTF-8 -*-

import concurrent.futures
import threading
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
import multiprocessing

Base = declarative_base()

thread_local = threading.local()

conn = "mysql+pymysql://saran:SADA2028jaya@127.0.0.1:3306/learning"
engine = create_engine(conn, echo=True)


class TextBook(Base):
    __tablename__ = 'book'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    line = Column(TEXT)

    def __init__(self, line):
        self.line = line


Base.metadata.create_all(engine)
session_factory = sessionmaker(bind=engine, autoflush=False)
session = session_factory()


def add_chunk(chunk):
    session.add(TextBook(chunk))


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def main():
    start_time = time.time()

    with open('gutenberg.txt', encoding='utf-8') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(add_chunk, read_in_chunks(f))

    session.commit()

    duration = time.time() - start_time
    print(f"Saved in time {duration} seconds")


if __name__ == '__main__':
    main()
