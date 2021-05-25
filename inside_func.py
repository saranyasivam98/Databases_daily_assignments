# -*- coding: UTF-8 -*-

import concurrent.futures
import threading
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
from multiprocessing import Process

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


def add_to_db(file_name, session):
    for line in read_in_chunks(file_name):
        session.add()


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def main():

    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    start_time = time.time()

    p1 = Process(target=add_to_db, args=('gutenberg.txt',))

    p1.start()
    p1.join()


    '''with multiprocessing.Pool(processes=4) as multi_pool:
        multi_pool.map(add_line, lines)'''

    session.commit()

    duration = time.time() - start_time
    print(f"Saved in time {duration} seconds")

# File I/O operation should be inside the function.


if __name__ == '__main__':
    main()
