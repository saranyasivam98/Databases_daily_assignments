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
engine = create_engine(conn)


class TextBook(Base):
    __tablename__ = 'book'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    line = Column(TEXT)
    # average word length
    # statistics of the line
    # max repeating words

    def __init__(self, line):
        self.line = line


Base.metadata.create_all(engine)
session_factory = sessionmaker(bind=engine, autoflush=False)   # try with scoped session
session = session_factory()


def add_line(line):
    session.add(TextBook(line))


def main():
    # Not ideal to load everything into memory
    f = open('gutenberg.txt', encoding='utf-8')
    lines = f.readlines()

    for line in lines:
        if line == '\n':
            lines.remove(line)

    start_time = time.time()
    '''with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(add_line, lines)
    for line in lines:
        session.add(TextBook(line))'''

    with multiprocessing.Pool(processes=4) as multi_pool:
        multi_pool.map(add_line, lines)         # What map is doing to the input

    session.commit()

    duration = time.time() - start_time
    print(f"Saved {len(lines)} in time {duration} seconds")

# File I/O operation should be inside the function.


if __name__ == '__main__':
    main()
