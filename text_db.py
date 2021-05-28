# -*- coding: UTF-8 -*-
"""
Insert a book into database using threading
"""
import concurrent.futures
import threading
import time
import multiprocessing
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

thread_local = threading.local()

conn = "mysql+pymysql://saran:SADA2028jaya@127.0.0.1:3306/learning"
engine = create_engine(conn)


class TextBook(Base):
    """
    :ivar id: Primary Key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar line: Each line in the textbook
    :vartype line: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'book'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    line = Column(TEXT)

    def __init__(self, line):
        self.line = line


Base.metadata.create_all(engine)
session_factory = sessionmaker(bind=engine, autoflush=False)   # try with scoped session
session = session_factory()


def add_line(line):
    """

    :param line: Each line from textbook
    :type line: str
    :return: None
    """
    session.add(TextBook(line))


def main():
    """Main Function"""
    file = open('gutenberg.txt', encoding='utf-8')
    lines = file.readlines()

    for line in lines:
        if line == '\n':
            lines.remove(line)

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(add_line, lines)
    for line in lines:
        session.add(TextBook(line))

    with multiprocessing.Pool(processes=4) as multi_pool:
        multi_pool.map(add_line, lines)

    session.commit()

    duration = time.time() - start_time
    print(f"Saved %f in time %f seconds", len(lines), duration)


if __name__ == '__main__':
    main()
