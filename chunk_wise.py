from multiprocessing import Pool
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


def process_line(line):
    session.add(TextBook(line))
    session.commit()


def main():
    pool = Pool(4)
    with open('gutenberg.txt', encoding='utf-8') as source_file:
        # chunk the work into batches of 4 lines at a time
        pool.map(process_line, source_file, 4)

    session.commit()


if __name__ == "__main__":
    main()

