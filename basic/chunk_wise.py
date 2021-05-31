# -- coding: UTF-8 --
"""
==================
Adding chunk wise
==================
Adding data into database chunk wise
"""

from multiprocessing import Pool
import threading
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

thread_local = threading.local()

connection = "mysql+pymysql://saran:SADA2028jaya@127.0.0.1:3306/learning"
engine = create_engine(connection, echo=True)


class TextBook(Base):
    """
    Class for storing the textbook in database
    :ivar id: Primary Key
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar line: A line in textbook
    :vartype line: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'book'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    line = Column(TEXT)

    def __init__(self, line):
        self.line = line


Base.metadata.create_all(engine)
session_factory = sessionmaker(bind=engine, autoflush=False)
session = session_factory()


def process_line(line):
    """
    To add the line to database
    :param line: The line from the text document
    :type: str
    :return: None
    """
    session.add(TextBook(line))
    session.commit()


def main():
    """Main Function"""
    pool = Pool(4)
    with open('gutenberg.txt', encoding='utf-8') as source_file:
        # chunk the work into batches of 4 lines at a time
        pool.map(process_line, source_file, 4)

    session.commit()


if __name__ == "__main__":
    main()
