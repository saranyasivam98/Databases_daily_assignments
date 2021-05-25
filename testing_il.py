# -- coding: UTF-8 --
"""
Testing different isolation levels namely READ COMMITTED, REPEATABLE READ, SERIALIZABLE, READ UNCOMMITTED, AUTOCOMMIT
"""
import logging
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

import pandas as pd
import time
from helpers import setup_logging

Base = declarative_base()


class IsolationLevel(Base):
    __tablename__ = 'isolation_level'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    level = Column(VARCHAR(20))

    def __init__(self, level):
        self.level = level


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost:3306/learning"
    engine = create_engine(conn, echo=True, echo_pool='debug'
                           )# , execution_options={"isolation_level": "REPEATABLE READ"})

    '''with engine.connect() as conn:
        conn.execute("DROP TABLE learning.isolation_level")'''

    # Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    '''session.add(IsolationLevel("first_level"))
    # time.sleep(20)
    session.add(IsolationLevel("second_level"))
    session.add(IsolationLevel("third_level"))
    session.commit()'''
    # time.sleep(15)

    session.query(IsolationLevel).filter(IsolationLevel.id == 1).update({'level': "changed"})
    # session.add(IsolationLevel("update check"))
    session.commit()


if __name__ == '__main__':
    main()

# session.merge() for updating the values in the DB
# Designing the
# Normalisation Tools and test my hospital db
