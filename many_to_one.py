# -- coding: UTF-8 --
"""
Many to One relationship explained
"""

import logging
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, VARCHAR, FLOAT
from sqlalchemy.orm import relationship

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Characters(Base):
    __tablename__ = 'characters'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    character_name = Column(VARCHAR(30))
    cast_in = Column(INTEGER, ForeignKey("series.id"))
    actor_name = Column(VARCHAR(30))

    series = relationship("Series", backref="characters")


class Series(Base):
    __tablename__ = 'series'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(30), unique=True)
    genre = Column(VARCHAR(30))
    imdb = Column(FLOAT)
    ott = Column(VARCHAR(30))
    start_date = Column(DATETIME)


def add_series(session, df):
    try:
        for index, row in df.iterrows():
            c1 = Series()
            c1.name = row['series']
            c1.genre = row['genre']
            c1.imdb = row['imdb']
            c1.ott = row['OTT']
            c1.start_date = row['start_date']

            session.add(c1)
    except:
        session.rollback()
        raise
    else:
        session.commit()


def add_chars(session, df):
    try:
        for index, row in df.iterrows():
            c1 = Characters()
            c1.character_name = row['character_name']
            c1.cast_in = row['series_name']
            c1.actor_name = row['actor_name']
            series_obj = session.query(Series).filter_by(name=c1.cast_in).first()
            c1.series = series_obj

            session.add(c1)
    except:
        session.rollback()
        raise
    else:
        session.commit()


def main():
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    # Creating the tables in the DB
    Base.metadata.create_all(engine)

    # Creating the session
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # Adding the values to DB
    df = pd.read_excel("series.xlsx")
    add_series(session, df)

    df1 = pd.read_excel("characters.xlsx")
    add_chars(session, df1)


if __name__ == '__main__':
    main()
