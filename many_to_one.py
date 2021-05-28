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
    """
    Class to store the characters in database
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar character_name: The name of the character in the series
    :vartype character_name: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar cast_in: The series the character is in
    :vartype cast_in: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar actor_name: Name of the actor who plays the character
    :vartype actor_name: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'characters'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    character_name = Column(VARCHAR(30))
    cast_in = Column(INTEGER, ForeignKey("series.id"))
    actor_name = Column(VARCHAR(30))

    series = relationship("Series", backref="characters")


class Series(Base):
    """
    Class to store the series details
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar name: Name of the series
    :vartype name: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar genre: Genre of the series
    :vartype genre: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar imdb: IMDB rating of the series
    :vartype imdb: :class:`sqlalchemy.dialects.mysql.FLOAT`

    :ivar ott: The platform in which its streaming
    :vartype ott: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar start_date:
    :vartype start_date: :class:`sqlalchemy.dialects.mysql.DATETIME`
    """
    __tablename__ = 'series'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(30), unique=True)
    genre = Column(VARCHAR(30))
    imdb = Column(FLOAT)
    ott = Column(VARCHAR(30))
    start_date = Column(DATETIME)


def add_series(session, df):
    """
    To add series to database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
    try:
        for _, row in df.iterrows():
            series = Series()
            series.name = row['series']
            series.genre = row['genre']
            series.imdb = row['imdb']
            series.ott = row['OTT']
            series.start_date = row['start_date']

            session.add(series)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def add_chars(session, df):
    """
    To add characters to database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
    try:
        for _, row in df.iterrows():
            chars = Characters()
            chars.character_name = row['character_name']
            chars.cast_in = row['series_name']
            chars.actor_name = row['actor_name']
            series_obj = session.query(Series).filter_by(name=chars.cast_in).first()
            chars.series = series_obj

            session.add(chars)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def main():
    """Main function"""
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
