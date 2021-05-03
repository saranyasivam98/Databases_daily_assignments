# -- coding: UTF-8 --
"""
Many to One relationship explained
"""

import logging
from sqlalchemy import Table, Column, ForeignKey
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


class Series(Base):
    __tablename__ = 'series'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(30), primary_key=True, unique=True)
    genre = Column(VARCHAR(30))
    imdb = Column(FLOAT)
    ott = Column(VARCHAR(30))
    start_date = Column(DATETIME)


class Characters(Base):
    __tablename__ = 'characters'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    character_name = Column(VARCHAR(30))
    cast_in = Column(INTEGER, ForeignKey("series.id"))
    actor_name = Column(VARCHAR(30))

    series = relationship("Series", backref="characters")


conn = "mysql+pymysql://saran:SADA2028jaya@localhost/students"
engine = create_engine(conn, echo=True)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

df = pd.read_excel("series.xlsx")

for index, row in df.iterrows():
    s1 = Series()
    s1.name = row['series']
    s1.genre = row['genre']
    s1.imdb = row['imdb']
    s1.ott = row['OTT']
    s1.start_date = row['start_date']
    session.add(s1)

session.commit()

df1 = pd.read_excel("characters.xlsx")

for index, row in df1.iterrows():
    c1 = Characters()
    c1.character_name = row['character_name']
    c1.cast_in = row['series_name']
    c1.actor_name = row['actor_name']
    series_obj = session.query(Series).filter_by(name=c1.cast_in).first()
    c1.series = series_obj
    session.add(c1)


session.commit()
