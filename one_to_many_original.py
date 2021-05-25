# -- coding: UTF-8 --
"""
One to Many relationship explained
"""
import logging
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

import pandas as pd

from helpers import setup_logging

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Specifications(Base):
    __tablename__ = 'specifications'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model = Column(VARCHAR(10), unique=True)
    capacity_control = Column(VARCHAR(15))
    refrigerant = Column(VARCHAR(10))
    technology = Column(VARCHAR(15))


class Values(Base):
    __tablename__ = 'values'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model_id = Column(INTEGER, ForeignKey("specifications.id"))
    evap_temp = Column(INTEGER)
    cond_temp = Column(INTEGER)
    power = Column(FLOAT)

    specs = relationship("Specifications", backref="values", cascade='all, delete')
    # specs = relationship("Specifications", backref=backref("values", cascade='all, delete'))


def add_specs(session, df):

    try:
        for index, row in df.iterrows():
            c1 = Specifications()
            c1.model = row['Model']
            c1.capacity_control = row['Capacity_control']
            c1.refrigerant = row['Refrigerant']
            c1.technology = row['Technology']

            session.add(c1)
    except:
        session.rollback()
        raise
    else:
        session.commit()


def add_values(session, df):

    try:
        for index, row in df.iterrows():
            v1 = Values()

            v1.model_id = row['model']
            v1.power = row['power']
            v1.cond_temp = row['condenser_temp']
            v1.evap_temp = row['evaporator_temp']
            specs_object = session.query(Specifications).filter_by(model=v1.model_id).one()
            v1.specs = specs_object

            session.add(v1)
    except:
        session.rollback()
        raise
    else:
        session.commit()

    session.commit()


def main():
    # setup_logging()

    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost:3306/learning"
    engine = create_engine(conn, echo=True)

    with engine.connect() as conn:
        conn.execute("DROP TABLE learning.values")
        conn.execute("DROP TABLE learning.specifications")

    # Creating the tables in the DB
    Base.metadata.create_all(engine)

    df1 = pd.read_csv("compressor_models.csv")
    df = pd.read_excel("specs.xlsx")

    # Creating the session
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    # Adding values in the DB
    add_specs(session, df)
    add_values(session, df1)

    # If parent object(Specifications) is deleted, then all the children(Values) are also deleted
    '''obj = session.query(Specifications).filter_by(id=6).one()
    session.delete(obj)
    session.commit()'''

    obj = Specifications('changed', )


if __name__ == '__main__':
    main()
