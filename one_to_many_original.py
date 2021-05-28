# -- coding: UTF-8 --
"""
============================
One to Many relationship
============================
One to Many relationship explained
"""
import logging
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Specifications(Base):
    """
    Class to store the specifications details
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar model: The model of the compressor
    :vartype model: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar capacity_control: Control of the compressor
    :vartype capacity_control: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar refrigerant: Refrigerant used.
    :vartype refrigerant: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar technology: The motor technology inside the compressor
    :vartype technology: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'specifications'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model = Column(VARCHAR(10), unique=True)
    capacity_control = Column(VARCHAR(15))
    refrigerant = Column(VARCHAR(10))
    technology = Column(VARCHAR(15))


class Values(Base):
    """
    Class to store the values of each model.

    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar model_id: The model of the compressor
    :vartype model_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar evap_temp:Evaporator Temperature
    :vartype evap_temp: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar cond_temp: Condenser Temperature
    :vartype cond_temp: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar power: The power input for the compressor
    :vartype power: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'values'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model_id = Column(INTEGER, ForeignKey("specifications.id"))
    evap_temp = Column(INTEGER)
    cond_temp = Column(INTEGER)
    power = Column(FLOAT)

    specs = relationship("Specifications", backref=backref("values", cascade='all, delete'))


def add_specs(session, df):
    """
    To add specifications into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
    try:
        for _, row in df.iterrows():
            specs = Specifications()
            specs.model = row['Model']
            specs.capacity_control = row['Capacity_control']
            specs.refrigerant = row['Refrigerant']
            specs.technology = row['Technology']

            session.add(specs)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def add_values(session, df):
    """
    To add values into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
    try:
        for _, row in df.iterrows():
            values = Values()

            values.model_id = row['model']
            values.power = row['power']
            values.cond_temp = row['condenser_temp']
            values.evap_temp = row['evaporator_temp']
            specs_object = session.query(Specifications).filter_by(model=values.model_id).one()
            values.specs = specs_object

            session.add(values)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def main():
    """Main Function"""
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
    obj = session.query(Specifications).filter_by(id=1).one()
    session.delete(obj)
    session.commit()


if __name__ == '__main__':
    main()
