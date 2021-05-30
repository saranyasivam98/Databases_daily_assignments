# -- coding: UTF-8 --
"""
Performance of adding objects into database using three different methods.
"""
import logging
from sqlalchemy import Column, create_engine
from sqlalchemy.dialects.mysql import INTEGER, FLOAT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from helpers import timer

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Values(Base):
    """
    To store the compressor values
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar model: Model of the compressor
    :vartype model: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar condenser_temp: Inlet temperature to condenser
    :vartype condenser_temp: :class:`sqlalchemy.dialects.mysql.FLOAT`

    :ivar evaporator_temp: Outlet temperature to condenser
    :vartype evaporator_temp: :class:`sqlalchemy.dialects.mysql.FLOAT`

    :ivar power: Power required for the compressor
    :vartype power: :class:`sqlalchemy.dialects.mysql.FLOAT`

    """
    __tablename__ = 'values'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model_id = Column(INTEGER)
    evap_temp = Column(INTEGER)
    cond_temp = Column(INTEGER)
    power = Column(FLOAT)

    def __init__(self, model, temp_e, temp_c, power):
        self.model_id = model
        self.evap_temp = temp_e
        self.cond_temp = temp_c
        self.power = power


@timer
def add_one_at_a_time(session, df):
    """
    To demonstrate the performance of adding one object at a time

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """

    try:
        for _, row in df.iterrows():
            values = Values(row['model'], row['condenser_temp'], row['evaporator_temp'], row['power'])
            session.add(values)
            session.commit()
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


@timer
def add_all(session, df):
    """
    To demonstrate the performance of adding all objects at a time using add_all

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
    obj_list = []
    try:
        for _, row in df.iterrows():
            values = Values(row['model'], row['condenser_temp'], row['evaporator_temp'], row['power'])
            obj_list.append(values)

        session.add_all(obj_list)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


@timer
def add_bulk(session, df):
    """
    To demonstrate the performance of adding all objects at a time using bulk_save_objects

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """

    obj_list = []
    try:
        for _, row in df.iterrows():
            obj_list.append(Values(row['model'], row['condenser_temp'], row['evaporator_temp'], row['power']))

        session.bulk_save_objects(obj_list)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def main():
    """ Main function """
    # Creating an engine
    conn = "mysql+pymysql://saran:SADA2028jaya@127.0.0.1:3306/learning"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    # Creating a session
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    df = pd.read_csv("compressor_models - Copy.csv")

    # Adding each object one by one to the session
    add_one_at_a_time(session, df)

    # Adding all the objects at once
    add_all(session, df)

    # Adding all the objects at once
    add_bulk(session, df)


if __name__ == '__main__':
    main()
