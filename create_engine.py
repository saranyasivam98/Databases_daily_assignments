# -- coding: UTF-8 --
"""
==============
Create engine
==============
Exploring different options in create_engine()
"""
import logging
from sqlalchemy import Column, create_engine, text
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Specifications(Base):
    """
    To store the properties of compressor

    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar model: Model of the compressor
    :vartype model: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar technology: Technology of the motor
    :vartype technology: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar refrigerant: Refrigerant used in the compressor
    :vartype refrigerant: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar capacity_control: Speed control of the compressor
    :vartype capacity_control: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'specifications'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model = Column(VARCHAR(10), unique=True)
    capacity_control = Column(VARCHAR(15))
    refrigerant = Column(VARCHAR(10))
    technology = Column(VARCHAR(15))

    def __init__(self, model, cap, ref, tech):
        self.model = model
        self.technology = tech
        self.refrigerant = ref
        self.capacity_control = cap


def get_engine(conn_str, engine_options):
    """
    Method to get the SQLAlchemy engine with the given connection string and engine options

    :param conn_str: SQLAlchemy connection string
    :type conn_str: str

    :param engine_options: The engine options from SQLAlchemy create_engine API :func:`~sqlalchemy.create_engine`
    :type engine_options: dict

    :return: An sqlalchemy engine
    :rtype: :class:`sqlalchemy.engine.Engine`
    """

    db_engine = create_engine(conn_str,
                              **engine_options)

    return db_engine


def get_db_conn_str(dialect, driver, user, password, host, port, schema):
    """
    Get the database connection string as understood by sqlalchemy

    :param dialect: The database dialect i.e. mysql, postgresql etc.
    :type dialect: str

    :param driver: The database driver i.e. pymysql, psycopg2 etc.
    :type driver: str

    :param user: The database user
    :type user: str

    :param password: The database password
    :type password: str

    :param host: The database host
    :type host: str

    :param port: The database port
    :type port: str

    :param schema: The database schema
    :type schema: str

    :return: The connection string
    :rtype: str
    """
    conn_str = ""
    conn_str += dialect
    conn_str += "+"
    conn_str += driver
    conn_str += "://"
    conn_str += user
    conn_str += ":"
    conn_str += password
    conn_str += "@"
    conn_str += host
    conn_str += ":"
    conn_str += port
    conn_str += "/"
    conn_str += schema

    return conn_str


def add_specs(session):
    """
    Add specifications table to database
    :param session:  An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_excel("specs.xlsx")

    try:
        for _, row in df.iterrows():
            specs = Specifications(row['Model'], row['Technology'], row['Refrigerant'], row['Capacity_control'])
            session.add(specs)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def main():
    """ Main function"""
    conn_string = get_db_conn_str(dialect="mysql", driver="pymysql", user="saran", password="SADA2028jaya",
                                  host="127.0.0.1", port="3306", schema="learning")

    db_engine_options = {
        # "case_sensitive": False,
        "echo": True,
        "echo_pool": 'debug',
        "execution_options": {"isolation_level": "REPEATABLE READ"},
        "hide_parameters": True,
        "logging_name": "demon",
        # "max_identifier_length": 10,
        "pool_pre_ping": True,
        "pool_recycle": 3600,
        "pool_reset_on_return": "rollback",    # read again
        # "pool_size": 4,                       # demonstrated in other code
        # "paramstyle": "format",             # %s for format and %(name)s for py format
        # "max_overflow": 0
        }

    engine = get_engine(conn_str=conn_string, engine_options=db_engine_options)

    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    add_specs(session)

    # Case sensitive
    with engine.connect() as connection:
        results = connection.execute(text("SELECT Refrigerant FROM learning.specifications"))

    print(results)

    # Hide parameters
    session.add(Specifications("one_u", "two", "three", "four"))
    session.commit()


if __name__ == '__main__':
    main()
