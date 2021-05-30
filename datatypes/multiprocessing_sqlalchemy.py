# -*- coding: UTF-8 -*-
"""
Process Pool Demo
================================
This script is intended to demonstrate connection pooling of SQLAlchemy with the help of multiprocessing
"""

import logging
from concurrent.futures.thread import ThreadPoolExecutor
from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import StaticPool

from helpers import setup_logging

Base = declarative_base()

__author__ = "saranya@gyandata.com"

LOGGER = logging.getLogger(__name__)


class Specifications(Base):
    """
    To store the properties of compressor

    :ivar model: Model of the compressor
    :vartype model: :class:`marshmallow.fields.Str`

    :ivar technology: Technology of the motor
    :vartype technology: :class:`marshmallow.fields.Str`

    :ivar refrigerant: Refrigerant used in the compressor
    :vartype refrigerant: :class:`marshmallow.fields.Str`

    :ivar capacity_control: Speed control of the compressor
    :vartype capacity_control: :class:`marshmallow.fields.Str`
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


def process_id(pkey):
    """
    Returns a connectable after creating a transactional scope

    :return: An sqlalchemy connection
    :rtype: :class:`sqlalchemy.engine.Connection`
    """

    with engine.connect() as connection:
        connection.execute(f"SELECT * FROM learning.specifications WHERE id={pkey}")
        connection.execute("SELECT SLEEP(1)")

        LOGGER.info(engine.pool.status())


conn_string = get_db_conn_str(dialect="mysql",
                              driver="pymysql",
                              user="saran",
                              password="SADA2028jaya",
                              host="127.0.0.1",
                              port="3306",
                              schema="learning")

# checked out connection: used for a performing an operation
db_engine_options = {
    "echo": True,
    "echo_pool": 'debug',
    "pool_pre_ping": True,
    "pool_recycle": 3600,
    # "pool_size": 3,
    # "max_overflow": 1,
    "poolclass": StaticPool,
    "execution_options": {"isolation_level": "REPEATABLE READ"}
}

# Read the entire book, take everyline is one entry in db using multiprocessing.


engine = get_engine(conn_str=conn_string, engine_options=db_engine_options)

if __name__ == '__main__':
    setup_logging()

    specs_ids = [1, 2, 3, 4, 5, 6, 7, 8]

    with ThreadPoolExecutor(max_workers=8) as multi_pool:
        result = list(multi_pool.map(process_id, specs_ids))

    LOGGER.info("--------------------")
