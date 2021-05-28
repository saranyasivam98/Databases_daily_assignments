# -- coding: UTF-8 --
"""
Code that illustrates the use of TEXT and JSON datatype
"""

import logging
import json
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYTEXT, TEXT, JSON
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, deferred

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Paper(Base):
    """
    Class to store the details of a paper
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar paper_id: Id of the paper in the publication
    :vartype paper_id: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar submitter: Submitter of the paper
    :vartype submitter: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar authors: Author of the paper
    :vartype authors: :class:`sqlalchemy.dialects.mysql.TINYTEXT`

    :ivar title: Title of the paper
    :vartype title: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar abstract: The abstract of the paper
    :vartype abstract: :class:`sqlalchemy.dialects.mysql.TEXT`

    :ivar license_paper: License of the paper
    :vartype license_paper: :class:`sqlalchemy.dialects.mysql.TINYTEXT`

    :ivar versions: Versions of the paper
    :vartype versions: :class:`sqlalchemy.dialects.mysql.JSON`
    """
    __tablename__ = 'papers'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    paper_id = Column(VARCHAR(15), index=True)
    submitter = Column(VARCHAR(50))
    authors = Column(TINYTEXT)
    title = Column(VARCHAR(250), index=True)
    abstract = deferred(Column(TEXT))
    license_paper = Column(TINYTEXT)
    versions = Column(JSON)


def main():
    """ Main Function"""
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    # Creating the session
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    with open("ten.json") as file:
        papers = json.load(file)

    for paper in papers:
        obj = Paper()
        obj.paper_id = paper['id']
        obj.title = paper['title']
        obj.submitter = paper['submitter']
        obj.authors = paper['authors']
        obj.abstract = paper['abstract']
        obj.license_paper = paper['license']
        obj.versions = dict(versions=paper['versions'])

        session.add(obj)

    session.commit()

    for data in session.query(Paper).filter_by(id=2).all():
        print(data.__dict__)
        print(data.abstract)


if __name__ == '__main__':
    main()
