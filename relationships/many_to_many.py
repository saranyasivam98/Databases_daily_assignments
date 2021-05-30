# -- coding: UTF-8 --
"""
==========================
Many to many relationship
==========================
Demonstrate many to many relationships using parent-child example
"""

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()

association_table = Table('association', Base.metadata,
                          Column('parent_id', INTEGER, ForeignKey('parent.parent_id')),
                          Column('child_id', INTEGER, ForeignKey('child.child_id')))


class Parent(Base):
    """
    To store details of parent

    :ivar parent_id: Primary key of the table
    :vartype parent_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar name: Nam eof the parent
    :vartype name: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar family: Name of the family
    :vartype family: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'parent'
    parent_id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50))
    family = Column(VARCHAR(50))


class Child(Base):
    """
    To store details of child

    :ivar child_id: Primary key of the table
    :vartype child_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar name: Nam eof the parent
    :vartype name: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar residence: Name of the residence
    :vartype residence: :class:`sqlalchemy.dialects.mysql.VARCHAR`
    """
    __tablename__ = 'child'
    child_id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50))
    residence = Column(VARCHAR(50))

    parent = relationship("Parent", secondary=association_table, backref=backref("children", cascade='all, delete'))


def add_parent(session):
    """
    To print the name of the staff who sold the maximum no of products.

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`
    :return: None
    """
    df = pd.read_excel("parent.xlsx")
    try:
        for _, row in df.iterrows():
            parent = Parent()
            parent.name = row['parent_name']
            parent.family = row['family']
            session.add(parent)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def add_child(session):
    """
    To print the name of the staff who sold the maximum no of products.

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`
    :return: None
    """
    df = pd.read_excel("child.xlsx")
    try:
        for _, row in df.iterrows():
            child = Child()
            child.name = row['child_name']
            child.residence = row['Residence']
            father_obj = session.query(Parent).filter_by(name=row['father_name']).first()
            child.parent.append(father_obj)
            mother_obj = session.query(Parent).filter_by(name=row['mother_name']).first()
            child.parent.append(mother_obj)
            session.add(child)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def main():
    """ Main function"""
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    with engine.connect() as conn:
        conn.execute("DROP TABLE learning.association")
        conn.execute("DROP TABLE learning.child")
        conn.execute("DROP TABLE learning.parent")

    # Creating the tables in the DB
    Base.metadata.create_all(engine)

    # Creating the session
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # Adding the values to DB
    add_parent(session)
    add_child(session)

    obj = session.query(Parent).filter(Parent.parent_id == 1).one()
    session.delete(obj)
    session.commit()


if __name__ == '__main__':
    main()
