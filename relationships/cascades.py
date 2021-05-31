# -- coding: UTF-8 --
"""
========================================
Deletion in many to many relationships
========================================
Explaining many to many relationships
"""
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()

association_table = Table('association', Base.metadata,
                          Column('parent_id', INTEGER, ForeignKey('parent.parent_id', ondelete="CASCADE")),
                          Column('child_id', INTEGER, ForeignKey('child.child_id', ondelete="CASCADE")))


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

    children = relationship(
        "Child",
        secondary=association_table,
        back_populates="parents",
        cascade="all, delete",
        passive_deletes=True
    )


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

    parents = relationship(
        "Parent",
        secondary=association_table,
        back_populates="children"
        # passive_deletes=True
    )


def add_parent(session, df):
    """
    To add parents to database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
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


def add_child(session, df):
    """
    To add child to database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :param df: Dataframe containing the values to be stored in the database
    :type df: :class:`pandas.DataFrame`

    :return: None
    """
    try:
        for _, row in df.iterrows():
            child = Child()
            child.name = row['child_name']
            child.residence = row['Residence']
            father_obj = session.query(Parent).filter_by(name=row['father_name']).first()
            child.parents.append(father_obj)
            mother_obj = session.query(Parent).filter_by(name=row['mother_name']).first()
            child.parents.append(mother_obj)
            session.add(child)
    except Exception as ex:
        session.rollback()
        raise ex
    else:
        session.commit()


def main():
    """Main Function"""
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    with engine.connect() as conn:
        conn.execute("DROP TABLE learning.association")
        conn.execute("DROP TABLE learning.child")
        conn.execute("DROP TABLE learning.parent")

    # Creating the tables in the DB
    Base.metadata.create_all(engine)

    df = pd.read_excel("parent.xlsx")
    df1 = pd.read_excel("child.xlsx")

    # Creating the session
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # Adding the values to DB
    add_parent(session, df)
    add_child(session, df1)

    obj = session.query(Child).filter(Child.child_id == 1).one()
    session.delete(obj)
    session.commit()


if __name__ == '__main__':
    main()
