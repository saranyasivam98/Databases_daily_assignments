from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import json

import pandas as pd

Base = declarative_base()

association_table = Table('association', Base.metadata,
                          Column('parent_id', INTEGER, ForeignKey('parent.parent_id', ondelete="CASCADE")),
                          Column('child_id', INTEGER, ForeignKey('child.child_id', ondelete="CASCADE")))


class Parent(Base):
    __tablename__ = 'parent'
    parent_id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50))
    family = Column(VARCHAR(50))

    children = relationship(
        "Child",
        secondary=association_table,
        back_populates="parents",
        cascade="all, delete"
    )


class Child(Base):
    __tablename__ = 'child'
    child_id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50))
    residence = Column(VARCHAR(50))

    parents = relationship(
        "Parent",
        secondary=association_table,
        back_populates="children",
        passive_deletes=False
    )


def add_parent(session, df):
    try:
        for index, row in df.iterrows():
            p1 = Parent()
            p1.name = row['parent_name']
            p1.family = row['family']
            session.add(p1)
    except:
        session.rollback()
        raise
    else:
        session.commit()


def add_child(session, df):
    try:
        for index, row in df.iterrows():
            c1 = Child()
            c1.name = row['child_name']
            c1.residence = row['Residence']
            father_obj = session.query(Parent).filter_by(name=row['father_name']).first()
            c1.parents.append(father_obj)
            mother_obj = session.query(Parent).filter_by(name=row['mother_name']).first()
            c1.parents.append(mother_obj)
            session.add(c1)
    except:
        session.rollback()
        raise
    else:
        session.commit()


def main():
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

    obj = session.query(Parent).filter(Parent.parent_id == 1).one()
    session.delete(obj)
    session.commit()


if __name__ == '__main__':
    main()
