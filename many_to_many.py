from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import json

import pandas as pd

Base = declarative_base()

association_table = Table('association', Base.metadata,
                          Column('parent_id', INTEGER, ForeignKey('parent.parent_id')),
                          Column('child_id', INTEGER, ForeignKey('child.child_id')))


class Parent(Base):
    __tablename__ = 'parent'
    parent_id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50))
    family = Column(VARCHAR(50))


class Child(Base):
    __tablename__ = 'child'
    child_id = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50))
    residence = Column(VARCHAR(50))
    parent_id = Column(INTEGER, ForeignKey("parent.parent_id"))

    parent = relationship("Parent", secondary=association_table, backref="children")


conn = "mysql+pymysql://saran:SADA2028jaya@localhost/students"
engine = create_engine(conn, echo=True)

Base.metadata.create_all(engine)

df = pd.read_excel("parent.xlsx")
df1 = pd.read_excel("child.xlsx")

with open("parent_child.json") as file:
    connection_data = json.load(file)

Session = sessionmaker(bind=engine)
session = Session()


for index, row in df.iterrows():
    p1 = Parent()
    p1.name = row['parent_name']
    p1.family = row['family']
    session.add(p1)

session.commit()

for index, row in df1.iterrows():
    c1 = Child()
    c1.name = row['child_name']
    c1.residence = row['Residence']
    father_obj = session.query(Parent).filter_by(name=row['father_name']).first()
    c1.parent.append(father_obj)
    mother_obj = session.query(Parent).filter_by(name=row['mother_name']).first()
    c1.parent.append(mother_obj)
    session.add(c1)

session.commit()
