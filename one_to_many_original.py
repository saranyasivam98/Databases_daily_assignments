from sqlalchemy import Table, Column, Integer, ForeignKey, String, Float
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()


# Not Null
class Specifications(Base):
    __tablename__ = 'specifications'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model = Column(VARCHAR(10))
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

    specs = relationship("Specifications", backref="values")

    def __str__(self):
        return f"The value of model_id is : {self.model_id}"


conn = "mysql+pymysql://saran:SADA2028jaya@localhost/students"
engine = create_engine(conn)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine, autoflush=False)
session = Session()

df = pd.read_excel("specs.xlsx")

for index, row in df.iterrows():

    c1 = Specifications()
    c1.model = row['Model']
    c1.capacity_control = row['Capacity_control']
    c1.refrigerant = row['Refrigerant']
    c1.technology = row['Technology']

    session.add(c1)

session.commit()

df1 = pd.read_csv("compressor_models.csv")
for index, row in df1.iterrows():
    v1 = Values()

    v1.model_id = row['model']
    v1.power = row['power']
    v1.cond_temp = row['condenser_temp']
    v1.evap_temp = row['evaporator_temp']
    specs_object = session.query(Specifications).filter_by(model=v1.model_id).first()
    v1.specs = specs_object

    session.add(v1)


session.commit()
