from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT, DATE
from sqlalchemy.ext.declarative import declarative_base

from yiled_db import GroundWaterLevel, Weather

conn = "mysql+pymysql://saran:SADA2028jaya@localhost/yield_prediction"
engine = create_engine(conn, echo=True)


Session = sessionmaker(bind=engine)
session = Session()

value1 = session.query(Weather.id, Weather.rainfall).filter(Weather.id == '1').one()
print(value1.id, value1.rainfall, value1.temp_min)
