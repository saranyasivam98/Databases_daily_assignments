from sqlalchemy import Table, Column, Integer, ForeignKey, Sequence
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT, DATE
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()
metadata = MetaData()


class YieldDB(Base):
    __tablename__ = 'yield_data'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    season_yield = Column(INTEGER, nullable=False)
    crop = Column(VARCHAR(15), nullable=False)
    dist_id = Column(INTEGER, nullable=False)
    district = Column(VARCHAR(25), nullable=False)
    year = Column(VARCHAR(15), nullable=False)
    season = Column(VARCHAR(10), nullable=False)
    total = Column(INTEGER, nullable=False)


class GroundWaterLevel(Base):
    __tablename__ = 'groundwater_level'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    year = Column(VARCHAR(15), nullable=False)
    district = Column(VARCHAR(25), nullable=False)
    season = Column(VARCHAR(10), nullable=False)
    water_level = Column(FLOAT, nullable=False)


class PHValues(Base):
    __tablename__ = 'ph_values'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    year = Column(VARCHAR(15), nullable=False)
    district = Column(VARCHAR(25), nullable=False)
    ph = Column(FLOAT, nullable=False)


class SoilNutrients(Base):
    __tablename__ = 'soil_nutrients'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    district = Column(VARCHAR(25), nullable=False)
    zinc = Column(FLOAT, nullable=False)
    iron = Column(FLOAT, nullable=False)
    manganese = Column(FLOAT, nullable=False)
    copper = Column(FLOAT, nullable=False)


class LatLong(Base):
    __tablename__ = 'latlong'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    district = Column(VARCHAR(25), nullable=False)
    latitude = Column(FLOAT, nullable=False)
    longitude = Column(FLOAT, nullable=False)


class Acreage(Base):
    __tablename__ = 'acreage'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    crop = Column(VARCHAR(15), nullable=False)
    season = Column(VARCHAR(10), nullable=False)
    year = Column(VARCHAR(15), nullable=False)
    district = Column(VARCHAR(25), nullable=False)
    actual_area = Column(FLOAT, nullable=False)


class Weather(Base):
    __tablename__ = 'weather'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    district = Column(VARCHAR(25), nullable=False)
    crop_season = Column(VARCHAR(10), nullable=False)
    date = Column(DATE, nullable=False)
    temp_min = Column(FLOAT, nullable=False)
    temp_max = Column(FLOAT, nullable=False)
    rainfall = Column(FLOAT, nullable=False)
    humidity_min = Column(FLOAT, nullable=False)
    humidity_max = Column(FLOAT, nullable=False)
    Wind_max = Column(FLOAT, nullable=False)


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/yield_prediction"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    df = pd.read_excel("yield_files/1_yield_data (1).xlsx")
    df.to_sql('yield_data', con=engine, if_exists='replace', index_label='id')

    gwl = pd.read_excel("yield_files/groundwater_level.xlsx")
    gwl.to_sql('groundwater_level', con=engine, if_exists='replace', index_label='id')

    ph_values = pd.read_excel("yield_files/ph_values.xlsx")
    ph_values.to_sql('ph_values', con=engine, if_exists='replace', index_label='id')

    soil = pd.read_excel("yield_files/soil_nutrients.xlsx")
    soil.to_sql('soil_nutrients', con=engine, if_exists='replace', index_label='id')

    latlong = pd.read_excel("yield_files/latlong.xlsx")
    latlong.to_sql('latlong', con=engine, if_exists='replace', index_label='id')

    acreage = pd.read_csv("yield_files/acreage.csv")
    acreage.to_sql('acreage', con=engine, if_exists='replace', index_label='id')

    weather = pd.read_excel("yield_files/6_weather_data_ (1).xlsx")
    weather.to_sql('weather', con=engine, if_exists='replace', index_label='id')


if __name__ == '__main__':
    main()
