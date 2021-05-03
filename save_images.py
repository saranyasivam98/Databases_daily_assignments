# -- coding: UTF-8 --

from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, LONGBLOB, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

Base = declarative_base()


class SingleLabelClassificationData(Base):
    __tablename__ = 'classification_data'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    label = Column(VARCHAR(10))
    image = Column(LONGBLOB)
    saved_time = Column(TIMESTAMP, )


def read_file(filename):
    with open(filename, 'rb') as f:
        photo = f.read()
    return photo


def write_file(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)


def add_classification_data(session):
    df = pd.read_excel("C:/Users/saran/Documents/GyanData/blob/path_label.xlsx")

    for index, row in df.iterrows():
        obj = SingleLabelClassificationData()
        obj.label = row['label']
        obj.image = read_file(row['file'])

        session.add(obj)

    session.commit()


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    add_classification_data(session)

    obj = session.query(SingleLabelClassificationData).filter_by(id=1).one()
    write_file(obj.image, 'testing.jpg')


if __name__ == '__main__':
    main()
