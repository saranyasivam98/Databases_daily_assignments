from sqlalchemy import Table, Column, Integer, ForeignKey, Sequence
from sqlalchemy.orm import synonym
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()


class MyClass(Base):
    __tablename__ = 'my_table'

    id = Column(INTEGER, primary_key=True)
    status = Column(VARCHAR(50))

    @property
    def job_status(self):
        return "Status: " + self.status

    job_status = synonym("status", descriptor=job_status)


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn)  # , echo=True)

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    session.add(MyClass(status = 'unemployed'))
    session.commit()

    q = session.query(MyClass).first()
    print(q.job_status)

    q.job_status = 'employed'
    print(q.job_status)


if __name__ == '__main__':
    main()
