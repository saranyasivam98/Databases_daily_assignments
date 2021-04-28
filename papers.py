from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYTEXT, TEXT, JSON
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json

Base = declarative_base()


class Paper(Base):
    __tablename__ = 'papers'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    paper_id = Column(VARCHAR(15), index=True)
    submitter = Column(VARCHAR(50))
    authors = Column(TINYTEXT)
    title = Column(TINYTEXT)
    abstract = Column(TEXT)
    license_paper = Column(TINYTEXT)
    versions = Column(JSON)


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/students"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    '''with open("ten.json") as file:
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

    session.commit()'''

    for data in session.query(Paper).filter_by(id=2).all():
        print(data.versions)


if __name__ == '__main__':
    main()
