# -- coding: UTF-8 --
"""
======================
 BLOB
======================
To save images as BLOB
"""
import datetime
from sqlalchemy import Column, create_engine
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, LONGBLOB, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

Base = declarative_base()


class SingleLabelClassificationData(Base):
    """
    A class to store the data for a single label classification problem
    :ivar id: The primary key
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar label: The label of the image
    :vartype label: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar image: The image to be stored in the database
    :vartype image: :class:`sqlalchemy.dialects.mysql.LONGBLOB`

    :ivar saved_time:
    :vartype saved_time: :class:`sqlalchemy.dialects.mysql.TIMESTAMP`
    """
    __tablename__ = 'classification_data'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    label = Column(VARCHAR(10))
    image = Column(LONGBLOB)
    # file_name
    # file_extension to retrieve the image # When the object is created. Client side


def read_file(filename):
    """
    To read the file as raw byte and return that string
    :param filename: Name of the file
    :type filename: str
    :return: The image in raw byte format
    :rtype: bytearray
    """
    with open(filename, 'rb') as file:
        photo = file.read()
    return photo


def write_file(data, filename):
    """
    To open a file and write the contents from the database into the file and save it as a picture.
    :param data: The value read from the database
    :type data: :class:`sqlalchemy.dialects.mysql.LONGBLOB`
    :param filename: Name of the file
    :type filename: str
    :return: None
    """
    with open(filename, 'wb') as file:
        file.write(data)


def add_classification_data(session):
    """
    To read each row and save it in the database.

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_excel("C:/Users/saran/Documents/GyanData/blob/path_label.xlsx")

    for _, row in df.iterrows():
        obj = SingleLabelClassificationData()
        obj.label = row['label']
        obj.image = read_file(row['file'])

        session.add(obj)

    session.commit()


def main():
    """ Main Function"""
    # Create engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    # Creating session
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    add_classification_data(session)

    obj = session.query(SingleLabelClassificationData).filter_by(id=1).one()
    write_file(obj.image, 'testing.jpg')


if __name__ == '__main__':
    main()
