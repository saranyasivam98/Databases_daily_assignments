# -- coding: UTF-8 --
"""
Explaining the use cases of the hospital database
"""

import logging
from sqlalchemy import Table, Column, Integer, ForeignKey, String, Float
from sqlalchemy.orm import relationship, lazyload, raiseload, joinedload, subqueryload


from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from hospital import Doctor, Patient, Appointment, ConsultationPayment, Diagnosis

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/hospital_db"
    engine = create_engine(conn, echo=True)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    # Doctor removes one of his appointments
    '''obj = session.query(Doctor).filter_by(id=14).one()
    print(obj.appointments[0].patient_id)

    a1 = obj.appointments[0]
    obj.appointments.remove(a1)
    session.commit()'''

    # Doctor leaves the hospital
    '''doc = session.query(Doctor).filter_by(id=14).one()
    session.delete(doc)
    session.commit()'''

    # Doctor has to access the previous diagnosis
    obj1 = session.query(Patient).options(joinedload(Patient.diagnosis)).\
        filter(Patient.id == 1).one()

    for obj in obj1.diagnosis:
        print(obj.id)


if __name__ == '__main__':
    main()
