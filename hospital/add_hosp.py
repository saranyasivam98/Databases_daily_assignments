# -- coding: UTF-8 --
"""
=============================
Hospital database operations
=============================
Explaining the use cases of the hospital database
"""

import logging
from sqlalchemy.orm import joinedload

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, subqueryload

from hospital import Diagnosis, Doctor, Patient
from helpers import setup_logging

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


def main():
    """Main Function"""
    setup_logging()
    # Creating the engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/hospital_db"
    engine = create_engine(conn, echo=True)

    # Creating the session
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    # Doctor removes one of his appointments
    obj = session.query(Doctor).filter_by(id=14).one()
    LOGGER.info(obj.appointments[0].patient_id)

    obj1 = obj.appointments[0]
    obj1.appointments.remove(obj1)
    session.commit()

    # Doctor leaves the hospital
    doc = session.query(Doctor).filter_by(id=14).one()
    session.delete(doc)
    session.commit()

    # Doctor has to access the previous diagnosis
    obj1 = session.query(Patient).options(subqueryload(Patient.diagnosis)).\
        filter(Patient.id == 1).one()

    for obj in obj1.diagnosis:
        LOGGER.info(obj.id)
    obj = session.query(Diagnosis).options(joinedload(Diagnosis.doctor).raiseload('*')).first()
    LOGGER.info(obj.patient)


if __name__ == '__main__':
    main()
