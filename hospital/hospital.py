# -- coding: UTF-8 --
"""
=======================
Hospital Database
=======================
To build a hospital database
"""

import logging
from sqlalchemy import Column, ForeignKey, create_engine
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYTEXT, FLOAT, BIGINT, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker

import pandas as pd

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Patient(Base):
    """
    Class to store the details of the patient
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar name: Name of the patient
    :vartype name: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar address: Address of the patient
    :vartype address: :class:`sqlalchemy.dialects.mysql.TINYTEXT`

    :ivar ph_no: Phone number of the patient
    :vartype ph_no: :class:`sqlalchemy.dialects.mysql.BIGINT`
    """
    __tablename__ = 'patient'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    name = Column(VARCHAR(50))
    address = Column(TINYTEXT)
    ph_no = Column(BIGINT)

    def __init__(self, name, address, ph_no):
        self.name = name
        self.address = address
        self.ph_no = ph_no


class Doctor(Base):
    """
    Class to store the details of the patient
    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar name: Name of the doctor
    :vartype name: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar department: Address of the patient
    :vartype department: :class:`sqlalchemy.dialects.mysql.VARCHAR`

    :ivar ph_no: Phone number of the patient
    :vartype ph_no: :class:`sqlalchemy.dialects.mysql.BIGINT`
    """
    __tablename__ = 'doctor'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    name = Column(VARCHAR(50))
    department = Column(VARCHAR(100))
    ph_no = Column(BIGINT, unique=True)

    def __init__(self, name, department, ph_no):
        self.name = name
        self.department = department
        self.ph_no = ph_no


class PaymentMixin:
    """
    Mixin class for all payment

    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar date: Date of the payment
    :vartype date: :class:`sqlalchemy.dialects.mysql.DATE`

    :ivar payment: Payment amount
    :vartype payment: :class:`sqlalchemy.dialects.mysql.INTEGER`
    """
    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    date = Column(DATE)
    payment = Column(INTEGER)


class Appointment(Base):
    """
    Class to store all appointment details

    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar doctor_id: Doctor id
    :vartype doctor_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar patient_id: Patient id
    :vartype patient_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar appointment_datetime: Payment amount
    :vartype appointment_datetime: :class:`sqlalchemy.dialects.mysql.DATE`
    """
    __tablename__ = 'appointment'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    doctor_id = Column(INTEGER, ForeignKey("doctor.id"))
    patient_id = Column(INTEGER, ForeignKey("patient.id"))
    appointment_datetime = Column(DATE)

    doctor = relationship("Doctor", backref=backref('appointments', cascade='all, delete-orphan'))
    patient = relationship("Patient", backref=backref('appointments', cascade='all, delete-orphan'))

    def __init__(self, doctor_id, patient_id, appointment_datetime):
        self.doctor_id = doctor_id
        self.patient_id = patient_id
        self.appointment_datetime = appointment_datetime


class ConsultationPayment(PaymentMixin, Base):
    """
    Consultation Payment
    :ivar appointment_id: Appointment id
    :vartype appointment_id: :class:`sqlalchemy.dialects.mysql.INTEGER`
    """
    __tablename__ = 'consultation_payment'
    appointment_id = Column(INTEGER, ForeignKey("appointment.id"))

    appointment = relationship("Appointment", backref=backref("consultation_payment", uselist=False))


class Diagnosis(Base):
    """
    Class to store all diagnosis details

    :ivar id: Primary key of the table
    :vartype id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar doctor_id: Doctor id
    :vartype doctor_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar patient_id: Patient id
    :vartype patient_id: :class:`sqlalchemy.dialects.mysql.INTEGER`

    :ivar appointment_id: Appointment id
    :vartype appointment_id: :class:`sqlalchemy.dialects.mysql.INTEGER`
    """
    __tablename__ = 'diagnosis'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    description = Column(TINYTEXT)
    doctor_id = Column(INTEGER, ForeignKey("doctor.id"))
    patient_id = Column(INTEGER, ForeignKey("patient.id"))
    appointment_id = Column(INTEGER, ForeignKey("appointment.id"))

    doctor = relationship("Doctor", backref='diagnosis')
    patient = relationship("Patient", backref='diagnosis')
    appointment = relationship("Appointment")


class Prescription(Base):
    __tablename__ = 'prescription'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    tablet_id = Column(INTEGER, ForeignKey("tablet.id"))
    diagnosis_id = Column(INTEGER, ForeignKey("diagnosis.id"))
    tablet_quantity = Column(INTEGER)

    tablet = relationship("Tablet", backref='prescription')
    diagnosis = relationship("Diagnosis", backref='prescription')


class MedicinePayment(PaymentMixin, Base):
    __tablename__ = 'medicine_payment'

    diagnosis_id = Column(INTEGER, ForeignKey("diagnosis.id"))


class Tablet(Base):
    __tablename__ = 'tablet'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    name = Column(VARCHAR(50), index=True)
    price = Column(FLOAT)
    stock = Column(INTEGER)

    def __init__(self, name, stock, price):
        self.name = name
        self.stock = stock
        self.price = price


def add_doctor_data(session):
    """
    To add doctor into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/doctors.csv")

    for _, data in df.iterrows():
        session.add(Doctor(data['name'], data['dept'], data['ph_no']))

    session.commit()


def add_patient_data(session):
    """
    To add patient into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/patients.csv")

    for _, data in df.iterrows():
        session.add(Patient(data['name'], data['address'], data['ph_no']))

    session.commit()


def add_appointment_data(session):
    """
    To add appointment into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/appointments.csv")

    for _, data in df.iterrows():
        obj = Appointment(data['doctor_id'], data['patient_id'], data['appointment_datetime'])
        patient_obj = session.query(Patient).filter_by(id=data['patient_id']).one()
        obj.patient = patient_obj

        doctor_obj = session.query(Doctor).filter_by(id=data['doctor_id']).one()
        obj.doctor = doctor_obj

        session.add(obj)

    session.commit()


def add_consultation_payment(session):
    """
    To add consultation into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/consultation_payment.csv")

    for _, data in df.iterrows():
        obj = ConsultationPayment()
        obj.payment = data['payment']
        obj.date = data['date']
        obj.appointment_id = data['appointment_id']
        app_obj = session.query(Appointment).filter_by(id=data['appointment_id']).one()
        obj.appointment = app_obj
        session.add(obj)

    session.commit()


def add_diagnosis_data(session):
    """
    To add diagnosis into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/diagnosis.csv")

    for _, data in df.iterrows():
        obj = Diagnosis()
        obj.patient_id = data['patient_id']
        obj.doctor_id = data['doctor_id']
        obj.description = data['description']
        doctor_obj = session.query(Doctor).filter_by(id=data['doctor_id']).one()
        obj.doctor = doctor_obj
        patient_obj = session.query(Patient).filter_by(id=data['patient_id']).one()
        obj.patient = patient_obj
        app_obj = session.query(Appointment).filter_by(id=data['appointment_id']).one()
        obj.appointment = app_obj

        session.add(obj)

    session.commit()


def add_tablets_data(session):
    """
    To add tablets into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/tablets.csv")

    for _, data in df.iterrows():
        obj = Tablet(data['name'], data['stock'], data['price'])
        session.add(obj)

    session.commit()


def add_prescription_data(session):
    """
    To add prescription into the database

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    df = pd.read_csv("hospital/prescription.csv")

    for _, data in df.iterrows():
        obj = Prescription()
        obj.tablet_id = data['tablet_id']
        obj.diagnosis_id = data['diagnosis_id']
        obj.tablet_quantity = data['tablet_quantity']

        tablet_obj = session.query(Tablet).filter_by(id=data['tablet_id']).one()
        obj.tablet = tablet_obj

        diagnosis_obj = session.query(Diagnosis).filter_by(id=data['diagnosis_id']).one()
        obj.diagnosis = diagnosis_obj

    session.commit()


def add_medicine_payment(session):
    """ Adding medicine payment"""
    df = pd.read_csv("hospital/medicine_payment.csv")

    for _, data in df.iterrows():
        obj = MedicinePayment()
        obj.diagnosis_id = data['diagnosis_id']
        obj.payment = data['payment']
        obj.date = data['date']

        session.add(obj)

    session.commit()


def main():
    """Main Function"""
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/hospital_db"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    # Creating the session
    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    add_doctor_data(session)
    add_patient_data(session)
    add_tablets_data(session)

    add_appointment_data(session)
    add_consultation_payment(session)

    add_diagnosis_data(session)
    add_prescription_data(session)

    add_medicine_payment(session)


if __name__ == '__main__':
    main()
