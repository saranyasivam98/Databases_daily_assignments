# -- coding: UTF-8 --
"""
To build a hospital database
"""

import logging
from sqlalchemy import Column, ForeignKey, create_engine
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYTEXT, TEXT, JSON, DATETIME, FLOAT, BIGINT, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker

import pandas as pd

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Patient(Base):
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
    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    date = Column(DATE)
    payment = Column(INTEGER)


class Appointment(Base):
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
    __tablename__ = 'consultation_payment'
    appointment_id = Column(INTEGER, ForeignKey("appointment.id"))

    appointment = relationship("Appointment", backref=backref("consultation_payment", uselist=False))


class Diagnosis(Base):
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


'''  
class LabReport(Base):
    __tablename__ = 'labreport'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    lab_id = Column(INTEGER, ForeignKey("lab.id"))
    test_sample_id = Column(INTEGER, ForeignKey("testsample.id"))
    doctor_id = Column(INTEGER, ForeignKey("doctor.id"))
    patient_id = Column(INTEGER, ForeignKey("patient.id"))
    diagnosis_id = Column(INTEGER, ForeignKey("diagnosis.id"))
    payment_id = Column(INTEGER, ForeignKey("test_payment.id"))
    result = Column(JSON)

    doctor = relationship("Doctor", backref='labreport')
    diagnosis = relationship("Diagnosis", backref='labreport')
    patient = relationship("Patient", backref='labreport')
    lab = relationship("Doctor", backref='labreport')
    test_sample = relationship("TestSample", backref='labreport')
    payment = relationship("TestPayment")


class TestPayment(PaymentMixin, Base):
    __tablename__ = 'test_payment'

    appointment_id = Column(INTEGER, ForeignKey("appointment.id"))
'''


class Lab(Base):
    __tablename__ = 'lab'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    lab_name = Column(VARCHAR(50))
    description = Column(TINYTEXT)


class TestSample(Base):
    __tablename__ = 'testsample'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    sample = Column(VARCHAR(50))


def add_doctor_data(session):
    df = pd.read_csv("hospital/doctors.csv")

    for index, data in df.iterrows():
        session.add(Doctor(data['name'], data['dept'], data['ph_no']))

    session.commit()


def add_patient_data(session):
    df = pd.read_csv("hospital/patients.csv")

    for index, data in df.iterrows():
        session.add(Patient(data['name'], data['address'], data['ph_no']))

    session.commit()


def add_appointment_data(session):
    df = pd.read_csv("hospital/appointments.csv")

    for index, data in df.iterrows():
        obj = Appointment(data['doctor_id'], data['patient_id'], data['appointment_datetime'])
        patient_obj = session.query(Patient).filter_by(id=data['patient_id']).one()
        obj.patient = patient_obj

        doctor_obj = session.query(Doctor).filter_by(id=data['doctor_id']).one()
        obj.doctor = doctor_obj

        session.add(obj)

    session.commit()


def add_consultation_payment(session):
    df = pd.read_csv("hospital/consultation_payment.csv")

    for index, data in df.iterrows():
        obj = ConsultationPayment()
        obj.payment = data['payment']
        obj.date = data['date']
        obj.appointment_id = data['appointment_id']
        app_obj = session.query(Appointment).filter_by(id=data['appointment_id']).one()
        obj.appointment = app_obj
        session.add(obj)

    session.commit()


def add_diagnosis_data(session):
    df = pd.read_csv("hospital/diagnosis.csv")

    for index, data in df.iterrows():
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
    df = pd.read_csv("hospital/tablets.csv")

    for index, data in df.iterrows():
        obj = Tablet(data['name'], data['stock'], data['price'])
        session.add(obj)

    session.commit()


def add_prescription_data(session):
    df = pd.read_csv("hospital/prescription.csv")

    for index, data in df.iterrows():
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
    df = pd.read_csv("hospital/medicine_payment.csv")

    for index, data in df.iterrows():
        obj = MedicinePayment()
        obj.diagnosis_id = data['diagnosis_id']
        obj.payment = data['payment']
        obj.date = data['date']

        session.add(obj)

    session.commit()


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/hospital_db"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

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
