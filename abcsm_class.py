from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR,  BIGINT, DATETIME
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd
import json

Base = declarative_base()


class Staff(Base):
    __tablename__ = 'staff'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    staff_code = Column(VARCHAR(10), primary_key=True)
    staff_name = Column(VARCHAR(50), unique=True, index=True)
    staff_email = Column(VARCHAR(50), unique=True)
    staff_ph_no = Column(BIGINT, unique=True)

    def __init__(self, staff_code, staff_name, staff_email, staff_ph_no):
        self.staff_code = staff_code
        self.staff_name = staff_name
        self.staff_email = staff_email
        self.staff_ph_no = staff_ph_no


class Customer(Base):
    __tablename__ = 'customer'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    customer_code = Column(VARCHAR(10), primary_key=True)
    customer_name = Column(VARCHAR(50), unique=True, index=True)
    customer_email = Column(VARCHAR(50), unique=True)
    customer_ph_no = Column(BIGINT, unique=True)

    def __init__(self, customer_code, customer_name, customer_email, customer_ph_no):
        self.customer_code = customer_code
        self.customer_name = customer_name
        self.customer_email = customer_email
        self.customer_ph_no = customer_ph_no


class Branch(Base):
    __tablename__ = 'branch'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    branch_code = Column(VARCHAR(15), unique=True, primary_key=True)
    branch_address = Column(VARCHAR(150))
    branch_ph_no = Column(BIGINT, unique=True)

    def __init__(self, branch_code, branch_address, branch_ph_no):
        self.branch_code = branch_code
        self.branch_address = branch_address
        self.branch_ph_no = branch_ph_no


class Transaction(Base):
    __tablename__ = 'transaction'

    trans_id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    trans_code = Column(VARCHAR(25), unique=True)
    trans_date = Column(DATETIME)
    branch_id = Column(INTEGER, ForeignKey("branch.id"))
    customer_id = Column(INTEGER, ForeignKey("customer.id"))
    staff_id = Column(INTEGER, ForeignKey("staff.id"))

    branch = relationship("Branch", backref='transactions')
    staff = relationship("Staff", backref='transactions')
    customer = relationship("Customer", backref='transactions')

    '''def __init__(self, trans_code, trans_date, customer_details, staff_details, branch_details):
        self.trans_code = trans_code
        self.trans_date = trans_date
        self.customer_id = customer_details
        self.staff_id = staff_details
        self.branch_id = branch_details'''


def get_branch_data(path):
    with open(path) as file:
        branch_data = json.load(file)

    for data in branch_data:
        yield Branch(data['branch_code'], data['branch_address'], data['branch_ph_no'])


def get_customer_data(path):
    with open(path) as file:
        customer_data = json.load(file)

    for data in customer_data:
        yield Customer(data['customer_code'], data['customer_name'], data['customer_email'], data['customer_ph_no'])


def get_staff_data(path):
    with open(path) as file:
        staff_data = json.load(file)

    for data in staff_data:
        yield Staff(data['staff_id'], data['staff_name'], data['staff_email'], data['staff_ph_no'])


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/students"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    '''session.bulk_save_objects(get_branch_data("abc_super_market/branch.json"))
    session.bulk_save_objects(get_staff_data("abc_super_market/staff.json"))
    session.bulk_save_objects(get_customer_data("abc_super_market/customers.json"))'''

    session.commit()

    with open("abc_super_market/transactions.json") as file:
        transactions = json.load(file)

    for data in transactions:
        obj = Transaction()        #data['trans_id'], data['trans_date'], data['customer_details'], data['staff_details'],
                          # data['branch_details'])
        obj.trans_code = data['trans_id']
        obj.trans_date = data['trans_date']
        obj.customer_id = data['customer_details']
        obj.staff_id = data['staff_details']
        obj.branch_id = data['branch_details']

        # branch_obj = session.query(Branch).filter_by(branch_code=obj.branch_id).one()
        # obj.branch = branch_obj

        session.add(obj)

    session.commit()

    test_obj = session.query(Transaction).filter_by(id=1)
    print(test_obj.branch.branch_id)

if __name__ == '__main__':
    main()
