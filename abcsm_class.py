# -- coding: UTF-8 --
"""
Build a database for the ABC super market assignment
"""

from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR,  BIGINT, DATETIME
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd
import json
import logging

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


class Staff(Base):
    __tablename__ = 'staff'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    staff_code = Column(VARCHAR(10), unique=True)
    staff_name = Column(VARCHAR(50), index=True)
    staff_email = Column(VARCHAR(50), unique=True)
    staff_ph_no = Column(BIGINT, unique=True)

    def __init__(self, staff_code, staff_name, staff_email, staff_ph_no):
        self.staff_code = staff_code
        self.staff_name = staff_name
        self.staff_email = staff_email
        self.staff_ph_no = staff_ph_no

    def __str__(self):
        return f"<Staff Code:{self.staff_code}, Name:{self.staff_name}, Email:{self.staff_email}, " \
               f"Phone Number:{self.staff_ph_no}>"


class Customer(Base):
    __tablename__ = 'customer'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    customer_code = Column(VARCHAR(10), unique=True)
    customer_name = Column(VARCHAR(50), index=True)
    customer_email = Column(VARCHAR(50), unique=True)
    customer_ph_no = Column(BIGINT, unique=True)

    def __init__(self, customer_code, customer_name, customer_email, customer_ph_no):
        self.customer_code = customer_code
        self.customer_name = customer_name
        self.customer_email = customer_email
        self.customer_ph_no = customer_ph_no

    def __str__(self):
        return f"<Customer Code:{self.customer_code}, Name:{self.customer_name}, Email:{self.customer_email}, " \
               f"Phone Number:{self.customer_ph_no}>"


class Branch(Base):
    __tablename__ = 'branch'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    branch_code = Column(VARCHAR(15), unique=True)
    branch_address = Column(VARCHAR(150))
    branch_ph_no = Column(BIGINT, unique=True)

    def __init__(self, branch_code, branch_address, branch_ph_no):
        self.branch_code = branch_code
        self.branch_address = branch_address
        self.branch_ph_no = branch_ph_no

    def __str__(self):
        return f"<Branch Code:{self.branch_code}, Email:{self.branch_email}, " \
           f"Phone Number:{self.branch_ph_no}>"


class Transaction(Base):
    __tablename__ = 'transaction'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    trans_code = Column(VARCHAR(25), unique=True)
    trans_date = Column(DATETIME)
    branch_id = Column(INTEGER, ForeignKey("branch.id"))
    customer_id = Column(INTEGER, ForeignKey("customer.id"))
    staff_id = Column(INTEGER, ForeignKey("staff.id"))

    branch = relationship("Branch", backref='transactions')
    staff = relationship("Staff", backref='transactions')
    customer = relationship("Customer", backref='transactions')

    def __str__(self):
        return f"<Transaction Code: {self.trans_code}, Date:{self.trans_date}, Branch: {self.branch_id}, " \
               f"Customer: {self.customer_id}, Staff: {self.staff_id}>"


class Product(Base):
    __tablename__ = 'products'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    product_name = Column(VARCHAR(25), unique=True)
    product_quantity = Column(INTEGER)

    def __init__(self, product_name, product_quantity):
        self.product_quantity = product_quantity
        self.product_name = product_name

    def __str__(self):
        return f"Product Name: {self.product_name}, Quantity: {self.product_quantity}"


class Purchase(Base):
    __tablename__ = 'purchase'

    id = Column(INTEGER, primary_key=True, autoincrement=True, index=True)
    purchase_code = Column(VARCHAR(25))
    transaction_id = Column(INTEGER, ForeignKey("transaction.id"))
    product_id = Column(INTEGER, ForeignKey("products.id"))

    transaction = relationship("Transaction", backref='purchase')
    product = relationship("Product", backref='purchase')

    def __str__(self):
        return f"Purchase Code: {self.purchase_code}, Transaction: {self.transaction_id}, Product: {self.product_id}"


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


def get_product_data(path):
    with open(path) as file:
        staff_data = json.load(file)

    for data in staff_data:
        yield Product(data['product_name'], data['product_quantity'])


def add_transaction(session):
    try:
        with open("abc_super_market/transactions.json") as file:
            transactions = json.load(file)
        for data in transactions:
            obj = Transaction()
            obj.trans_code = data['trans_id']
            obj.trans_date = data['trans_date']

            branch_obj = session.query(Branch).filter_by(branch_code=data['branch_details']).one()
            obj.branch = branch_obj

            customer_obj = session.query(Customer).filter_by(customer_code=data['customer_details']).one()
            obj.customer = customer_obj

            staff_obj = session.query(Staff).filter_by(staff_code=data['staff_details']).one()
            obj.staff = staff_obj

            session.add(obj)
    except:
        session.rollback()
    else:
        session.commit()


def add_purchase(session):
    try:
        with open("sample.json") as file:
            purchases = json.load(file)

        for purchase in purchases:
            obj = Purchase()

            obj.purchase_code = purchase['purchase_id']

            transaction_obj = session.query(Transaction).filter_by(trans_code=purchase['trans_details']).one()
            obj.transaction = transaction_obj

            product_obj = session.query(Product).filter_by(product_name=purchase['product_details']).one()
            obj.product = product_obj

            session.add(obj)
    except:
        session.rollback()
    else:
        session.commit()


def main():
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    # Creating the tables in the DB
    Base.metadata.create_all(engine)

    # Creating the session
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # Adding Branch, Staff, Customer, Products data
    session.bulk_save_objects(get_branch_data("abc_super_market/branch.json"))
    session.bulk_save_objects(get_staff_data("abc_super_market/staff.json"))
    session.bulk_save_objects(get_customer_data("abc_super_market/customers.json"))
    session.bulk_save_objects(get_product_data("abc_super_market/products.json"))

    session.commit()

    add_transaction(session)
    add_purchase(session)


if __name__ == '__main__':
    main()
