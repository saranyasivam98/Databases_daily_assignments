# -- coding: UTF-8 --

"""
Using SQLALCHEMY to query the use cases from ABC Super Market Assignment
"""

import logging
from abcsm_class import Transaction, Purchase, Staff, Product, Branch

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy import select, text

from helpers import setup_logging

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


def update_stock(session):
    textual_sql = text("SELECT * FROM products WHERE product_quantity<500")
    orm_sql = select(Product).from_statement(textual_sql)
    for product_obj in session.execute(orm_sql).scalars():
        LOGGER.info("%s " % product_obj)


def staff_most_products(session):

    sub_query = session.query(Transaction.staff_id.label('staff_id'),
                              func.count(Purchase.product_id).label('no_of_products')). \
        join(Transaction, Transaction.id == Purchase.transaction_id). \
        group_by(Transaction.staff_id).order_by(text('no_of_products desc')).subquery()

    max_query = session.query(Staff).filter(Staff.id == sub_query.c.staff_id).first()

    LOGGER.info("Name of the staff who sold most no of products: %s" % max_query.staff_name)


def most_transaction_branch(session):
    obj = session.query(Transaction.staff_id, func.count(Transaction.trans_code).label("no_of_trans"),
                        Branch.branch_address.label('address')).\
        join(Branch, Transaction.branch_id == Branch.id).group_by(Branch.id).\
        group_by(Transaction.staff_id).order_by(text('no_of_trans desc')).first()
    LOGGER.info("The address of the branch with most no of transactions is: %s" % obj.address)


def main():
    setup_logging()
    # Creating engine
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn)

    # Creating the session
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # Querying for products with quantity less then 500
    update_stock(session)

    # Query for displaying the name of the staff who sold most no of products.
    staff_most_products(session)

    # Query for displaying the address of the branch with most no of transactions.
    most_transaction_branch(session)


if __name__ == '__main__':
    main()
