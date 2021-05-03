# -- coding: UTF-8 --

import logging
from abcsm_class import Transaction, Purchase, Staff, Product

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, lazyload, raiseload, joinedload

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    # obj = session.query(Transaction).options(raiseload(Transaction.staff)).filter(Transaction.id == 1).one()
    # obj = session.query(Transaction).options(joinedload(Transaction.branch), raiseload('*')).filter_by(id=1).one()

    obj = session.query(Purchase).options(
        joinedload(Purchase.transaction).
        joinedload(Transaction.staff, innerjoin=True)).filter(Purchase.id == 1).one()
    print(obj.transaction.__dict__)
    print(obj.transaction.staff.staff_email)


if __name__ == '__main__':
    main()
