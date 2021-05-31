# -- coding: UTF-8 --
"""
===================
Loading techniques
===================
Different loading techniques
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload
from abc_super_market.abcsm_class import Transaction, Purchase

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


def main():
    """ Main Function"""
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/learning"
    engine = create_engine(conn, echo=True)

    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    # obj = session.query(Transaction).options(raiseload(Transaction.staff)).filter(Transaction.id == 1).one()
    # obj = session.query(Transaction).options(joinedload(Transaction.branch), raiseload('*')).filter_by(id=1).one()

    obj = session.query(Purchase).options(
        joinedload(Purchase.transaction).
        joinedload(Transaction.staff, innerjoin=True)).filter(Purchase.id == 1).one()
    print(obj.transaction.__dict__)
    print(obj.transaction.staff.staff_email)


if __name__ == '__main__':
    main()
