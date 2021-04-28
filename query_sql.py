from abcsm_class import Transaction, Purchase, Staff

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker


def main():
    conn = "mysql+pymysql://saran:SADA2028jaya@localhost/students"
    engine = create_engine(conn, echo=True)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    # Query for displaying the name of the staff who sold most no of products.

    sub_query = session.query(Transaction.staff_id.label('staff_id'), func.count(Purchase.product_id).label('no_of_products')).\
        join(Transaction, Transaction.id == Purchase.transaction_id).\
        group_by(Transaction.staff_id).subquery()
    print(sub_query)

    max_query = session.query(Staff, func.min(sub_query.c.no_of_products)).filter(Staff.id == sub_query.c.staff_id)

    for a, b in max_query.all():
        print(a.staff_name, b)


if __name__ == '__main__':
    main()
