# -- coding: UTF-8 --
"""
========================
Transaction states
========================
Different Transaction states
"""
import logging
from sqlalchemy import Column, inspect, create_engine
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from helpers import setup_logging

Base = declarative_base()

__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)


# Not Null
class Specifications(Base):
    """
    To store the properties of compressor

    :ivar model: Model of the compressor
    :vartype model: str

    :ivar technology: Technology of the motor
    :vartype technology: str

    :ivar refrigerant: Refrigerant used in the compressor
    :vartype refrigerant: str

    :ivar capacity_control: Speed control of the compressor
    :vartype capacity_control: str
    """
    __tablename__ = 'specifications'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    model = Column(VARCHAR(10), unique=True)
    capacity_control = Column(VARCHAR(15))
    refrigerant = Column(VARCHAR(10))
    technology = Column(VARCHAR(15))

    def __init__(self, model, cap, ref, tech):
        self.model = model
        self.technology = tech
        self.refrigerant = ref
        self.capacity_control = cap


def add_in_transaction(session):
    """
    To observe the state change of an object from Transient to Pending to Persistent

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    for i in range(5):
        try:
            LOGGER.info("___________Transient to Pending to Persistent____________")
            obj = Specifications(str(i), 'Variable Speed', 'R407B', 'Reciprocating')
            insp = inspect(obj)
            LOGGER.info("After creating the object, its state is Transient: %s", insp.transient)

            session.add(obj)
            LOGGER.info("After adding the object to the session, its state is Pending: %s", insp.pending)

            if i == 3:
                raise RuntimeError()
            LOGGER.info("After flushing the state is Pending:%s", insp.pending)

            session.commit()
            LOGGER.info("After its committed, its state is Persistent: %s", insp.persistent)

        except Exception as ex:
            session.rollback()
            raise ex


def delete_in_transaction(session):
    """
    To observe the state change of an object from Persistent to Deleted to Detached

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    LOGGER.info("_________Persistent to Deleted to Detached____________")
    obj = session.query(Specifications).filter(Specifications.model == 'MTZO64-2').one()
    LOGGER.info("Object state when queried from Db is Persistent: %s", inspect(obj).persistent)
    session.delete(obj)
    session.flush()
    LOGGER.info("Object state after flush is Deleted: %s", inspect(obj).deleted)

    session.commit()
    LOGGER.info("Deleted object after committing has state Detached: %s", inspect(obj).detached)


def pending_to_transient(session):
    """
    To observe the state change of an object from Pending to Transient

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    LOGGER.info("_____________Pending to Transient________________")
    obj = Specifications('MTZO64-A', 'Variable Speed', 'R407B', 'Reciprocating')
    insp = inspect(obj)
    LOGGER.info("After creating the object, its state is Transient: %s", insp.transient)

    session.add(obj)
    LOGGER.info("After adding the object to the session, its state is Pending: %s", insp.pending)

    session.rollback()
    LOGGER.info("After rollback, the state is Transient: %s", insp.transient)


def persistent_to_transient(session):
    """
    To observe the state change of an object from Persistent to Transient

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    LOGGER.info("___________Persistent to Transient____________")
    obj = Specifications('MTZO64-B', 'Variable Speed', 'R407B', 'Reciprocating')
    insp = inspect(obj)
    LOGGER.info("After creating the object, its state is Transient: %s", insp.transient)

    session.add(obj)
    LOGGER.info("After adding the object to the session, its state is Pending: %s", insp.pending)

    session.flush()
    LOGGER.info("After its flushed, its state is Persistent: %s", insp.persistent)

    session.rollback()
    LOGGER.info("After rolling back from flush, its state is Transient: %s", insp.transient)


def persistent_to_detached(session):
    """
    To observe the state change of an object from Persistent to Detached

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    LOGGER.info("___________Persistent to Detached_______________")
    obj = session.query(Specifications).filter(Specifications.model == 'MTZO64-2').one()
    LOGGER.info("Object queried from Db had state persistent when loaded: %s", inspect(obj).persistent)
    session.delete(obj)

    session.commit()
    LOGGER.info("Deleted object after committing has state Detached: %s", inspect(obj).detached)


def deleted_to_persistent(session):
    """
    To observe the state change of an object from Persistent to Deleted to Persistent

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    LOGGER.info("_________Deleted to Persistent____________")
    obj = session.query(Specifications).filter(Specifications.model == 'MTZO64-2').one()
    LOGGER.info("Object state when queried from Db is Persistent: %s", inspect(obj).persistent)
    session.delete(obj)
    session.flush()
    LOGGER.info("Object state after flush is Deleted: %s", inspect(obj).deleted)

    session.rollback()
    LOGGER.info("Flushed object after rollback has state Persistent: %s", inspect(obj).persistent)


def detached_to_persistent(session):
    """
    To observe the state change of an object from Detached to Persistent

    :param session: An sqlalchemy session
    :type session: :class:`sqlalchemy.orm.session.Session`

    :return: None
    """
    LOGGER.info("____________Detached to Persistent____________")
    obj = session.query(Specifications).filter(Specifications.model == 'MTZO64-2').one()
    LOGGER.info("Object state when queried from Db is Persistent: %s", inspect(obj).persistent)
    session.delete(obj)
    session.flush()
    LOGGER.info("Object state after flush is Deleted: %s", inspect(obj).deleted)

    session.commit()
    LOGGER.info("Deleted object after committing has state Detached: %s", inspect(obj).detached)

    session.add(obj)
    LOGGER.info("After adding the obj using session.add it state is Persistent: %s", inspect(obj).persistent)
    

def main():
    """ Main function"""
    setup_logging()

    conn = "mysql+pymysql://saran:SADA2028jaya@localhost:3306/learning"
    engine = create_engine(conn, echo=True)

    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine, autoflush=False)
    session = session_factory()

    add_in_transaction(session)
    # delete_in_transaction(session)

    # pending_to_transient(session)
    # persistent_to_transient(session)
    # persistent_to_detached(session)
    # deleted_to_persistent(session)
    # detached_to_persistent(session)

    obj = session.query(Specifications).filter_by(id=1).one()
    LOGGER.info(obj.id)


if __name__ == '__main__':
    main()
