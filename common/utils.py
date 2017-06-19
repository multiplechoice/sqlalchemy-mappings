from contextlib import contextmanager

import sqlalchemy.engine.base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common import Base


@contextmanager
def session_scope(credentials):
    """Provide a transactional scope around a series of operations."""
    engine = create_engine(credentials)
    session = sessionmaker(bind=engine)()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def install_pgcrypto(db_connection):
    assert isinstance(db_connection, sqlalchemy.engine.base.Engine)
    db_connection.execute("""CREATE EXTENSION IF NOT EXISTS pgcrypto;""")


def create_table(db_connection):
    assert isinstance(db_connection, sqlalchemy.engine.base.Engine)
    Base.metadata.create_all(db_connection, checkfirst=True)
