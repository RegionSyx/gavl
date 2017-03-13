import pytest
import sqlalchemy as sa


@pytest.fixture(scope='module')
def db():
    engine = sa.create_engine("sqlite://")
    yield engine
    engine.dispose()


@pytest.fixture(scope='module')
def connection(db):
    conn = db.connect()
    yield conn
    conn.close()


@pytest.fixture(scope='function')
def transaction(connection):
    transaction = connection.begin()
    yield transaction
    transaction.rollback()
