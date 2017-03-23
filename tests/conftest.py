import pytest
import sqlalchemy as sa


@pytest.fixture(scope='function')
def db(request):
    engine = sa.create_engine("sqlite://")
    conn = engine.connect()
    transaction = conn.begin()

    def teardown():
        transaction.rollback()
        conn.close()
        engine.dispose()

    request.addfinalizer(teardown)
    return conn
