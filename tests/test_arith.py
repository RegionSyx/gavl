import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
import gavl


Base = declarative_base()
metadata = Base.metadata


class SampleTable(Base):
    __tablename__ = 'test_table'

    pk = Column(Integer, primary_key=True)
    left = Column(Integer, nullable=False)
    right = Column(Integer, nullable=False)

@pytest.mark.skip()
@pytest.mark.parametrize('testcase', [
    ('test.left + test.right', 12),
    ('test.right + test.left', 12),
    ('test.left + test.left', 14),
])
def test_simple_add(db, testcase):

    SampleTable.__table__.create(bind=db)
    ins = SampleTable.__table__.insert().values(pk=1, left=7, right=5)
    db.execute(ins)

    gavl_engine = gavl.Engine(db.engine)
    gavl_engine.add_relation('test', gavl.SARelation(db.engine, SampleTable, {}))

    query = testcase[0]
    result = gavl_engine.query(query)
    assert result["result"].iloc[0] == testcase[1]



@pytest.mark.skip()
@pytest.mark.parametrize('testcase', [
    ('test.left - test.right', 2),
    ('test.right - test.left', -2),
    ('test.left - test.left', 0),
])
def test_simple_subtract(db, testcase):
    SampleTable.__table__.create(bind=db)
    ins = SampleTable.__table__.insert().values(pk=1, left=7, right=5)
    db.execute(ins)

    gavl_engine = gavl.Engine(db.engine)
    gavl_engine.add_relation('test', gavl.SARelation(db.engine, SampleTable, {}))

    query = testcase[0]
    result = gavl_engine.query(query)
    assert result["result"].iloc[0] == testcase[1]



@pytest.mark.skip()
@pytest.mark.parametrize('testcase', [
    ('test.left * test.right', 35),
    ('test.right * test.left', 35),
    ('test.left * test.left', 49),
])
def test_simple_multiply(db, testcase):
    SampleTable.__table__.create(bind=db)
    ins = SampleTable.__table__.insert().values(pk=1, left=7, right=5)
    db.execute(ins)

    gavl_engine = gavl.Engine(db.engine)
    gavl_engine.add_relation('test', gavl.SARelation(db.engine, SampleTable, {}))

    query = testcase[0]
    result = gavl_engine.query(query)
    assert result["result"].iloc[0] == testcase[1]



@pytest.mark.skip()
@pytest.mark.parametrize('testcase', [
    ('test.left / test.right', 1),
    ('test.right / test.left', 0),
    ('test.left / test.left', 1),
])
def test_simple_divide(db, testcase):
    SampleTable.__table__.create(bind=db)
    ins = SampleTable.__table__.insert().values(pk=1, left=7, right=5)
    db.execute(ins)

    gavl_engine = gavl.Engine(db.engine)
    gavl_engine.add_relation('test', gavl.SARelation(db.engine, SampleTable, {}))

    query = testcase[0]
    result = gavl_engine.query(query)
    assert result["result"].iloc[0] == testcase[1]
