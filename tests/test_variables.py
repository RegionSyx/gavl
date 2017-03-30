from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
import gavl


Base = declarative_base()
metadata = Base.metadata


class SampleTable(Base):
    __tablename__ = 'test_table'

    pk = Column(Integer, primary_key=True)
    field = Column(Integer, nullable=False)


def test_simple_load(db):
    assert db.in_transaction()

    SampleTable.__table__.create(bind=db)
    ins = SampleTable.__table__.insert().values(pk=1, field=42)
    db.execute(ins)

    gavl_engine = gavl.Engine(db.engine)
    gavl_engine.add_relation('test', gavl.SARelation(db.engine, SampleTable, {}))

    query = "test.field"
    result = gavl_engine.query(query)
    assert result["result"].iloc[0] == 42
