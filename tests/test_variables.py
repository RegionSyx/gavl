from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
import gavl


Base = declarative_base()
metadata = Base.metadata


class SampleTable(Base):
    __tablename__ = 'test_table'

    pk = Column(Integer, primary_key=True)
    field = Column(Integer, nullable=False)


def test_simple_load(db, connection, transaction):
    assert connection.in_transaction()

    SampleTable.__table__.create(bind=connection)
    ins = SampleTable.__table__.insert().values(pk=1, field=42)
    connection.execute(ins)

    gavl_engine = gavl.Engine(db)
    gavl_engine.add_relation('test', gavl.SARelation(db, SampleTable, {}))

    query = "SUM(test.field)"
    result = gavl_engine.query(query)
    assert result["result"].iloc[0] == 42
