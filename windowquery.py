import sqlalchemy
from sqlalchemy import and_, func

def column_windows(session, column, windowsize):
    """Return a series of WHERE clauses against 
    a given column that break it into windows.

    Result is an iterable of tuples, consisting of
    ((start, end), whereclause), where (start, end) are the ids.

    Requires a database that supports window functions, 
    i.e. Postgresql, SQL Server, Oracle.

    Enhance this yourself !  Add a "where" argument
    so that windows of just a subset of rows can
    be computed.

    """
    def int_for_range(start_id, end_id):
        if end_id:
            return and_(
                column>=start_id,
                column<end_id
            )
        else:
            return column>=start_id

    q = session.query(
                column, 
                func.row_number().\
                        over(order_by=column).\
                        label('rownum')
                ).\
                from_self(column)
    if windowsize > 1:
        q = q.filter(sqlalchemy.text("rownum %% %d=1" % windowsize))

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)

def windowQuery(q, column, windowsize):
    """"Break a Query into windows on a given column."""

    for whereclause in column_windows(
                                        q.session, 
                                        column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row


if __name__ == '__main__':
    from sqlalchemy import Column, Integer, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base
    import random

    Base = declarative_base()

    class Widget(Base):
        __tablename__ = 'widget'
        id = Column(Integer, primary_key=True)
        data = Column(Integer)

    e = create_engine('postgresql://scott:tiger@localhost/test', echo='debug')

    Base.metadata.drop_all(e)
    Base.metadata.create_all(e)

    # get some random list of unique values
    data = set([random.randint(1, 1000000) for i in xrange(10000)])

    s = Session(e)
    s.add_all([Widget(id=i, data=j) for i, j in enumerate(data)])
    s.commit()

    q = s.query(Widget)

    for widget in windowed_query(q, Widget.data, 1000):
        print("data:", widget.data)
