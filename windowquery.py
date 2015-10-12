import sqlalchemy
from sqlalchemy import and_, func, text
import numpy as np

def windows(session, column, windowsize, filter=None):
    """Generate a series of WHERE clauses which break a given column into windows.

    """
    subq = session.query(column.label('col'))
    if filter is not None:
        subq = subq.filter(filter)
    subq = subq.distinct().subquery()

    q = session.query(subq.c.col,
                      func.row_number().over(order_by='col').label('rownum')) \
               .from_self(subq.c.col) \
               .filter(text('rownum %% %d=1' % windowsize)) \
               .order_by(subq.c.col)
    a = None
    for b, in q:
        if a is not None:
            yield a, b
        a = b
    yield a, None


def windowQuery(q, column, windowsize=10000, filter=None):
    """"Break a query into windows on a given column.

    Args:
      q (query object): The query to split into windows.
      column (Column object): The column on which to split.
      windowsize (int, optional): The number of distinct values of `column` in
        one window. Defaults to 10000.
      filter (filter object or None, optional): Filter to apply to `q` as well
        as the query which determines the possible values of `column`.
    
    Yields:
      The same rows that `q` would yield.

    """

    if filter is not None:
        q = q.filter(filter)
    for a, b in windows(q.session, column, windowsize, filter=filter):
        if b is not None:
            whereclause = and_(column >= a, column < b)
        else:
            whereclause = column >= a
        for row in q.filter(whereclause).order_by(column):
            yield row
            
