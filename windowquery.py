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
            yield and_(column >= a, column < b)
        a = b
    yield column >= a


def partitions(q, column, nbatches):
    """Break a query into even-sized batches according to values in one column.

    Args:
      q (query object): The query to split.
      column (Column object): The column on which to split.
      nbatches (int): The number of batches.

    Yields:
      A tuples holding the lower (inclusive) and upper (exclusive)
      bounds for the values of `column`. For the last tuple the upper bound
      will be ``None``.

    """
    nbatches = max(1, nbatches)
    q = q.from_self(column).distinct()
    ntotal = q.count()
    rows = np.linspace(1, ntotal, nbatches+1, dtype=int)
    rows = list(map(repr, rows))
    columnvals = q.add_columns(func.row_number().over(order_by=column) \
                               .label('__rownum__')) \
                  .from_self(column, '__rownum__') \
                  .filter(text('__rownum__ IN ({0:s})' \
                               .format(', '.join(rows)))) \
                  .order_by('__rownum__') \
                  .all()
    for a, b in zip(columnvals[:-2], columnvals[1:-1]):
        yield a[0], b[0]
    yield (columnvals[-2][0], None)
    

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
    for whereclause in windows(q.session, column, windowsize, filter=filter):
        for row in q.filter(whereclause).order_by(column):
            yield row
            
