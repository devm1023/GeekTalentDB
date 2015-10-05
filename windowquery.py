import sqlalchemy
from sqlalchemy import and_, func, text

def windows(q, column, windowsize):
    """Generate a series of WHERE clauses which break a given column into windows.

    """
    q = q.from_self(column).distinct() \
         .add_columns(func.row_number().over(order_by=column) \
                      .label('__rownum__')) \
         .from_self(column, '__rownum__')
    if windowsize > 1:
        q = q.filter(text('__rownum__ %% %d=1' % windowsize))
    q = q.order_by('__rownum__')

    intervals = [id for id, row in q]
    if intervals:
        for start_id, end_id in zip(intervals[:-1], intervals[1:]):
            yield and_(column >= start_id, column < end_id)
        yield column >= intervals[-1]

def windowQuery(q, column, windowsize=1000, values=None):
    """"Break a query into windows on a given column.

    Args:
      q (query object): The query to split into windows.
      column (Column object): The column on which to split.
      windowsize (int, optional): The number of distinct values of `column` in
        one window. Defaults to 1000.
      values (query object or None, optional): Auxiliary query for obtaining
        the values for `column` on which to construct the windows. To improve
        performance you can use a simpler query than `q` here, e.g. by removing
        joins. `values` must return the same values for `column` as `q`. Defaults
        to ``None``, in which case the values are obtained from `q`.
        
    """

    if values is None:
        values = q
    for whereclause in windows(values, column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row
            
