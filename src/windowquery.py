"""Functions for partitioning queries and parallel processing of the results.

The most common pattern for processing the results of a large query in
parallel uses the ``split_process`` and ``process_db`` functions. It goes
as follows::

  from windowquery import split_process, process_db
  from logger import Logger
  from mydb import MyDB    # database session class
  from mydb import MyTable # some table in mydb
  from myotherdb import MyOtherDB    # another database session class
  from myotherdb import MyOtherTable # some table in myotherdb


  db1 = MyDB(...)
  logger = Logger()


  def process_rows(jobid, fromid, toid, *args):
      # can't use global object here because of parallelisation
      db1 = MyDB(...) 
      logger = Logger()

      db2 = MyOtherDB(...)

      # construct a query with the same filters as the one in the global
      # scope, but also restrict the `id` column to the range given by
      # `fromid` and `toid`. You can use `args` to pass filter
      # parameters from the global scope.
      q = db1.query(...) \
             .filter(...) \
             .filter(MyTable.id >= fromid)
      if toid is not None:
          q = q.filter(MyTable.id < toid)
      
      def process_row(row):
          newrow = MyOtherTable(...) # construct from row
          db2.add(newrow)

      process_db(q, process_row, db2, logger=logger, batchsize=100)
      
  
  # query to select the IDs to process.
  q = db1.query(MyTable.id) \
         .filter(...)
  batchsize = 1000
  # extra arguments passed to process_rows
  args = [...] 
  split_process(q, process_rows, batchsize, njobs=2, args=args,
                logger=logger, workdir='myjobs', prefix='myprefix')



Created by: Martin Wiebusch
Last modified: 2016-08-08 MW

"""

__all__ = [
    'windows',
    'window_query',
    'split_process',
    'process_db',
]

import sqlalchemy
from sqlalchemy import and_, func, text
from logger import Logger
from datetime import datetime
from math import ceil


def windows(query, windowsize=None, nwindows=None):
    """Generate a series of intervals to break a given column into windows.

    Args:
      query (sqlalchemy Query object): The query returning the values to split
        on. `query` must have exactly one column.
      windowsize (int, optional): The number of distinct values of `query` in
        each interval. Defaults to ``None``.
      nwindows (int, optional): The desired number of windows. Defaults to
        ``None``

    Note:
      One of the arguments `windowsize` or `nwindows` must be an integer number.
      The other one must be ``None``.

    Yields:
      a: The lower (inclusive) bound of the interval.
      b: The upper (exclusive) bound of the interval. May be ``None``, which
        means no upper bound.
      ra: The row number of record `a`
      rb: The row number of record `b`
      nr: The total number of records.

    """
    if (windowsize is None and nwindows is None) or \
       (windowsize is not None and nwindows is not None) or \
       (windowsize is not None and windowsize < 1) or \
       (nwindows is not None and nwindows < 1):
        raise ValueError('Invalid values for `windowsize` and `nwindows`.')

    columns = list(query.statement.inner_columns)
    if len(columns) != 1:
        raise ValueError('Query must have exactly one column.')
    column = columns[0]

    nrows = query.distinct().count()
    if nrows < 1:
        raise StopIteration()
    if windowsize is None:
        windowsize = ceil(nrows/nwindows)
    subq = query.order_by(column).distinct().subquery()
    q = query.session.query(subq,
                            func.row_number().over() \
                            .label('__rownum__')) \
                     .from_self(column, '__rownum__') \
                     .filter(text('__rownum__ %% %d=1' % windowsize))

    a = None
    ra = None
    for b, rb in q:
        if a is not None:
            yield a, b, ra, rb, nrows
        a = b
        ra = rb
    yield a, None, ra, nrows+1, nrows


def window_query(q, column, windowsize=10000, values=None):
    """"Break a query with many results into windows to save RAM.

    Args:
      q (query object): The query to split into windows.
      column (Column object): The column on which to split.
      windowsize (int, optional): The number of distinct values of `column` in
        one window. Defaults to 10000.
      values (query object or None, optional): A query returning the values
        to split on. Defaults to ``None``, in which case ``q.from_self(column)``
        is used.

    Yields:
      The same rows that `q` would yield.

    """

    if values is None:
        wq = q.from_self(column)
    else:
        wq = values

    for a, b, ra, rb, nr in windows(wq, windowsize):
        if b is not None:
            whereclause = and_(column >= a, column < b)
        else:
            whereclause = column >= a
        for row in q.filter(whereclause).order_by(column):
            yield row


def _log_batchstart(logger, starttime, fromid):
    logger.log('Starting batch at {0:s}.\n' \
               .format(starttime.strftime('%Y-%m-%d %H:%M:%S%z')))
    logger.log('First record: {0:s}\n'.format(repr(fromid)))


def _log_batchend(logger, starttime, endtime, firststart,
                  fromrow, torow, nrows):
    logger.log('Completed batch {0:s} at {1:f} records/sec.\n' \
               .format(endtime.strftime('%Y-%m-%d %H:%M:%S%z'),
                       (torow-fromrow)/ \
                       (endtime-starttime).total_seconds()))
    x = (torow-1)/nrows
    etf = firststart + (endtime-firststart)/x
    logger.log('{0:d} of {1:d} records processed ({2:d}%).\n'\
               .format(torow-1, nrows, round(x*100)))
    logger.log('Estimated finish: {0:s}.\n' \
               .format(etf.strftime('%Y-%m-%d %H:%M:%S%z')))


def split_process(query, f, batchsize, njobs=1, args=[],
                 logger=Logger(None), workdir='.', prefix=None):
    """Apply a function to ranges of distinct values returned by a query.

    This function partitions the values returned by `query` into intervals of
    at most `batchsize` distinct values. It then executes the function `f`
    in parallel, passing to it a job ID and the lower and upper bounds of the
    intervals.

    Example:
      Assume that `query` returns the letters 'a' to 'z' (in any order and
      possibly with repetitions). Then

          split_process(query, f, 10, njobs=2, args=['foo', 'bar'])

      will first execute
    
          f(1, 'a', 'k', 'foo', 'bar')
          f(2, 'k', 'u', 'foo', 'bar')

      in parallel and then execute

          f(1, 'u', None, 'foo', 'bar')

    If `logger` is supplied messages indicating the progress and the 
    estimated time to finish will be logged.

    Args:
      query (sqlalchemy Query object): The query returning the values to split
        on. `query` must have exactly one column, but the returned values do
        not need to be distinct or sorted.
      f (callable): The function to apply to the ranges. `f` will be called
        in parallel as ``f(i, lb, ub, *args)``, where `i` is the index of the
        parallel job, `lb` is the lower (inclusive) bound of a subrange of
        the values returned by `query`, `ub` is the upper (exclusive) bound
        of the subrange (or ``None`` for the last batch), and `args` are
        additional arguments supplied in `args`.
      batchsize (int): The size (i.e. number of distinct `query` values) of the
        intervals that are passed to `f`.
      njobs (int, optional): Number of parallel processes to start. Defaults to
        1.
      args (list, optional): Additional arguments to pass to `f`. Defaults to
        ``[]``.
      logger (Logger object, optional): Object to write log messages to.
        Defaults to ``Logger(None)``.
      workdir (str, optional): Working directory for parallel jobs. Defaults to
        ``'.'``.
      prefix (str or None, optional): Prefix for creating temporary files.
        Defaults to ``None``, in which case a UUID is used.

    """
    firststart = datetime.now()
    if njobs <= 1:
        for fromid, toid, fromrow, torow, nrows in windows(query, batchsize):
            starttime = datetime.now()
            _log_batchstart(logger, starttime, fromid)
            f(*([0, fromid, toid]+args))
            endtime = datetime.now()
            _log_batchend(logger, starttime, endtime, firststart,
                          fromrow, torow, nrows)
    else:
        from parallelize import ParallelFunction

        pargs = []
        fromid_batch = None
        toid_batch = None
        fromrow_batch = None
        torow_batch = None
        parallel_process = ParallelFunction(f,
                                            batchsize=1,
                                            workdir=workdir,
                                            prefix=prefix,
                                            tries=1,
                                            cleanup=1,
                                            append=False)
        for fromid, toid, fromrow, torow, nrows in windows(query, batchsize):
            pargs.append([len(pargs), fromid, toid]+args)
            if fromid_batch is None or fromid < fromid_batch:
                fromid_batch = fromid
            if toid is not None and (toid_batch is None or toid > toid_batch):
                toid_batch = toid
            if fromrow_batch is None or fromrow < fromrow_batch:
                fromrow_batch = fromrow
            if torow_batch is None or torow > torow_batch:
                torow_batch = torow

            if len(pargs) == njobs:
                starttime = datetime.now()
                _log_batchstart(logger, starttime, fromid_batch)
                parallel_process(pargs)
                parallel_process.append = True
                endtime = datetime.now()
                _log_batchend(logger, starttime, endtime, firststart,
                              fromrow_batch, torow_batch, nrows)

                pargs = []
                fromid_batch = None
                toid_batch = None
                fromrow_batch = None
                torow_batch = None

        if pargs:
            starttime = datetime.now()
            _log_batchstart(logger, starttime, fromid_batch)
            parallel_process(pargs)
            endtime = datetime.now()
            _log_batchend(logger, starttime, endtime, firststart,
                          fromrow_batch, nrows+1, nrows)


def process_db(q, f, db, batchsize=1000, logger=Logger(None),
               msg='process_db: {0:d} records processed.\n'):
    """Apply a function to all rows returned by query and commit in bulk.

    This function applies `f` to all rows returned by `q` and calls 
    ``db.commit()`` after processing `batchsize` rows. Progress information
    will be logged if `logger` is supplied.

    Args:
      q (query object): Query yielding rows to process.
      f (callable): Function to process rows. It will be called as
        ``f(row)`` where `row` is a row returned by `q` (usually a tuple).
      db (session object): The database session to commit. The function `f`
        should insert or update rows in `db`.
      batchsize (int, optional): Number of processed rows after which
        ``db.commit()`` is called.
      logger (Logger object, optional): Object to write log messages to.
      msg (str, optional): Format string for constructing log messages. The
        messages will be constructed with ``msg.format(n)`` where `n` is the
        number of rows processed so far.

    """
    recordcount = 0
    for rec in q:
        f(rec)
        recordcount += 1
        if recordcount % batchsize == 0:
            db.commit()
            logger.log(msg.format(recordcount))
    if recordcount % batchsize != 0:
        db.commit()
        logger.log(msg.format(recordcount))


def collapse(q, on=1):
    """Collapse the results of a query over one or more of its columns.

    Example:
      Assume that `q` yields the following rows:

        ('foo', 1 'a')
        ('foo', 1 'b')
        ('foo', 2 'c')
        ('bar', 1 'd')
        ('bar', 1 'e')
        ('bar', 1 'f')
        ('bar', 2 'g')
        ('bar', 2 'h')

      Then ``collapse(q, on=2)`` will yield
      
        ('foo', 1, ['a', 'b'])
        ('foo', 2, ['c'])
        ('bar', 1, ['d', 'e', 'f'])
        ('bar', 2, ['g', 'h'])

    Args:
      q (query object): The query to collapse.
      on (int, optional): The number of columns (from the left) on which to
        collapse.

    """
    currentid = None
    rows = []
    for row in q:
        newid = row[:on]
        fields = row[on:]
        if currentid is None:
            currentid = newid
        if newid != currentid:
            yield currentid + (rows,)
            rows = []
            currentid = newid
        rows.append(fields)
    if currentid is not None:
        yield currentid + (rows,)
