__all__ = [
    'windows',
    'windowQuery',
    'splitProcess'
]

import sqlalchemy
from sqlalchemy import and_, func, text
import numpy as np
from logger import Logger
from parallelize import ParallelFunction
from datetime import datetime

def windows(query, windowsize):
    """Generate a series of intervals break a given column into windows.

    Args:
      query (sqlalchemy Query object): The query returning the values to split
        on. `query` must have exactly one column.
      windowsize (int): The number of distinct values of `query` in each
        interval.

    Yields:
      a: The lower (inclusive) bound of the interval.
      b: The upper (exclusive) bound of the interval. May be ``None``, which
        means no upper bound.

    """
    columns = list(query.statement.inner_columns)
    if len(columns) != 1:
        raise ValueError('Query must have exactly one column.')
    column = columns[0]
    
    subq = query.order_by(column).distinct().subquery()
    q = query.session.query(subq,
                            func.row_number().over() \
                            .label('__rownum__')) \
                     .from_self(column) \
                     .filter(text('__rownum__ %% %d=1' % windowsize))

    a = None
    for b, in q:
        if a is not None:
            yield a, b
        a = b
    yield a, None


def windowQuery(q, column, windowsize=10000, values=None):
    """"Break a query into windows on a given column.

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
    
    for a, b in windows(wq, windowsize):
        if b is not None:
            whereclause = and_(column >= a, column < b)
        else:
            whereclause = column >= a
        for row in q.filter(whereclause).order_by(column):
            yield row


def splitProcess(query, f, batchsize, njobs=1, args=[], 
                 logger=Logger(None), workdir='.', prefix=None):
    """Apply a function to ranges of distinct values returned by a query.

    Args:
      query (sqlalchemy Query object): The query returning the values to split
        on. `query` must have exactly one column.
      f (callable): The function to apply to the ranges. The first two arguments
        of `f` must be the lower (inclusive) and upper (exclusive) limits for
        the values returned by `query`. The second argument may be ``None``,
        which means no upper limit. The return value of `f` must be a tuple
        holding the number of processed records and the id of the
        last processed record.
      batchsize (int): The size (i.e. number of distinct `query` values) of the 
        intervals that are passed to `f`.
      njobs (int, optional): Number of parallel processes to start. Defaults to
        1.
      args (list, optional): Additional arguments to pass to `f`. Defaults to
        ``[]``.
      logger (Logger object, optional): Object to write log messages to. Defaults
        to ``Logger(None)``.
      workdir (str, optional): Working directory for parallel jobs. Defaults to
        ``'.'``.
      prefix (str or None, optional): Prefix for creating temporary files.
        Defaults to ``None``, in which case a UUID is used.

    """    
    if njobs <= 1:
        recordcount = 0
        for fromrec, torec in windows(query, batchsize):
            starttime = datetime.now()
            logger.log('Starting batch at {0:s}.\n' \
                       .format(starttime.strftime('%Y-%m-%d %H:%M:%S%z')))
            nrecords, lastrec = f(*([fromrec, torec]+args))
            endtime = datetime.now()
            recordcount += nrecords
            logger.log('Completed batch {0:s} at {1:f} profiles/sec.\n' \
                       .format(endtime.strftime('%Y-%m-%d %H:%M:%S%z'),
                               nrecords/(endtime-starttime).total_seconds()))
            logger.log('{0:d} records processed.\n'.format(recordcount))
            logger.log('Last record: {0:s}\n'.format(repr(lastrec)))
    else:
        pargs = []
        parallelProcess = ParallelFunction(f,
                                           batchsize=1,
                                           workdir=workdir,
                                           prefix=prefix,
                                           tries=1)
        recordcount = 0
        for fromrec, torec in windows(query, batchsize):
            pargs.append([fromrec, torec]+args)
            if len(pargs) == njobs:
                starttime = datetime.now()
                logger.log('Starting batch at {0:s}.\n' \
                           .format(starttime.strftime('%Y-%m-%d %H:%M:%S%z')))
                results = parallelProcess(pargs)
                endtime = datetime.now()
                pargs = []
                nrecords = sum([r[0] for r in results])
                lastrec = max([r[1] for r in results])
                recordcount += nrecords
                logger.log('Completed batch {0:s} at {1:f} records/sec.\n' \
                           .format(endtime.strftime('%Y-%m-%d %H:%M:%S%z'),
                                   nrecords/(endtime-starttime).total_seconds()))
                logger.log('{0:d} records processed.\n'.format(recordcount))
                logger.log('Last record: {0:s}\n'.format(repr(lastrec)))
        if pargs:
            starttime = datetime.now()
            logger.log('Starting batch at {0:s}.\n' \
                       .format(starttime.strftime('%Y-%m-%d %H:%M:%S%z')))
            results = parallelProcess(pargs)
            endtime = datetime.now()
            nrecords = sum([r[0] for r in results])
            recordcount += nrecords
            lastrec = max([r[1] for r in results])
            logger.log('Completed batch {0:s} at {1:f} records/sec.\n' \
                       .format(endtime.strftime('%Y-%m-%d %H:%M:%S%z'),
                               nrecords/(endtime-starttime).total_seconds()))
            logger.log('{0:d} records processed.\n'.format(recordcount))
            logger.log('Last record: {0:s}\n'.format(repr(lastrec)))
