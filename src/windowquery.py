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
from math import ceil

def windows(query, windowsize=None, nwindows=None):
    """Generate a series of intervals break a given column into windows.

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
    
            
def splitProcess(query, f, batchsize, njobs=1, args=[], 
                 logger=Logger(None), workdir='.', prefix=None):
    """Apply a function to ranges of distinct values returned by a query.

    Args:
      query (sqlalchemy Query object): The query returning the values to split
        on. `query` must have exactly one column.
      f (callable): The function to apply to the ranges. The first two arguments
        passed to `f` are the lower (inclusive) and upper (exclusive) limits for
        the values returned by `query`. The second argument may be ``None``,
        which means no upper limit. The third argument passed to `f` is the
        ID of the parallel job.
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
        pargs = []
        fromid_batch = None
        toid_batch = None
        fromrow_batch = None
        torow_batch = None
        parallelProcess = ParallelFunction(f,
                                           batchsize=1,
                                           workdir=workdir,
                                           prefix=prefix,
                                           tries=1)
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
                parallelProcess(pargs)
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
            parallelProcess(pargs)
            endtime = datetime.now()
            _log_batchend(logger, starttime, endtime, firststart,
                          fromrow_batch, nrows+1, nrows)


def processDb(q, f, db, batchsize=1000, logger=Logger(None),
              msg='processDb: {0:d} records processed.\n'):
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
