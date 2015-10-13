from logger import Logger
from windowquery import windows, count
from parallelize import ParallelFunction
from datetime import datetime

def processTable(session, column, f, batchsize, njobs=1, args=[], 
                 filter=None, logger=Logger(None), workdir='.', prefix=None):
    """Apply a function to ranges of distinct values of a table column.

    Args:
      session (sqlalchemy session object): The database session.
      column (sqlalchemy Column object): The column to partition.
      f (callable): The function to apply to the ranges. The first two arguments
        of `f` must be the lower (inclusive) and upper (exclusive) limits for
        the values `column` to process. The second argument may be ``None``,
        which means no upper limit. The return value of `f` must be a tuple
        holding the number of processed records and the `column` value of the
        last processed record.
      batchsize (int): The size (i.e. number of distinct `column` values) of the 
        intervals that are passed to `f`.
      njobs (int): Number of parallel processes to start. Defaults to 1.
      args (list): Additional arguments to pass to `f`.
      filter (sqlalchemy filter expression or None): Filter to apply to the
        table containing `column` before selecting distinct values of `column`.
      logger (Logger object): Object to write log messages to.
      workdir (str): Working directory for parallel jobs.
      prefix (str): Prefix for creating temporary files.

    """    
    if njobs <= 1:
        recordcount = 0
        for fromrec, torec in windows(session, column, batchsize, filter):
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
        for fromrec, torec in windows(session, column, batchsize, filter):
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
