import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import splitProcess


def processLocations(fromlocation, tolocation, fromdate, todate):
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    logger = Logger(sys.stdout)
    batchsize = 10
    q = cndb.query(LIProfile.nrmLocation) \
            .filter(LIProfile.indexedOn >= fromdate,
                    LIProfile.indexedOn < todate,
                    LIProfile.nrmLocation >= fromlocation)
    if tolocation is not None:
        q = q.filter(LIProfile.nrmLocation < tolocation)

    recordcount = 0
    for nrmLocation, in q:
        recordcount += 1
        cndb.addLocation(nrmLocation)
        if recordcount % batchsize == 0:
            cndb.commit()
            logger.log('Batch: {0:d} records processed.\n'.format(recordcount))
    if recordcount % batchsize != 0:
        cndb.commit()
        logger.log('Batch: {0:d} records processed.\n'.format(recordcount))
            
        

njobs = int(sys.argv[1])
batchsize = int(sys.argv[2])
fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
fromlocation = None
if len(sys.argv) > 5:
    fromlocation = sys.argv[5]

filter = and_(LIProfile.indexedOn >= fromdate,
              LIProfile.indexedOn < todate)
if fromlocation is not None:
    filter = and_(filter, LIProfile.nrmLocation >= fromlocation)

cndb = CanonicalDB(url=conf.CANONICAL_DB)
logger = Logger(sys.stdout)

query = cndb.query(LIProfile.nrmLocation).filter(filter)
splitProcess(query, processLocations, batchsize,
             njobs=njobs, args=[fromdate, todate], logger=logger,
             workdir='jobs', prefix='geoupdate_linkedin')

