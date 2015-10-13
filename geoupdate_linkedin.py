import conf
from normalformdb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from processtable import processTable


def processLocations(fromlocation, tolocation, fromdate, todate):
    nfdb = NormalFormDB(url=conf.NF_WRITE_DB)
    logger = Logger(sys.stdout)
    batchsize = 10
    q = nfdb.query(LIProfile.nrmLocation) \
            .filter(LIProfile.indexedOn >= fromdate,
                    LIProfile.indexedOn < todate,
                    LIProfile.nrmLocation >= fromlocation)
    if tolocation is not None:
        q = q.filter(LIProfile.nrmLocation < tolocation)

    recordcount = 0
    lastname = None
    for nrmLocation, in q:
        recordcount += 1
        lastname = nrmLocation
        nfdb.addLocation(nrmLocation)
        if recordcount % batchsize == 0:
            nfdb.commit()
            logger.log('Batch: {0:d} records processed.\n'.format(recordcount))
    if recordcount % batchsize != 0:
        nfdb.commit()
        logger.log('Batch: {0:d} records processed.\n'.format(recordcount))

    return recordcount, lastname
            
        

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
    filter = and_(filter, LIProfile.nrmLocation > fromlocation)

nfdb = NormalFormDB(url=conf.NF_WRITE_DB)
logger = Logger(sys.stdout)

totalrecords = nfdb.query(LIProfile.id).filter(filter).count()
logger.log('{0:d} records found.\n'.format(totalrecords))

processTable(nfdb.session, LIProfile.nrmLocation, processLocations, batchsize,
             njobs=njobs, args=[fromdate, todate],
             filter = filter, logger=logger,
             workdir='geojobs', prefix='geoupdate')

