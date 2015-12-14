import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import splitProcess, processDb
from datetime import datetime, timedelta
import time


def processLocations(jobid, fromlocation, tolocation,
                     fromdate, todate, byIndexedOn, retry, maxretry):
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    if retry:
        q = cndb.query(Location.nrmName) \
                .filter(Location.placeId == None,
                        Location.tries < maxretry)
        if fromlocation is not None:
            q = q.filter(Location.nrmName >= fromlocation)
    else:
        if byIndexedOn:
            q1 = cndb.query(LIProfile.nrmLocation.label('nrmloc')) \
                     .filter(LIProfile.indexedOn >= fromdate,
                             LIProfile.indexedOn < todate)
            q2 = cndb.query(Experience.nrmLocation.label('nrmloc')) \
                     .join(LIProfile) \
                     .filter(LIProfile.indexedOn >= fromdate,
                             LIProfile.indexedOn < todate)
        else:
            q1 = cndb.query(LIProfile.nrmLocation.label('nrmloc')) \
                     .filter(LIProfile.crawledOn >= fromdate,
                             LIProfile.crawledOn < todate)
            q2 = cndb.query(Experience.nrmLocation.label('nrmloc')) \
                     .join(LIProfile) \
                     .filter(LIProfile.crawledOn >= fromdate,
                             LIProfile.crawledOn < todate)

        q1 = q1.filter(LIProfile.nrmLocation >= fromlocation)
        q2 = q2.filter(Experience.nrmLocation >= fromlocation)
        if tolocation is not None:
            q1 = q1.filter(LIProfile.nrmLocation < tolocation)
            q2 = q2.filter(Experience.nrmLocation < tolocation)

        q = q1.union(q2)

    def addLocation(rec):
        cndb.addLocation(rec[0], retry=retry, logger=logger)
 
    processDb(q, addLocation, cndb, logger=logger)



try:
    sys.argv.pop(0)
    njobs = int(sys.argv.pop(0))
    batchsize = int(sys.argv.pop(0))
    if sys.argv[0] == '--retry':
        sys.argv.pop(0)
        retry = True
        maxretry = int(sys.argv.pop(0))
        fromdate = None
        todate = None
        byIndexedOn = False
    else:
        retry = False
        maxretry = 0
        fromdate = datetime.strptime(sys.argv.pop(0), '%Y-%m-%d')
        todate = datetime.strptime(sys.argv.pop(0), '%Y-%m-%d')
        if sys.argv[0] == '--by-index-date':
            sys.argv.pop(0)
            byIndexedOn = True
        else:
            byIndexedOn = False
    fromlocation = None
    if sys.argv:
        fromlocation = sys.argv.pop(0)
except (ValueError, IndexError):
    print('usage: python3 canonical_geolookup_linkedin.py <njobs> <batchsize> '
          '(<from-date> <to-date> [--by-index-date] | --retry <max-retries>) '
          '[<from-location>]')
    exit(1)

cndb = CanonicalDB(url=conf.CANONICAL_DB)
logger = Logger(sys.stdout)

if retry:
    q = cndb.query(Location.nrmName) \
            .filter(Location.placeId == None,
                    Location.tries < maxretry)
    if fromlocation is not None:
        q = q.filter(Location.nrmName >= fromlocation)
else:
    if byIndexedOn:
        q1 = cndb.query(LIProfile.nrmLocation.label('nrmloc')) \
                 .filter(LIProfile.indexedOn >= fromdate,
                         LIProfile.indexedOn < todate)
        q2 = cndb.query(Experience.nrmLocation.label('nrmloc')) \
                 .join(LIProfile) \
                 .filter(LIProfile.indexedOn >= fromdate,
                         LIProfile.indexedOn < todate)
    else:
        q1 = cndb.query(LIProfile.nrmLocation.label('nrmloc')) \
                 .filter(LIProfile.crawledOn >= fromdate,
                         LIProfile.crawledOn < todate)
        q2 = cndb.query(Experience.nrmLocation.label('nrmloc')) \
                 .join(LIProfile) \
                 .filter(LIProfile.crawledOn >= fromdate,
                         LIProfile.crawledOn < todate)

    if fromlocation is not None:
        q1 = q1.filter(LIProfile.nrmLocation >= fromlocation)
        q2 = q2.filter(Experience.nrmLocation >= fromlocation)

    q = q1.union(q2)

splitProcess(q, processLocations, batchsize,
             njobs=njobs, args=[fromdate, todate, byIndexedOn, retry, maxretry],
             logger=logger, workdir='jobs', prefix='geoupdate_linkedin')

# at full speed: 178 out of 1845
# with throttling: 86 out of 700
