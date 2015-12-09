import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import splitProcess, processDb
from datetime import datetime, timedelta
import time


try:
    fromdate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    todate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
    byIndexedOn = False
    if len(sys.argv) > 3 and sys.argv[3] == '--by-index-date':
        byIndexedOn = True
        del sys.argv[3]
    usedquota = 0
    if len(sys.argv) > 3:
        usedquota = int(sys.argv[3])
    fromlocation = None
    if len(sys.argv) > 4:
        fromlocation = sys.argv[4]
except (ValueError, IndexError):
    print('usage: python3 canonical_geolookup_linkedin.py '
          '<from-date> <to-date> [--by-index-date] [<from-location>]')
    exit(1)

cndb = CanonicalDB(url=conf.CANONICAL_DB)
logger = Logger(sys.stdout)

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
    
batchsize = 100
requestlimit = 15000
quotarefresh = timedelta(hours=24.1)
requestrefresh = timedelta(seconds=0.11)
failwarn = 5

q = q1.union(q2).distinct().order_by('nrmloc')
totalcount = 0
requestcount = usedquota
failcount = 0
batchstart = datetime.now()
requeststart = datetime.now()
for nrmLocation, in q:
    if requestcount >= requestlimit:
        now = datetime.now()
        t = now + quotarefresh
        if t > now:
            logger.log('Out of quota. Waiting until {0:s}.\n' \
                       .format(t.strftime('%Y-%m-%d %H:%M:%S%z')))
            time.sleep(quotarefresh.total_seconds())
        requestcount = 0
    requeststart = datetime.now()
    location, cached = cndb.addLocation(nrmLocation, logger)
    if not cached:
        requestcount += 1
        if location.placeId is None:
            failcount += 1
        else:
            failcount = 0
    cndb.commit()
    totalcount += 1
    if failcount >= failwarn:
        logger.log('WARNING: {0:d} requests failed in a row.\n' \
                   .format(failcount))
    t = requeststart + requestrefresh
    now = datetime.now()
    if not cached and t > now:
        time.sleep((t-now).total_seconds())
    if totalcount > 0 and totalcount % batchsize == 0:
        rate = batchsize/(now-batchstart).total_seconds()
        logger.log('{0:d} locations processed at {1:f} locations/sec.\n'
                   'Last location: {2:s}\n' \
                   .format(totalcount, rate, nrmLocation))
        batchstart = now

