import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import splitProcess, processDb


def processLocations(jobid, fromlocation, tolocation, fromdate, todate,
                     byIndexedOn):
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    if byIndexedOn:
        q1 = cndb.query(LIProfile.nrmLocation.label('nrmloc')) \
                 .filter(LIProfile.indexedOn >= fromdate,
                         LIProfile.indexedOn < todate,
                         LIProfile.nrmLocation >= fromlocation)
        q2 = cndb.query(Experience.nrmLocation.label('nrmloc')) \
                 .join(LIProfile) \
                 .filter(LIProfile.indexedOn >= fromdate,
                         LIProfile.indexedOn < todate,
                         Experience.nrmLocation >= fromlocation)
    else:
        q1 = cndb.query(LIProfile.nrmLocation.label('nrmloc')) \
                 .filter(LIProfile.crawledOn >= fromdate,
                         LIProfile.crawledOn < todate,
                         LIProfile.nrmLocation >= fromlocation)
        q2 = cndb.query(Experience.nrmLocation.label('nrmloc')) \
                 .join(LIProfile) \
                 .filter(LIProfile.crawledOn >= fromdate,
                         LIProfile.crawledOn < todate,
                         Experience.nrmLocation >= fromlocation)
    if tolocation is not None:
        q1 = q1.filter(LIProfile.nrmLocation < tolocation)
        q2 = q2.filter(Experience.nrmLocation < tolocation)
    q = q1.union(q2).order_by('nrmloc')

    def addLocation(rec):
        cndb.addLocation(rec[0], logger)

    processDb(q, addLocation, cndb, logger=logger)            
        

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
    todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
    byIndexedOn = False
    if len(sys.argv) > 5 and sys.argv[5] == '--by-index-date':
        byIndexedOn = True
        del sys.argv[5]
    fromlocation = None
    if len(sys.argv) > 5:
        fromlocation = sys.argv[5]
except (ValueError, IndexError):
    print('usage: python3 canonical_geolookup_linkedin.py <njobs> <batchsize> '
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
    
q = q1.union(q2).order_by('nrmloc')
splitProcess(q, processLocations, batchsize,
             njobs=njobs, args=[fromdate, todate, byIndexedOn], logger=logger,
             workdir='jobs', prefix='geoupdate_linkedin')

