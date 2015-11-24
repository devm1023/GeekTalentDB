import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import splitProcess, processDb


def processLocations(jobid, fromlocation, tolocation, fromdate, todate):
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

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
        cndb.addLocation(rec[0])

    processDb(q, addLocation, cndb, logger=logger)            
        

njobs = int(sys.argv[1])
batchsize = int(sys.argv[2])
fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
fromlocation = None
if len(sys.argv) > 5:
    fromlocation = sys.argv[5]

filter = and_(LIProfile.crawledOn >= fromdate,
              LIProfile.crawledOn < todate)
if fromlocation is not None:
    filter = and_(filter, LIProfile.nrmLocation >= fromlocation)

cndb = CanonicalDB(url=conf.CANONICAL_DB)
logger = Logger(sys.stdout)

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
             njobs=njobs, args=[fromdate, todate], logger=logger,
             workdir='jobs', prefix='geoupdate_linkedin')

