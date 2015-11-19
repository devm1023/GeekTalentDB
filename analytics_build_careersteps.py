import conf
from analyticsdb import *
import sys
from logger import Logger
from windowquery import splitProcess, processDb
from datetime import datetime


def addCareerSteps(jobid, fromid, toid, minstart):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = andb.query(Experience.liprofileId,
                   Experience.nrmTitle) \
            .filter(Experience.nrmTitle != None,
                    Experience.start != None,
                    Experience.liprofileId >= fromid)
    if toid is not None:
        q = q.filter(Experience.liprofileId < toid)
    if minstart is not None:
        q = q.filter(Experience.start >= minstart)
    q = q.order_by(Experience.liprofileId, Experience.start)

    lastid = None
    lasttitle = None
    for liprofileId, title in q:
        if liprofileId != lastid:
            andb.commit()
            lasttitle = None
            lastid = liprofileId
        if lasttitle is not None:
            andb.addCareerStep(lasttitle, title)
        lasttitle = title
    andb.commit()
    

andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    minstart = None
    if len(sys.argv) > 3:
        minstart = datetime.strptime(sys.argv[3], '%Y-%m-%d')
except ValueError:
    logger.log('usage: python3 analytics_build_careersteps.py '
               '<njobs> <batchsize> [<min-start-date>]\n')

andb.query(CareerStep).delete()
andb.commit()

q = andb.query(LIProfile.id)
splitProcess(q, addCareerSteps, batchsize,
             njobs=njobs, logger=logger,
             args=[minstart],
             workdir='jobs', prefix='build_careersteps')
