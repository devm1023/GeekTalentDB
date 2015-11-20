import conf
from analyticsdb import *
import sys
from logger import Logger
from windowquery import splitProcess, processDb
from datetime import datetime


def addCareerSteps(jobid, fromtitle, totitle, minstart):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = andb.query(Experience.liprofileId,
                   Experience.nrmTitle,
                   Experience.start) \
            .filter(Experience.nrmTitle >= fromtitle,
                    Experience.start != None)
    if totitle is not None:
        q = q.filter(Experience.nrmTitle < totitle)
    if minstart is not None:
        q = q.filter(Experience.start >= minstart)

    def addRecord(rec):
        liprofileId, title1, minstart = rec
        title2 = andb.query(Experience.nrmTitle) \
                     .filter(Experience.liprofileId == liprofileId,
                             Experience.start != None,
                             Experience.start > minstart) \
                     .order_by(Experience.start) \
                     .first()
        if title2:
            andb.addCareerStep(title1, title2[0])

    processDb(q, addRecord, andb, logger=logger)
    

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

q = andb.query(Title.nrmName) \
        .filter(Title.experienceCount > 0)
splitProcess(q, addCareerSteps, batchsize,
             njobs=njobs, logger=logger,
             args=[minstart],
             workdir='jobs', prefix='build_careersteps')
