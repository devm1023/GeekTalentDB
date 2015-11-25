import conf
from analyticsdb import *
import sys
from logger import Logger
from windowquery import splitProcess, processDb
from datetime import datetime


def addCareerSteps(jobid, fromtitle, totitle, minstart):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = andb.query(Experience.liprofileId) \
            .filter(Experience.nrmTitle >= fromtitle,
                    Experience.start != None)
    if totitle is not None:
        q = q.filter(Experience.nrmTitle < totitle)
    if minstart is not None:
        q = q.filter(Experience.start >= minstart)
    q = q.distinct()

    def addRecord(rec):
        liprofileId, = rec
        q = andb.query(Experience.nrmTitle) \
                        .filter(Experience.liprofileId == liprofileId,
                                Experience.start != None)
        if minstart is not None:
            q = q.filter(Experience.start >= minstart)
        q = q.order_by(Experience.start)
        alltitles = q.all()
        
        titles = []
        for title, in alltitles:
            if title and title not in titles:
                titles.append(title)
                
        if len(titles) >= 1:
            if titles[0] >= fromtitle and (not totitle or titles[0] < totitle):
                andb.addCareerStep(titles[0], None, None)
        if len(titles) >= 2:
            if titles[0] >= fromtitle and (not totitle or titles[0] < totitle):
                andb.addCareerStep(titles[0], titles[1], None)
        if len(titles) >= 3:
            for i in range(len(titles)-3):
                if titles[i] >= fromtitle and \
                   (not totitle or titles[i] < totitle):
                    andb.addCareerStep(titles[i], titles[i+1], titles[i+2])

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
