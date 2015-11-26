import conf
from analyticsdb import *
import sys
from logger import Logger
from windowquery import splitProcess, processDb
from datetime import datetime


def addCareerSteps(jobid, fromtitle, totitle, minstart):
    import locale
    locale.setlocale(locale.LC_ALL, '')

    def str_lt(s1, s2):
        return locale.strcoll(s1, s2) > 0

    def str_ge(s1, s2):
        return not str_lt(s1, s2)

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
        if not titles:
            return

        titles = [None, None]+titles+[None, None]
        for i in range(len(titles)-3):
            triple = titles[i:i+3]
            title0 = next(t for t in triple if t is not None)
            if str_lt(title0, fromtitle) or \
               (totitle and str_ge(title0, totitle)):
                continue
            andb.addCareerStep(*triple)

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
