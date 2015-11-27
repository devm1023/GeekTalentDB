import conf
from analyticsdb import *
import sys
from logger import Logger
from windowquery import splitProcess, processDb
from datetime import datetime


def addCareerSteps(jobid, fromtitle, totitle, minstart):
    import locale
    locale.setlocale(locale.LC_COLLATE, 'en_US.UTF-8')

    fromtitle_xfrm = locale.strxfrm(fromtitle)
    totitle_xfrm = None
    if totitle:
        totitle_xfrm = locale.strxfrm(totitle)
    
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Experience.liprofileId, Experience.nrmTitle) \
            .filter(Experience.nrmTitle >= fromtitle,
                    Experience.start != None)
    if totitle is not None:
        q = q.filter(Experience.nrmTitle < totitle)
    if minstart is not None:
        q = q.filter(Experience.start >= minstart)
    q = q.distinct()

    def addRecord(rec):
        liprofileId, nrmTitle = rec
        nrmTitle_xfrm = locale.strxfrm(nrmTitle)
        if nrmTitle_xfrm < fromtitle_xfrm or \
           (totitle and nrmTitle_xfrm >= totitle_xfrm):
            raise RuntimeError('mismatch: {0:s} not between {1:s} and {2:s}' \
                               .format(repr(nrmTitle), repr(fromtitle),
                                       repr(totitle)))
        
        q = andb.query(Experience.titlePrefix, Experience.nrmTitle) \
                .filter(Experience.liprofileId == liprofileId,
                        Experience.start != None)
        if minstart is not None:
            q = q.filter(Experience.start >= minstart)
        q = q.order_by(Experience.start)
        alltitles = q.all()
        
        titles = []
        for prefix, title in alltitles:
            if title and (prefix, title) not in titles:
                titles.append((prefix, title))
        if not titles:
            return

        titles = [(None, None)]*2 + titles + [(None, None)]*2
        for i in range(len(titles)-3):
            triple = titles[i:i+3]
            title0 = next(t for t in triple if t[1] is not None)
            title0_xfrm = locale.strxfrm(title0[1])
            if title0_xfrm < fromtitle_xfrm or \
               (totitle and title0_xfrm >= totitle_xfrm):
                continue
            andb.addCareerStep(*[i for t in triple for i in t])

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
andb.execute('ALTER SEQUENCE career_step_id_seq RESTART WITH 1;')

q = andb.query(Title.nrmName) \
        .filter(Title.experienceCount > 0)
splitProcess(q, addCareerSteps, batchsize,
             njobs=njobs, logger=logger,
             args=[minstart],
             workdir='jobs', prefix='build_careersteps')
