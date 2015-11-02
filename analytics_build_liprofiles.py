import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from sqldb import dictFromRow
from windowquery import splitProcess, processDb


def addLIProfiles(fromid, toid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile).filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    def addLIProfile(liprofile):
        liprofiledict = dictFromRow(liprofile)
        if liprofiledict.get('experiences', None) is not None:
            for experience in liprofiledict['experiences']:
                if experience.get('skills', None) is not None:
                    experience['skills'] \
                        = [s['skill']['nrmName'] for s in experience['skills']]
            
        andb.addLIProfile(liprofiledict)
        
    processDb(q, addLIProfile, andb, logger=logger)


cndb = CanonicalDB(conf.CANONICAL_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    startval = None
    if len(sys.argv) > 3:
        startval = sys.argv[3]
except ValueError:
    logger.log('usage: python3 analytics_build_liprofiles.py '
               '<njobs> <batchsize> [<start-value>]\n')

q = cndb.query(LIProfile.id)
if startval:
    q = q.filter(LIProfile.id >= startval)
splitProcess(q, addLIProfiles, batchsize,
             njobs=njobs, logger=logger,
             workdir='jobs', prefix='analytics_build_liprofiles')

