import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from sqldb import dictFromRow
from windowquery import splitProcess, processDb


def addLIProfiles(jobid, fromid, toid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile, Location) \
            .outerjoin(Location, LIProfile.nrmLocation == Location.nrmName) \
            .filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    def addLIProfile(rec):
        liprofile, location = rec
        liprofiledict = dictFromRow(liprofile)
        
        if 'title' in liprofiledict:
            liprofiledict['rawTitle'] = liprofiledict.pop('title')
        if 'company' in liprofiledict:
            liprofiledict['rawCompany'] = liprofiledict.pop('company')
            
        if liprofiledict.get('experiences', None) is not None:
            for experience in liprofiledict['experiences']:                
                if 'title' in experience:
                    experience['rawTitle'] = experience.pop('title')
                if 'company' in experience:
                    experience['rawCompany'] = experience.pop('company')
                if experience.get('skills', None) is not None:
                    experience['skills'] \
                        = [s['skill']['nrmName'] for s in experience['skills']]

        if liprofiledict.get('educations', None) is not None:
            for education in liprofiledict['educations']:                
                if 'institute' in education:
                    education['rawInstitute'] = education.pop('institute')
                if 'degree' in education:
                    education['rawDegree'] = education.pop('degree')
                if 'subject' in education:
                    education['rawSubject'] = education.pop('subject')
                    
        if location is not None:
            liprofiledict['placeId'] = location.placeId
            
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

