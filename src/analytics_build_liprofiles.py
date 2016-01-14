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
        if 'sector' in liprofiledict:
            liprofiledict['rawSector'] = liprofiledict.pop('sector')
            
        if liprofiledict.get('experiences', None) is not None:
            for liexperience in liprofiledict['experiences']:
                placeId = None
                if liexperience.get('nrmLocation', None) is not None:
                    placeId = cndb.query(Location.placeId) \
                                  .filter(Location.nrmName ==
                                          liexperience['nrmLocation']) \
                                  .first()[0]
                liexperience['placeId'] = placeId
                if 'title' in liexperience:
                    liexperience['rawTitle'] = liexperience.pop('title')
                if 'company' in liexperience:
                    liexperience['rawCompany'] = liexperience.pop('company')
                if liexperience.get('skills', None) is not None:
                    liexperience['skills'] \
                        = [s['skill']['nrmName'] \
                           for s in liexperience['skills']]

        if liprofiledict.get('educations', None) is not None:
            for lieducation in liprofiledict['educations']:                
                if 'institute' in lieducation:
                    lieducation['rawInstitute'] = lieducation.pop('institute')
                if 'degree' in lieducation:
                    lieducation['rawDegree'] = lieducation.pop('degree')
                if 'subject' in lieducation:
                    lieducation['rawSubject'] = lieducation.pop('subject')
                    
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

