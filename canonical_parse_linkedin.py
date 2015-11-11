from datoindb import *
import canonicaldb as nf
from windowquery import splitProcess, processDb
from sqlalchemy import and_
import conf
import sys
from datetime import datetime, timedelta
from logger import Logger

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()


def parseProfiles(jobid, fromid, toid, fromTs, toTs):
    batchsize = 50
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    cndb = nf.CanonicalDB(url=conf.CANONICAL_DB)

    q = dtdb.query(LIProfile).filter(LIProfile.crawledDate >= fromTs,
                                     LIProfile.crawledDate < toTs,
                                     LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    def addLIProfile(liprofile):
        if liprofile.name:
            name = liprofile.name
        elif liprofile.firstName and liprofile.lastName:
            name = ' '.join([liprofile.firstName, liprofile.lastName])
        elif liprofile.lastName:
            name = liprofile.lastName
        elif liprofile.firstName:
            name = liprofile.firstName
        else:
            return

        if liprofile.city and liprofile.country:
            location = ', '.join([liprofile.city, liprofile.country])
        elif liprofile.country:
            location = liprofile.country
        elif liprofile.city:
            location = liprofile.city
        else:
            location = None

        if liprofile.indexedOn:
            indexedOn = timestamp0 + timedelta(milliseconds=liprofile.indexedOn)
        else:
            indexedOn = None

        if liprofile.crawledDate:
            crawledOn = timestamp0 \
                        + timedelta(milliseconds=liprofile.crawledDate)
        else:
            crawledOn = None
            
        connections = None
        try:
            connections = int(liprofile.connections)
        except (TypeError, ValueError):
            pass
        
        profiledict = {
            'datoinId'    : liprofile.id,
            'name'        : name,
            'location'    : location,
            'title'       : liprofile.title,
            'description' : liprofile.description,
            'sector'      : liprofile.sector,
            'url'         : liprofile.profileUrl,
            'pictureUrl'  : liprofile.profilePictureUrl,
            'skills'      : liprofile.categories,
            'connections' : connections,
            'indexedOn'   : indexedOn,
            'crawledOn'   : crawledOn,
            'experiences' : [],
            'educations'  : []
        }

        for experience in dtdb.query(Experience) \
                              .filter(Experience.parentId == liprofile.id):
            if experience.dateFrom:
                start = timestamp0 + timedelta(milliseconds=experience.dateFrom)
            else:
                start = None
            if start is not None and experience.dateTo:
                end = timestamp0 + timedelta(milliseconds=experience.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            if experience.indexedOn:
                indexedOn = timestamp0 + \
                            timedelta(milliseconds=experience.indexedOn)
            else:
                indexedOn = None

            experiencedict = {
                'datoinId'       : experience.id,
                'title'          : experience.name,
                'company'        : experience.company,
                'start'          : start,
                'end'            : end,
                'description'    : experience.description,
                'indexedOn'      : indexedOn,
                }
            profiledict['experiences'].append(experiencedict)

        for education in dtdb.query(Education) \
                             .filter(Education.parentId == liprofile.id):
            if education.dateFrom:
                start = timestamp0 + timedelta(milliseconds=education.dateFrom)
            else:
                start = None
            if start is not None and education.dateTo:
                end = timestamp0 + timedelta(milliseconds=education.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            if education.indexedOn:
                indexedOn = timestamp0 + \
                            timedelta(milliseconds=education.indexedOn)
            else:
                indexedOn = None

            educationdict = {
                'datoinId'       : education.id,
                'institute'      : education.institute,
                'degree'         : education.degree,
                'subject'        : education.area,
                'start'          : start,
                'end'            : end,
                'description'    : education.description,
                'indexedOn'      : indexedOn,
                }
            profiledict['educations'].append(educationdict)

        cndb.addLIProfile(profiledict, now)

    processDb(q, addLIProfile, cndb, logger=logger)


# process arguments

njobs = int(sys.argv[1])
batchsize = int(sys.argv[2])
fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
fromid = None
if len(sys.argv) > 5:
    fromid = sys.argv[5]

fromTs = int((fromdate - timestamp0).total_seconds())*1000
toTs   = int((todate   - timestamp0).total_seconds())*1000

filter = and_(LIProfile.crawledDate >= fromTs, LIProfile.crawledDate < toTs)
if fromid is not None:
    filter = and_(filter, LIProfile.id >= fromid)

dtdb = DatoinDB(url=conf.DATOIN_DB)
logger = Logger(sys.stdout)

query = dtdb.query(LIProfile.id).filter(filter)
splitProcess(query, parseProfiles, batchsize,
             njobs=njobs, args=[fromTs, toTs], logger=logger,
             workdir='jobs', prefix='canonical_parse_linkedin')
