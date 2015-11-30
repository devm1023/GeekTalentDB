from datoindb import *
import canonicaldb as nf
from windowquery import splitProcess, processDb
from sqlalchemy import and_
import conf
import sys
from datetime import datetime, timedelta
from logger import Logger
import re
import langdetect
from langdetect.lang_detect_exception import LangDetectException

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()
skillbuttonpatt = re.compile(r'See ([0-9]+\+|Less)')

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

        skills = [s for s in liprofile.categories \
                  if not skillbuttonpatt.match(s)]
        
        profiledict = {
            'datoinId'    : liprofile.id,
            'name'        : name,
            'location'    : location,
            'title'       : liprofile.title,
            'description' : liprofile.description,
            'sector'      : liprofile.sector,
            'url'         : liprofile.profileUrl,
            'pictureUrl'  : liprofile.profilePictureUrl,
            'skills'      : skills,
            'connections' : connections,
            'indexedOn'   : indexedOn,
            'crawledOn'   : crawledOn,
            'experiences' : [],
            'educations'  : []
        }

        for experience in dtdb.query(Experience) \
                              .filter(Experience.parentId == liprofile.id):
            if experience.city and experience.country:
                location = ', '.join([experience.city, experience.country])
            elif experience.country:
                location = experience.country
            elif experience.city:
                location = experience.city
            else:
                location = None

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
                'location'       : location,
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

        
        # determine language

        profiletexts = [profiledict['title'], profiledict['description']]
        profiletexts.extend(profiledict['skills'])
        for experience in profiledict['experiences']:
            profiletexts.append(experience['title'])
            profiletexts.append(experience['description'])
        for education in profiledict['educations']:
            profiletexts.append(education['degree'])
            profiletexts.append(education['subject'])
            profiletexts.append(education['description'])
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        profiledict['language'] = language


        # filter profiles
        
        if liprofile.country != 'United Kingdom' and language != 'en':
            return

        
        # add profile
        
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


dtdb = DatoinDB(url=conf.DATOIN_DB)
logger = Logger(sys.stdout)

query = dtdb.query(LIProfile.id) \
            .filter(LIProfile.country == 'United Kingdom',
                    LIProfile.crawledDate >= fromTs,
                    LIProfile.crawledDate < toTs)
if fromid is not None:
    query = query.filter(LIProfile.id >= fromid)

splitProcess(query, parseProfiles, batchsize,
             njobs=njobs, args=[fromTs, toTs], logger=logger,
             workdir='jobs', prefix='canonical_parse_linkedin')
