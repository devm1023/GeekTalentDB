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
countryLanguages = {
    'United Kingdom' : 'en',
    'Netherlands'    : 'nl',
    'Nederland'      : 'nl',
}


def parseProfiles(jobid, fromid, toid, fromTs, toTs, byIndexedOn):
    batchsize = 50
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    cndb = nf.CanonicalDB(url=conf.CANONICAL_DB)

    q = dtdb.query(LIProfile).filter(LIProfile.id >= fromid)
    if byIndexedOn:
        q = q.filter(LIProfile.indexedOn >= fromTs,
                     LIProfile.indexedOn < toTs)
    else:
        q = q.filter(LIProfile.crawledDate >= fromTs,
                     LIProfile.crawledDate < toTs)
                                     
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

        for liexperience in dtdb.query(LIExperience) \
                              .filter(LIExperience.parentId == liprofile.id):
            if liexperience.city and liexperience.country:
                location = ', '.join([liexperience.city, liexperience.country])
            elif liexperience.country:
                location = liexperience.country
            elif liexperience.city:
                location = liexperience.city
            else:
                location = None

            if liexperience.dateFrom:
                start = timestamp0 + timedelta(milliseconds=liexperience.dateFrom)
            else:
                start = None
            if start is not None and liexperience.dateTo:
                end = timestamp0 + timedelta(milliseconds=liexperience.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            if liexperience.indexedOn:
                indexedOn = timestamp0 + \
                            timedelta(milliseconds=liexperience.indexedOn)
            else:
                indexedOn = None

            liexperiencedict = {
                'datoinId'       : liexperience.id,
                'title'          : liexperience.name,
                'company'        : liexperience.company,
                'location'       : location,
                'start'          : start,
                'end'            : end,
                'description'    : liexperience.description,
                'indexedOn'      : indexedOn,
                }
            profiledict['experiences'].append(liexperiencedict)

        for lieducation in dtdb.query(LIEducation) \
                             .filter(LIEducation.parentId == liprofile.id):
            if lieducation.dateFrom:
                start = timestamp0 + timedelta(milliseconds=lieducation.dateFrom)
            else:
                start = None
            if start is not None and lieducation.dateTo:
                end = timestamp0 + timedelta(milliseconds=lieducation.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            if lieducation.indexedOn:
                indexedOn = timestamp0 + \
                            timedelta(milliseconds=lieducation.indexedOn)
            else:
                indexedOn = None

            lieducationdict = {
                'datoinId'       : lieducation.id,
                'institute'      : lieducation.institute,
                'degree'         : lieducation.degree,
                'subject'        : lieducation.area,
                'start'          : start,
                'end'            : end,
                'description'    : lieducation.description,
                'indexedOn'      : indexedOn,
                }
            profiledict['educations'].append(lieducationdict)

        
        # determine language

        profiletexts = [profiledict['title'], profiledict['description']]
        profiletexts.extend(profiledict['skills'])
        for liexperience in profiledict['experiences']:
            profiletexts.append(liexperience['title'])
            profiletexts.append(liexperience['description'])
        for lieducation in profiledict['educations']:
            profiletexts.append(lieducation['degree'])
            profiletexts.append(lieducation['subject'])
            profiletexts.append(lieducation['description'])
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        if liprofile.country not in countryLanguages.keys():
            if language not in countryLanguages.values():
                return
        elif language not in countryLanguages.values():
            language = countryLanguages[liprofile.country]
            
        profiledict['language'] = language
        
        
        # add profile
        
        cndb.addLIProfile(profiledict, now)

    processDb(q, addLIProfile, cndb, logger=logger)


# process arguments

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
    todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
    byIndexedOn = False
    if len(sys.argv) > 5 and sys.argv[5] == '--by-index-date':
        byIndexedOn = True
        del sys.argv[5]
    fromid = None
    if len(sys.argv) > 5:
        fromid = sys.argv[5]
except (ValueError, IndexError):
    print('usage: python3 canonical_parse_linkedin.py <njobs> <batchsize> '
          '<from-date> <to-date> [--by-index-date] [<from-id>]')
    exit(1)

fromTs = int((fromdate - timestamp0).total_seconds())*1000
toTs   = int((todate   - timestamp0).total_seconds())*1000


dtdb = DatoinDB(url=conf.DATOIN_DB)
logger = Logger(sys.stdout)

query = dtdb.query(LIProfile.id)
if byIndexedOn:
    query = query.filter(LIProfile.indexedOn >= fromTs,
                         LIProfile.indexedOn < toTs)
else:
    query = query.filter(LIProfile.crawledDate >= fromTs,
                         LIProfile.crawledDate < toTs)
if fromid is not None:
    query = query.filter(LIProfile.id >= fromid)

splitProcess(query, parseProfiles, batchsize,
             njobs=njobs, args=[fromTs, toTs, byIndexedOn], logger=logger,
             workdir='jobs', prefix='canonical_parse_linkedin')
