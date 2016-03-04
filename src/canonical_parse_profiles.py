from datoindb import *
import canonicaldb as nf
from windowquery import splitProcess, processDb
from phraseextract import PhraseExtractor
from textnormalization import tokenizedSkill
from sqldb import dictFromRow
from sqlalchemy import and_
import conf
import sys
import csv
from datetime import datetime, timedelta
from logger import Logger
import re
import langdetect
from langdetect.lang_detect_exception import LangDetectException
import argparse

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()
skillbuttonpatt = re.compile(r'See ([0-9]+\+|Less)')
truncatedpatt = re.compile(r'.*\.\.\.')
connectionspatt = re.compile(r'[^0-9]*([0-9]+)[^0-9]*')
countryLanguages = {
    'United Kingdom' : 'en',
    'Netherlands'    : 'nl',
    'Nederland'      : 'nl',
}

def makeName(name, firstName, lastName):
    if name:
        return name
    name = ' '.join(s for s in [firstName, lastName] if s)
    if not name:
        return None
    return name

def makeLocation(city, country):
    location = ', '.join(s for s in [city, country] if s)
    if not location:
        location = None
    return location

def makeDateTime(ts, offset=0):
    if ts:
        result = timestamp0 + timedelta(milliseconds=ts+offset)
    else:
        result = None
    return result

def makeDateRange(tsFrom, tsTo, offset=0):
    start = makeDateTime(tsFrom, offset=offset)
    end = makeDateTime(tsTo, offset=offset)
    if start is not None and end is not None and end < start:
        start = None
        end = None
    if start is None:
        end = None

    return start, end

def makeGeo(longitude, latitude):
    if longitude is None or latitude is None:
        return None
    return 'POINT({0:f} {1:f})'.format(longitude, latitude)

def makeList(val):
    if not val:
        return []
    else:
        return val


def parseLIProfiles(jobid, fromid, toid, fromTs, toTs, byIndexedOn,
                    skillextractor):
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
        profiledict = dictFromRow(liprofile)
        profiledict['datoinId'] = profiledict.pop('id')
        profiledict['indexedOn'] \
            = makeDateTime(profiledict.pop('indexedOn', None))
        profiledict['crawledOn'] \
            = makeDateTime(profiledict.pop('crawledDate', None))
        profiledict['name'] = makeName(profiledict.get('name', None),
                                       profiledict.get('firstName', None),
                                       profiledict.get('lastName', None))
        profiledict['location'] \
            = makeLocation(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profileUrl', None)
        profiledict['pictureUrl'] = profiledict.pop('profilePictureUrl', None)

        connections = profiledict.pop('connections', None)
        if connections is not None:
            connections = connectionspatt.match(connections)
        if connections is not None:
            connections = int(connections.group(1))
        profiledict['connections'] = connections

        skills = makeList(profiledict.pop('categories', None))
        skills = [s for s in skills if not skillbuttonpatt.match(s) \
                  and not truncatedpatt.match(s)]
        profiledict['skills'] = skills

        if not profiledict['experiences']:
            profiledict['experiences'] = []
        for experiencedict in profiledict['experiences']:
            experiencedict['title'] = experiencedict.pop('name', None)
            experiencedict['location'] \
                = makeLocation(experiencedict.pop('city', None),
                               experiencedict.pop('country', None))
            experiencedict['start'], experiencedict['end'] \
                = makeDateRange(experiencedict.pop('dateFrom', None),
                                experiencedict.pop('dateTo', None))

        if not profiledict['educations']:
            profiledict['educations'] = []
        for educationdict in profiledict['educations']:
            educationdict['institute'] = educationdict.pop('name', None)
            educationdict['subject'] = educationdict.pop('area', None)
            educationdict['start'], educationdict['end'] \
                = makeDateRange(educationdict.pop('dateFrom', None),
                                educationdict.pop('dateTo', None))
            
        
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
        
        cndb.addLIProfile(profiledict)

    processDb(q, addLIProfile, cndb, logger=logger)

def parseINProfiles(jobid, fromid, toid, fromTs, toTs, byIndexedOn,
                    skillextractor):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    cndb = nf.CanonicalDB(url=conf.CANONICAL_DB)

    q = dtdb.query(INProfile).filter(INProfile.id >= fromid)
    if byIndexedOn:
        q = q.filter(INProfile.indexedOn >= fromTs,
                     INProfile.indexedOn < toTs)
    else:
        q = q.filter(INProfile.crawledDate >= fromTs,
                     INProfile.crawledDate < toTs)
                                     
    if toid is not None:
        q = q.filter(INProfile.id < toid)

    def addINProfile(inprofile):
        profiledict = dictFromRow(inprofile)
        profiledict['datoinId'] = profiledict.pop('id')
        profiledict['indexedOn'] \
            = makeDateTime(profiledict.pop('indexedOn', None))
        profiledict['crawledOn'] \
            = makeDateTime(profiledict.pop('crawledDate', None))
        profiledict['name'] = makeName(profiledict.get('name', None),
                                       profiledict.get('firstName', None),
                                       profiledict.get('lastName', None))
        profiledict['updatedOn'] \
            = makeDateTime(profiledict.pop('profileUpdatedDate', None))
        profiledict['location'] \
            = makeLocation(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profileUrl', None)
        profiledict['skills'] = []

        if not profiledict['experiences']:
            profiledict['experiences'] = []
        for experiencedict in profiledict['experiences']:
            experiencedict['title'] = experiencedict.pop('name', None)
            experiencedict['location'] \
                = makeLocation(experiencedict.pop('city', None),
                               experiencedict.pop('country', None))
            experiencedict['start'], experiencedict['end'] \
                = makeDateRange(experiencedict.pop('dateFrom', None),
                                experiencedict.pop('dateTo', None))

        if not profiledict['educations']:
            profiledict['educations'] = []
        for educationdict in profiledict['educations']:
            educationdict['institute'] = educationdict.pop('name', None)
            educationdict['subject'] = educationdict.pop('area', None)
            educationdict['start'], educationdict['end'] \
                = makeDateRange(educationdict.pop('dateFrom', None),
                                educationdict.pop('dateTo', None))

        if not profiledict['certifications']:
            profiledict['certifications'] = []
        for certificationdict in profiledict['certifications']:
            certificationdict['start'], certificationdict['end'] \
                = makeDateRange(certificationdict.pop('dateFrom', None),
                                certificationdict.pop('dateTo', None))
            
        
        # determine language

        profiletexts = [profiledict['title'], profiledict['description']]
        profiletexts.extend(profiledict['skills'])
        for inexperience in profiledict['experiences']:
            profiletexts.append(inexperience['title'])
            profiletexts.append(inexperience['description'])
        for ineducation in profiledict['educations']:
            profiletexts.append(ineducation['degree'])
            profiletexts.append(ineducation['subject'])
            profiletexts.append(ineducation['description'])
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        if inprofile.country not in countryLanguages.keys():
            if language not in countryLanguages.values():
                return
        elif language not in countryLanguages.values():
            language = countryLanguages[inprofile.country]
            
        profiledict['language'] = language


        # extract skills

        if skillextractor is not None and language == 'en':
            text = ' '.join(s for s in [profiledict['title'],
                                        profiledict['description'],
                                        profiledict['additionalInformation']] \
                            if s)
            profiledict['skills'] = list(set(skillextractor(text)))
            for inexperience in profiledict['experiences']:
                text = ' '.join(s for s in [inexperience['title'],
                                            inexperience['description']] if s)
                inexperience['skills'] = list(set(skillextractor(text)))

        
        # add profile
        
        cndb.addINProfile(profiledict)

    processDb(q, addINProfile, cndb, logger=logger)

def parseUWProfiles(jobid, fromid, toid, fromTs, toTs, byIndexedOn,
                    skillextractor):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    cndb = nf.CanonicalDB(url=conf.CANONICAL_DB)

    q = dtdb.query(UWProfile).filter(UWProfile.id >= fromid)
    if byIndexedOn:
        q = q.filter(UWProfile.indexedOn >= fromTs,
                     UWProfile.indexedOn < toTs)
    else:
        q = q.filter(UWProfile.crawledDate >= fromTs,
                     UWProfile.crawledDate < toTs)
                                     
    if toid is not None:
        q = q.filter(UWProfile.id < toid)

    def addUWProfile(uwprofile):
        profiledict = dictFromRow(uwprofile)
        profiledict['datoinId'] = profiledict.pop('id')
        profiledict['indexedOn'] \
            = makeDateTime(profiledict.pop('indexedOn', None))
        profiledict['crawledOn'] \
            = makeDateTime(profiledict.pop('crawledDate', None))
        profiledict['name'] = makeName(profiledict.get('name', None),
                                       profiledict.get('firstName', None),
                                       profiledict.get('lastName', None))
        profiledict['location'] \
            = makeLocation(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profileUrl', None)
        profiledict['skills'] = makeList(profiledict.pop('categories', None))
        
        if not profiledict['experiences']:
            profiledict['experiences'] = []
        for experiencedict in profiledict['experiences']:
            experiencedict['title'] = experiencedict.pop('name', None)
            experiencedict['location'] \
                = makeLocation(experiencedict.pop('city', None),
                               experiencedict.pop('country', None))
            experiencedict['start'], experiencedict['end'] \
                = makeDateRange(experiencedict.pop('dateFrom', None),
                                experiencedict.pop('dateTo', None))

        if not profiledict['educations']:
            profiledict['educations'] = []
        for educationdict in profiledict['educations']:
            educationdict['institute'] = educationdict.pop('name', None)
            educationdict['subject'] = educationdict.pop('area', None)
            educationdict['start'], educationdict['end'] \
                = makeDateRange(educationdict.pop('dateFrom', None),
                                educationdict.pop('dateTo', None))
                            
        # determine language
        profiledict['language'] = 'en'
        
        # add profile
        cndb.addUWProfile(profiledict)

    processDb(q, addUWProfile, cndb, logger=logger)

def parseMUProfiles(jobid, fromid, toid, fromTs, toTs, byIndexedOn,
                    skillextractor):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    cndb = nf.CanonicalDB(url=conf.CANONICAL_DB)

    q = dtdb.query(MUProfile).filter(MUProfile.id >= fromid)
    if byIndexedOn:
        q = q.filter(MUProfile.indexedOn >= fromTs,
                     MUProfile.indexedOn < toTs)
    else:
        q = q.filter(MUProfile.crawledDate >= fromTs,
                     MUProfile.crawledDate < toTs)
                                     
    if toid is not None:
        q = q.filter(MUProfile.id < toid)

    def addMUProfile(muprofile):
        profiledict = dictFromRow(muprofile)
        profiledict['datoinId'] = profiledict.pop('id')
        profiledict['indexedOn'] \
            = makeDateTime(profiledict.pop('indexedOn', None))
        profiledict['crawledOn'] \
            = makeDateTime(profiledict.pop('crawledDate', None))
        profiledict['pictureId'] \
            = profiledict.pop('profilePictureId', None)
        profiledict['pictureUrl'] \
            = profiledict.pop('profilePictureUrl', None)
        profiledict['hqPictureUrl'] \
            = profiledict.pop('profileHQPictureUrl', None)
        profiledict['thumbPictureUrl'] \
            = profiledict.pop('profileThumbPictureUrl', None)
        profiledict['geo'] = makeGeo(profiledict.pop('longitude', None),
                                     profiledict.pop('latitude', None))
        profiledict['skills'] = makeList(profiledict.pop('categories', None))
        
        if not profiledict['groups']:
            profiledict['groups'] = []
        for groupdict in profiledict['groups']:
            groupdict['createdOn'] \
                = makeDateTime(groupdict.pop('createdDate', None))
            groupdict['hqPictureUrl'] = groupdict.pop('HQPictureUrl', None)
            groupdict['geo'] = makeGeo(groupdict.pop('longitude', None),
                                       groupdict.pop('latitude', None))
            groupdict['skills'] = makeList(groupdict.pop('categories', None))

        if not profiledict['events']:
            profiledict['events'] = []
        for eventdict in profiledict['events']:
            eventdict['createdOn'] \
                = makeDateTime(eventdict.pop('createdDate', None))
            eventdict['time'] = makeDateTime(eventdict.get('time', None))
            eventdict['geo'] = makeGeo(eventdict.pop('longitude', None),
                                       eventdict.pop('latitude', None))

        profiledict['comments'] = profiledict.pop('comments', None)
        if not profiledict['comments']:
            profiledict['comments'] = []
        for commentdict in profiledict['comments']:
            commentdict['createdOn'] \
                = makeDateTime(commentdict.pop('createdDate', None))
                        
        # determine language
        profiledict['language'] = 'en'        
        
        # add profile
        cndb.addMUProfile(profiledict)

    processDb(q, addMUProfile, cndb, logger=logger)


def parseGHProfiles(jobid, fromid, toid, fromTs, toTs, byIndexedOn,
                    skillextractor):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    cndb = nf.CanonicalDB(url=conf.CANONICAL_DB)

    q = dtdb.query(GHProfile).filter(GHProfile.id >= fromid)
    if byIndexedOn:
        q = q.filter(GHProfile.indexedOn >= fromTs,
                     GHProfile.indexedOn < toTs)
    else:
        q = q.filter(GHProfile.crawledDate >= fromTs,
                     GHProfile.crawledDate < toTs)
                                     
    if toid is not None:
        q = q.filter(GHProfile.id < toid)

    def addGHProfile(ghprofile):
        profiledict = dictFromRow(ghprofile)
        profiledict['datoinId'] = profiledict.pop('id')
        profiledict['indexedOn'] \
            = makeDateTime(profiledict.pop('indexedOn', None))
        profiledict['crawledOn'] \
            = makeDateTime(profiledict.pop('crawledDate', None))
        profiledict['createdOn'] \
            = makeDateTime(profiledict.pop('createdDate', None))
        profiledict['location'] \
            = makeLocation(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profileUrl', None)
        profiledict['pictureUrl'] = profiledict.pop('profilePictureUrl', None)

        if not profiledict['repositories']:
            profiledict['repositories'] = []
        for repositorydict in profiledict['repositories']:
            repositorydict['createdOn'] \
                = makeDateTime(repositorydict.pop('createdDate', None))
            repositorydict['pushedOn'] \
                = makeDateTime(repositorydict.pop('pushedDate', None))
            
        # determine language            
        profiledict['language'] = 'en'

        # add profile        
        cndb.addGHProfile(profiledict)

    processDb(q, addGHProfile, cndb, logger=logger)
    
    
def parseProfiles(fromTs, toTs, fromid, sourceId, byIndexedOn, skillextractor):
    logger = Logger(sys.stdout)
    if sourceId is None:
        parseProfiles(fromTs, toTs, fromid, 'linkedin', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'indeed', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'upwork', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'meetup', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'github', byIndexedOn,
                      skillextractor)
        return
    elif sourceId == 'linkedin':
        logger.log('Parsing LinkedIn profiles.\n')
        table = LIProfile
        parsefunc = parseLIProfiles
        prefix = 'canonical_parse_linkedin'
    elif sourceId == 'indeed':
        logger.log('Parsing Indeed profiles.\n')
        table = INProfile
        parsefunc = parseINProfiles
        prefix = 'canonical_parse_indeed'
    elif sourceId == 'upwork':
        logger.log('Parsing Upwork profiles.\n')
        table = UWProfile
        parsefunc = parseUWProfiles
        prefix = 'canonical_parse_upwork'
    elif sourceId == 'meetup':
        logger.log('Parsing Meetup profiles.\n')
        table = MUProfile
        parsefunc = parseMUProfiles
        prefix = 'canonical_parse_meetup'
    elif sourceId == 'github':
        logger.log('Parsing GitHub profiles.\n')
        table = GHProfile
        parsefunc = parseGHProfiles
        prefix = 'canonical_parse_github'
    else:
        raise ValueError('Invalid source type.')
    
    dtdb = DatoinDB(url=conf.DATOIN_DB)

    query = dtdb.query(table.id)
    if byIndexedOn:
        query = query.filter(table.indexedOn >= fromTs,
                             table.indexedOn < toTs)
    else:
        query = query.filter(table.crawledDate >= fromTs,
                             table.crawledDate < toTs)
    if fromid is not None:
        query = query.filter(table.id >= fromid)

    splitProcess(query, parsefunc, batchsize,
                 njobs=njobs, args=[fromTs, toTs, byIndexedOn, skillextractor],
                 logger=logger, workdir='jobs', prefix=prefix)
    

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('njobs', help='Number of parallel jobs.', type=int)
    parser.add_argument('batchsize', help='Number of rows per batch.', type=int)
    parser.add_argument('--from-date', help=
                        'Only process profiles crawled or indexed on or after\n'
                        'this date. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-date', help=
                        'Only process profiles crawled or indexed before\n'
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--by-index-date', help=
                        'Indicates that the dates specified with --fromdate and\n'
                        '--todate are index dates. Otherwise they are interpreted\n'
                        'as crawl dates.',
                        action='store_true')
    parser.add_argument('--from-id', help=
                        'Start processing from this datoin ID. Useful for\n'
                        'crash recovery.')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed', 'upwork', 'meetup',
                                 'github'],
                        help=
                        'Source type to process. If not specified all sources are\n'
                        'processed.')
    parser.add_argument('--skills', help=
                        'Name of a CSV file holding skill tags. Only needed when\n'
                        'processing Indeed CVs.')
    args = parser.parse_args()
    
    njobs = max(args.njobs, 1)
    batchsize = args.batchsize
    try:
        fromdate = datetime.strptime(args.from_date, '%Y-%m-%d')
        if not args.to_date:
            todate = datetime.now()
        else:
            todate = datetime.strptime(args.to_date, '%Y-%m-%d')
    except ValueError:
        sys.stderr.write('Invalid date format.\n')
        exit(1)
    byIndexedOn = bool(args.by_index_date)
    fromid = args.from_id
    skillfile = args.skills
    sourceId = args.source

    fromTs = int((fromdate - timestamp0).total_seconds())*1000
    toTs   = int((todate   - timestamp0).total_seconds())*1000

    skillextractor = None
    if skillfile is not None:
        skills = []
        with open(skillfile, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if row:
                    skills.append(row[0])
        tokenize = lambda x: tokenizedSkill('en', x)
        skillextractor = PhraseExtractor(skills, tokenize=tokenize)
        del skills
    
    parseProfiles(fromTs, toTs, fromid, sourceId, byIndexedOn, skillextractor)
