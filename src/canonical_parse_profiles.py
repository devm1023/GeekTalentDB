from datoindb import *
import canonicaldb as nf
from windowquery import splitProcess, processDb
from phraseextract import PhraseExtractor
from textnormalization import tokenizedSkill
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
connectionspatt = re.compile(r'[^0-9]*([0-9]+)[^0-9]*')
countryLanguages = {
    'United Kingdom' : 'en',
    'Netherlands'    : 'nl',
    'Nederland'      : 'nl',
}


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

        connections = liprofile.connections
        if connections is not None:
            connections = connectionspatt.match(connections)
        if connections is not None:
            connections = int(connections.group(1))

        skills = []
        if liprofile.categories:
            skills = [s for s in liprofile.categories \
                      if not skillbuttonpatt.match(s)]

        groups = []
        if liprofile.groups:
            groups = list(liprofile.groups)
        
        profiledict = {
            'datoinId'    : liprofile.id,
            'name'        : name,
            'location'    : location,
            'title'       : liprofile.title,
            'description' : liprofile.description,
            'sector'      : liprofile.sector,
            'url'         : liprofile.profileUrl,
            'pictureUrl'  : liprofile.profilePictureUrl,
            'connections' : connections,
            'indexedOn'   : indexedOn,
            'crawledOn'   : crawledOn,
            'experiences' : [],
            'educations'  : [],
            'skills'      : skills,
            'groups'      : groups
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

            liexperiencedict = {
                'datoinId'       : liexperience.id,
                'title'          : liexperience.name,
                'company'        : liexperience.company,
                'location'       : location,
                'start'          : start,
                'end'            : end,
                'description'    : liexperience.description,
                }
            profiledict['experiences'].append(liexperiencedict)

        for lieducation in dtdb.query(LIEducation) \
                             .filter(LIEducation.parentId == liprofile.id):
            if lieducation.dateFrom:
                start = timestamp0 \
                        + timedelta(milliseconds=lieducation.dateFrom)
            else:
                start = None
            if start is not None and lieducation.dateTo:
                end = timestamp0 + timedelta(milliseconds=lieducation.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            lieducationdict = {
                'datoinId'       : lieducation.id,
                'institute'      : lieducation.institute,
                'degree'         : lieducation.degree,
                'subject'        : lieducation.area,
                'start'          : start,
                'end'            : end,
                'description'    : lieducation.description,
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
        if inprofile.name:
            name = inprofile.name
        elif inprofile.firstName or inprofile.lastName:
            name = ' '.join(s for s in \
                            [inprofile.firstName, inprofile.lastName] if s)
        else:
            return

        if inprofile.city or inprofile.country:
            location = ', '.join(s for s in \
                                 [inprofile.city, inprofile.country] if s)
        else:
            location = None

        if inprofile.indexedOn:
            indexedOn = timestamp0 + timedelta(milliseconds=inprofile.indexedOn)
        else:
            indexedOn = None

        if inprofile.crawledDate:
            crawledOn = timestamp0 \
                        + timedelta(milliseconds=inprofile.crawledDate)
        else:
            crawledOn = None
            
        profiledict = {
            'datoinId'    : inprofile.id,
            'name'        : name,
            'location'    : location,
            'title'       : inprofile.title,
            'description' : inprofile.description,
            'url'         : inprofile.profileUrl,
            'skills'      : [],
            'indexedOn'   : indexedOn,
            'crawledOn'   : crawledOn,
            'experiences' : [],
            'educations'  : []
        }

        for inexperience in dtdb.query(INExperience) \
                                .filter(INExperience.parentId == inprofile.id):
            if inexperience.city or inexperience.country:
                location = ', '.join(s for s in \
                                     [inexperience.city, inexperience.country] \
                                     if s)
            else:
                location = None

            if inexperience.dateFrom:
                start = timestamp0 + timedelta(milliseconds=inexperience.dateFrom)
            else:
                start = None
            if start is not None and inexperience.dateTo:
                end = timestamp0 + timedelta(milliseconds=inexperience.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            inexperiencedict = {
                'datoinId'       : inexperience.id,
                'title'          : inexperience.name,
                'company'        : inexperience.company,
                'location'       : location,
                'start'          : start,
                'end'            : end,
                'description'    : inexperience.description,
                'skills'         : []
                }
            profiledict['experiences'].append(inexperiencedict)

        for ineducation in dtdb.query(INEducation) \
                               .filter(INEducation.parentId == inprofile.id):
            if ineducation.dateFrom:
                start = timestamp0 \
                        + timedelta(milliseconds=ineducation.dateFrom)
            else:
                start = None
            if start is not None and ineducation.dateTo:
                end = timestamp0 + timedelta(milliseconds=ineducation.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            ineducationdict = {
                'datoinId'       : ineducation.id,
                'institute'      : ineducation.institute,
                'degree'         : ineducation.degree,
                'subject'        : ineducation.area,
                'start'          : start,
                'end'            : end,
                'description'    : ineducation.description,
                }
            profiledict['educations'].append(ineducationdict)

        
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
                                        profiledict['description']] if s)
            profiledict['skills'] = list(set(skillextractor(text)))
            for inexperience in profiledict['experiences']:
                text = ' '.join(s for s in [inexperience['title'],
                                            inexperience['description']] if s)
                inexperience['skills'] = list(set(skillextractor(text)))

        
        # add profile
        
        cndb.addINProfile(profiledict, now)

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
        if uwprofile.name:
            name = uwprofile.name
        else:
            name = ' '.join(s for s in \
                            [uwprofile.firstName, uwprofile.lastName] if s)
        if not name:
            return

        location = ', '.join(s for s in \
                             [uwprofile.city, uwprofile.country] if s)
        if not location:
            location = None

        if uwprofile.indexedOn:
            indexedOn = timestamp0 + timedelta(milliseconds=uwprofile.indexedOn)
        else:
            indexedOn = None

        if uwprofile.crawledDate:
            crawledOn = timestamp0 \
                        + timedelta(milliseconds=uwprofile.crawledDate)
        else:
            crawledOn = None

        skills = []
        if uwprofile.categories:
            skills = [s for s in uwprofile.categories \
                      if not skillbuttonpatt.match(s)]

        profiledict = {
            'datoinId'    : uwprofile.id,
            'name'        : name,
            'location'    : location,
            'title'       : uwprofile.title,
            'description' : uwprofile.description,
            'url'         : uwprofile.profileUrl,
            'pictureUrl'  : uwprofile.profilePictureUrl,
            'indexedOn'   : indexedOn,
            'crawledOn'   : crawledOn,
            'experiences' : [],
            'educations'  : [],
            'skills'      : skills
        }

        for uwexperience in dtdb.query(UWExperience) \
                              .filter(UWExperience.parentId == uwprofile.id):
            location = ', '.join(s for s in \
                                 [uwexperience.city, uwexperience.country] if s)
            if not location:
                location = None

            if uwexperience.dateFrom:
                start = timestamp0 \
                        + timedelta(milliseconds=uwexperience.dateFrom)
            else:
                start = None
            if start is not None and uwexperience.dateTo:
                end = timestamp0 + timedelta(milliseconds=uwexperience.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            uwexperiencedict = {
                'title'          : uwexperience.name,
                'company'        : uwexperience.company,
                'location'       : location,
                'start'          : start,
                'end'            : end,
                'description'    : uwexperience.description,
                }
            profiledict['experiences'].append(uwexperiencedict)

        for uweducation in dtdb.query(UWEducation) \
                             .filter(UWEducation.parentId == uwprofile.id):
            if uweducation.dateFrom:
                start = timestamp0 \
                        + timedelta(milliseconds=uweducation.dateFrom)
            else:
                start = None
            if start is not None and uweducation.dateTo:
                end = timestamp0 + timedelta(milliseconds=uweducation.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            uweducationdict = {
                'institute'      : uweducation.institute,
                'degree'         : uweducation.degree,
                'subject'        : uweducation.area,
                'start'          : start,
                'end'            : end,
                'description'    : uweducation.description,
                }
            profiledict['educations'].append(uweducationdict)

        
        # determine language

        profiledict['language'] = 'en'
        
        
        # add profile
        
        cndb.addUWProfile(profiledict, now)

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
        name = muprofile.name
        if not name:
            return

        if muprofile.city and muprofile.country:
            location = ', '.join(s for s in \
                                 [muprofile.city, muprofile.country] if s)
        if not location:
            location = None

        if muprofile.indexedOn:
            indexedOn = timestamp0 + timedelta(milliseconds=muprofile.indexedOn)
        else:
            indexedOn = None

        if muprofile.crawledDate:
            crawledOn = timestamp0 \
                        + timedelta(milliseconds=muprofile.crawledDate)
        else:
            crawledOn = None

        skills = []
        if muprofile.categories:
            skills = [s for s in muprofile.categories \
                      if not skillbuttonpatt.match(s)]

        links = []
        if muprofile.links:
            links = [{'type' : l.type, 'url' : l.url} for l in muprofile.links]

        profiledict = {
            'datoinId'        : muprofile.id,
            'name'            : name,
            'location'        : location,
            'status'          : muprofile.status,
            'description'     : muprofile.description,
            'url'             : muprofile.profileUrl,
            'pictureId'       : muprofile.profilePictureId,
            'pictureUrl'      : muprofile.profilePictureUrl,
            'hqPictureUrl'    : muprofile.profileHQPictureUrl,
            'thumbPictureUrl' : muprofile.profileThumbPictureUrl,
            'indexedOn'       : indexedOn,
            'crawledOn'       : crawledOn,
            'skills'          : skills,
            'links'           : links
        }

        # determine language
            
        profiledict['language'] = 'en'
        
        
        # add profile
        
        cndb.addMUProfile(profiledict, now)

    processDb(q, addMUProfile, cndb, logger=logger)

    
def parseProfiles(fromTs, toTs, fromid, sourceId, byIndexedOn, skillextractor):
    logger = Logger(sys.stdout)
    if sourceId is None:
        parseProfiles(fromTs, toTs, fromid, 'linkedin', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'indeed', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'upwork', byIndexedOn,
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
                        choices=['linkedin', 'indeed', 'upwork', 'meetup'],
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
