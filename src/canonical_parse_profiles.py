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

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()
skillbuttonpatt = re.compile(r'See ([0-9]+\+|Less)')
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

def parseProfiles(fromTs, toTs, fromid, sourceId, byIndexedOn, skillextractor):
    logger = Logger(sys.stdout)
    if sourceId is None:
        parseProfiles(fromTs, toTs, fromid, 'linkedin', byIndexedOn,
                      skillextractor)
        parseProfiles(fromTs, toTs, fromid, 'indeed', byIndexedOn,
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
    try:
        sys.argv.pop(0)
        njobs = max(int(sys.argv.pop(0)), 1)
        batchsize = int(sys.argv.pop(0))
        fromdate = datetime.strptime(sys.argv.pop(0), '%Y-%m-%d')
        todate = datetime.strptime(sys.argv.pop(0), '%Y-%m-%d')

        sourceId = None
        byIndexedOn = False
        fromid = None
        skillfile = None
        while sys.argv:
            option = sys.argv.pop(0).split('=')
            if len(option) == 1:
                option = option[0]
                if option == '--by-index-date':
                    byIndexedOn = True
                else:
                    raise ValueError('Invalid command line argument.')
            elif len(option) == 2:
                value=option[1]
                option=option[0]
                if option == '--source':
                    if value in ['linkedin', 'indeed']:
                        sourceId = value
                    else:
                        raise ValueError('Invalid command line argument.')
                elif option == '--fromid':
                    fromid = value
                elif option == '--skills':
                    skillfile = value
            else:
                raise ValueError('Invalid command line argument.')
    except ValueError:
        print('python3 canonical_parse_profiles.py <njobs> <batchsize> '
              '<from-date> <to-date> [--by-index-date] '
              '[--source=<sourceid>] [--fromid=<fromid>] '
              '[--skills=<skills.csv>]')
        exit(1)

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
