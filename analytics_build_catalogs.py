import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import splitProcess, processDb


def entities(q):
    currententity = None
    maxcount = 0
    profilecount = 0
    bestname = None
    for nrmName, name, count in q:
        if nrmName != currententity:
            if bestname:
                yield currententity, bestname, profilecount
            maxcount = 0
            profilecount = 0
            bestname = None
            currententity = nrmName
        if count > maxcount:
            bestname = name
            maxcount = count
        profilecount += count
    
    if bestname:
        yield currententity, bestname, profilecount
        

def addSkills(fromskill, toskill):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Skill.nrmName, Skill.name, func.count(Skill.profileId)) \
            .filter(Skill.nrmName >= fromskill)
    if toskill is not None:
        q = q.filter(Skill.nrmName < toskill)
    q = q.group_by(Skill.nrmName, Skill.name).order_by(Skill.nrmName)

    def addSkill(rec):
        nrmName, bestname, profileCount = rec
        experienceCount = cndb.query(ExperienceSkill.experienceId) \
                              .join(Skill) \
                              .filter(Skill.nrmName == nrmName) \
                              .count()
        andb.addSkill(nrmName, bestname, profileCount, experienceCount)

    processDb(entities(q), addSkill, andb, logger=logger)


def addTitles(fromtitle, totitle):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile.nrmTitle, LIProfile.parsedTitle,
                   func.count(LIProfile.id)) \
            .filter(LIProfile.nrmTitle >= fromtitle)
    if totitle is not None:
        q = q.filter(LIProfile.nrmTitle < totitle)
    q = q.group_by(LIProfile.nrmTitle, LIProfile.parsedTitle) \
         .order_by(LIProfile.nrmTitle)

    def addTitle(rec):
        nrmName, bestname, profileCount = rec
        experienceCount = cndb.query(Experience) \
                              .filter(Experience.nrmTitle == nrmName) \
                              .count()
        andb.addTitle(nrmName, bestname, profileCount, experienceCount)
        
    processDb(entities(q), addTitle, andb, logger=logger)


def addCompanies(fromcompany, tocompany):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Experience.nrmCompany, Experience.company,
                   func.count(Experience.id)) \
            .filter(Experience.nrmCompany >= fromcompany)
    if tocompany is not None:
        q = q.filter(Experience.nrmCompany < tocompany)
    q = q.group_by(Experience.nrmCompany, Experience.company) \
         .order_by(Experience.nrmCompany)

    def addCompany(rec):
        nrmName, bestname, experienceCount = rec
        liprofileCount = cndb.query(LIProfile) \
                             .filter(LIProfile.nrmCompany == nrmName) \
                             .count()
        andb.addCompany(nrmName, bestname, liprofileCount, experienceCount)
        
    processDb(entities(q), addCompany, andb, logger=logger)


def addLocations(fromlocation, tolocation):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(Location.placeId, Location.name, Location.geo) \
            .distinct().order_by(Location.nrmName)

    processDb(q, lambda rec: andb.addLocation(*rec), andb, logger=logger) 


cndb = CanonicalDB(conf.CANONICAL_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    catalog = None
    startval = None
    if len(sys.argv) > 3:
        catalog = sys.argv[3]
        if catalog not in ['skills', 'titles', 'companies', 'locations']:
            raise ValueError('Invalid catalog string')
    if len(sys.argv) > 4:
        startval = sys.argv[4]
except ValueError:
    logger.log('usage: python3 build_catalogs.py <njobs> <batchsize> '
               '[(skills | titles) [<start-value>]]\n')

if catalog is None or catalog == 'skills':
    logger.log('\nBuilding skills catalog.\n')
    q = cndb.query(Skill.nrmName).filter(Skill.nrmName != None)
    if startval:
        q = q.filter(Skill.nrmName >= startval)
    splitProcess(q, addSkills, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_skills')

if catalog is None or catalog == 'titles':
    logger.log('\nBuilding titles catalog.\n')
    q = cndb.query(LIProfile.nrmTitle).filter(LIProfile.nrmTitle != None)
    if startval:
        q = q.filter(LIProfile.nrmTitle >= startval)
    splitProcess(q, addTitles, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_titles')

if catalog is None or catalog == 'companies':
    logger.log('\nBuilding companies catalog.\n')
    q = cndb.query(LIProfile.nrmCompany).filter(LIProfile.nrmCompany != None)
    if startval:
        q = q.filter(LIProfile.nrmCompany >= startval)
    splitProcess(q, addCompanies, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_companies')

if catalog is None or catalog == 'locations':
    logger.log('\nBuilding locations catalog.\n')
    q = cndb.query(Location.nrmName)
    if startval:
        q = q.filter(Location.nrmName >= startval)
    splitProcess(q, addLocations, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_locations')
