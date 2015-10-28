import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from sqlalchemy.sql.expression import literal_column
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

def entities2(q):
    currententity = None
    maxprofilecount = 0
    maxexperiencecount = 0
    profilecount = 0
    experiencecount = 0
    bestprofilename = None
    bestexperiencename = None
    for nrmName, name, tpe, count in q:
        if nrmName != currententity:
            if bestexperiencename:
                yield currententity, bestexperiencename, \
                    profilecount, experiencecount
            elif bestprofilename:
                yield currententity, bestprofilename, \
                    profilecount, experiencecount
            maxprofilecount = 0
            profilecount = 0
            bestprofilename = None
            maxexperiencecount = 0
            experiencecount = 0
            bestexperiencename = None
            currententity = nrmName
        if tpe == 1:
            if count > maxprofilecount:
                bestprofilename = name
                maxprofilecount = count
            profilecount += count
        if tpe == 2:
            if count > maxexperiencecount:
                bestexperiencename = name
                maxexperiencecount = count
            experiencecount += count
    
    if bestexperiencename:
        yield currententity, bestexperiencename, profilecount, experiencecount
    elif bestprofilename:
        yield currententity, bestprofilename, profilecount, experiencecount


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
    
    q1 = cndb.query(LIProfile.nrmTitle.label('nrm'),
                    LIProfile.parsedTitle.label('parsed'),
                    literal_column('1').label('type'),
                    func.count(LIProfile.id)) \
            .filter(LIProfile.nrmTitle >= fromtitle)
    q2 = cndb.query(Experience.nrmTitle.label('nrm'),
                    Experience.parsedTitle.label('parsed'),
                    literal_column('2').label('type'),
                    func.count(Experience.id)) \
            .filter(Experience.nrmTitle >= fromtitle)
    if totitle is not None:
        q1 = q1.filter(LIProfile.nrmTitle < totitle)
        q2 = q2.filter(Experience.nrmTitle < totitle)
    q1 = q1.group_by('nrm', 'parsed', 'type')
    q2 = q2.group_by('nrm', 'parsed', 'type')
    q = q1.union(q2).order_by('nrm')

    processDb(entities2(q), lambda r: andb.addTitle(*r), andb, logger=logger)


def addCompanies(fromcompany, tocompany):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q1 = cndb.query(LIProfile.nrmCompany.label('nrm'),
                    LIProfile.company.label('raw'),
                    literal_column('1').label('type'),
                    func.count(LIProfile.id)) \
            .filter(LIProfile.nrmCompany >= fromcompany)
    q2 = cndb.query(Experience.nrmCompany.label('nrm'),
                    Experience.company.label('raw'),
                    literal_column('2').label('type'),
                    func.count(Experience.id)) \
            .filter(Experience.nrmCompany >= fromcompany)
    if tocompany is not None:
        q1 = q1.filter(LIProfile.nrmCompany < tocompany)
        q2 = q2.filter(Experience.nrmCompany < tocompany)
    q1 = q1.group_by('nrm', 'raw', 'type')
    q2 = q2.group_by('nrm', 'raw', 'type')
    q = q1.union(q2).order_by('nrm')

    processDb(entities2(q), lambda r: andb.addCompany(*r), andb, logger=logger)


def addLocations(fromlocation, tolocation):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    print(repr(fromlocation))
    
    q = cndb.query(Location.placeId, Location.name, Location.geo) \
            .filter(Location.nrmName != None) \
            .filter(Location.nrmName >= fromlocation)
    if tolocation is not None:
        q = q.filter(Location.nrmName < tolocation)
    q = q.distinct().order_by(Location.nrmName)

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
    q1 = cndb.query(LIProfile.nrmTitle).filter(LIProfile.nrmTitle != None)
    q2 = cndb.query(Experience.nrmTitle).filter(Experience.nrmTitle != None)
    if startval:
        q1 = q1.filter(LIProfile.nrmTitle >= startval)
        q2 = q2.filter(Experience.nrmTitle >= startval)
    q = q1.union(q2)
    splitProcess(q, addTitles, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_titles')

if catalog is None or catalog == 'companies':
    logger.log('\nBuilding companies catalog.\n')
    q1 = cndb.query(LIProfile.nrmCompany).filter(LIProfile.nrmCompany != None)
    q2 = cndb.query(Experience.nrmCompany).filter(Experience.nrmCompany != None)
    if startval:
        q1 = q1.filter(LIProfile.nrmCompany >= startval)
        q2 = q2.filter(Experience.nrmCompany >= startval)
    q = q1.union(q2)
    splitProcess(q, addCompanies, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_companies')

if catalog is None or catalog == 'locations':
    logger.log('\nBuilding locations catalog.\n')
    q = cndb.query(Location.nrmName).filter(Location.nrmName != None)
    if startval:
        q = q.filter(Location.nrmName >= startval)
    splitProcess(q, addLocations, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_locations')
