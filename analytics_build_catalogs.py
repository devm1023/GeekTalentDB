import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from sqlalchemy.sql.expression import literal_column
from geoalchemy2.functions import ST_AsText
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


def addSkills(jobid, fromskill, toskill):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Skill.nrmName, Skill.name, func.count(Skill.profileId)) \
            .filter(Skill.nrmName >= fromskill)
    if toskill is not None:
        q = q.filter(Skill.nrmName < toskill)
    q = q.group_by(Skill.nrmName, Skill.name).order_by(Skill.nrmName)

    def addProfileSkill(rec):
        nrmName, bestname, liprofileCount = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : bestname,
            'liprofileCount'  : liprofileCount,
            'experienceCount' : 0,
        }, analyticsdb.Skill)

    processDb(entities(q), addProfileSkill, andb, logger=logger)

    q = cndb.query(func.count(ExperienceSkill.experienceId), Skill.nrmName) \
            .join(Skill) \
            .filter(Skill.nrmName >= fromskill)
    if toskill is not None:
        q = q.filter(Skill.nrmName < toskill)
    q = q.group_by(Skill.nrmName)

    def addExperienceSkill(rec):
        experienceCount, nrmName = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'experienceCount' : experienceCount,
        }, analyticsdb.Skill)

    processDb(q, addExperienceSkill, andb, logger=logger)


def addTitles(jobid, fromtitle, totitle):
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

    def addTitle(rec):
        nrmName, name, liprofileCount, experienceCount = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'liprofileCount'  : liprofileCount,
            'experienceCount' : experienceCount,
            }, analyticsdb.Title)
    
    processDb(entities2(q), addTitle, andb, logger=logger)


def addCompanies(jobid, fromcompany, tocompany):
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

    def addCompany(rec):
        nrmName, name, liprofileCount, experienceCount = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'liprofileCount'  : liprofileCount,
            'experienceCount' : experienceCount,
            }, analyticsdb.Company)
    
    processDb(entities2(q), addCompany, andb, logger=logger)

def addSectors(jobid, fromsector, tosector):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile.nrmSector, LIProfile.sector, \
                   func.count(LIProfile.id)) \
            .filter(LIProfile.nrmSector != None) \
            .filter(LIProfile.nrmSector >= fromsector)
    if tosector is not None:
        q = q.filter(LIProfile.nrmSector < tosector)
    q = q.distinct().order_by(LIProfile.nrmSector)

    def addSector(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'count'           : count,
        }, analyticsdb.Sector)

    processDb(q, addSector, andb, logger=logger) 


def addLocations(jobid, fromplaceid, toplaceid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Location.placeId, Location.name, ST_AsText(Location.geo)) \
            .filter(Location.placeId != None) \
            .filter(Location.placeId >= fromplaceid)
    if toplaceid is not None:
        q = q.filter(Location.placeId < toplaceid)
    q = q.distinct().order_by(Location.placeId)

    def addLocation(rec):
        from copy import deepcopy
        placeId, name, geo = rec
        andb.addFromDict({
            'placeId'         : placeId,
            'name'            : name,
            'geo'             : geo,
        }, analyticsdb.Location)

    processDb(q, addLocation, andb, logger=logger) 


def addInstitutes(jobid, frominstitute, toinstitute):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Education.nrmInstitute, Education.institute,
                   func.count(Education.id)) \
            .filter(Education.nrmInstitute >= frominstitute)
    if toinstitute is not None:
        q = q.filter(Education.nrmInstitute < toinstitute)
    q = q.group_by(Education.nrmInstitute, Education.institute) \
         .order_by(Education.nrmInstitute)

    def addInstitute(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'count'           : count,
            }, analyticsdb.Institute)
    
    processDb(entities(q), addInstitute, andb, logger=logger)


def addDegrees(jobid, fromdegree, todegree):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Education.nrmDegree, Education.degree,
                   func.count(Education.id)) \
            .filter(Education.nrmDegree >= fromdegree)
    if todegree is not None:
        q = q.filter(Education.nrmDegree < todegree)
    q = q.group_by(Education.nrmDegree, Education.degree) \
         .order_by(Education.nrmDegree)

    def addDegree(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'count'           : count,
            }, analyticsdb.Degree)
    
    processDb(entities(q), addDegree, andb, logger=logger)


def addSubjects(jobid, fromsubject, tosubject):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Education.nrmSubject, Education.subject,
                   func.count(Education.id)) \
            .filter(Education.nrmSubject >= fromsubject)
    if tosubject is not None:
        q = q.filter(Education.nrmSubject < tosubject)
    q = q.group_by(Education.nrmSubject, Education.subject) \
         .order_by(Education.nrmSubject)

    def addSubject(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'count'           : count,
            }, analyticsdb.Subject)
    
    processDb(entities(q), addSubject, andb, logger=logger)


cndb = CanonicalDB(conf.CANONICAL_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    catalog = None
    startval = None
    if len(sys.argv) > 3:
        catalog = sys.argv[3]
        if catalog not in ['skills', 'titles', 'sectors', 'companies',
                           'locations', 'institutes', 'degrees', 'subjects']:
            raise ValueError('Invalid catalog string')
    if len(sys.argv) > 4:
        startval = sys.argv[4]
except ValueError:
    logger.log('usage: python3 build_catalogs.py <njobs> <batchsize> '
               '[(skills | titles | sectors | companies | locations | '
               'institutes | degrees | subjects) [<start-value>]]\n')
    exit(1)

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

if catalog is None or catalog == 'sectors':
    logger.log('\nBuilding sectors catalog.\n')
    q = cndb.query(LIProfile.nrmSector).filter(LIProfile.nrmTitle != None)
    splitProcess(q, addSectors, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_sectors')

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
    q = cndb.query(Location.placeId).filter(Location.placeId != None)
    if startval:
        q = q.filter(Location.placeId >= startval)
    splitProcess(q, addLocations, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_locations')

if catalog is None or catalog == 'institutes':
    logger.log('\nBuilding institutes catalog.\n')
    q = cndb.query(Education.nrmInstitute).filter(Education.nrmInstitute != None)
    if startval:
        q = q.filter(Education.nrmInstitute >= startval)
    splitProcess(q, addInstitutes, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_institutes')

if catalog is None or catalog == 'degrees':
    logger.log('\nBuilding degrees catalog.\n')
    q = cndb.query(Education.nrmDegree).filter(Education.nrmDegree != None)
    if startval:
        q = q.filter(Education.nrmDegree >= startval)
    splitProcess(q, addDegrees, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_degrees')

if catalog is None or catalog == 'subjects':
    logger.log('\nBuilding subjects catalog.\n')
    q = cndb.query(Education.nrmSubject).filter(Education.nrmSubject != None)
    if startval:
        q = q.filter(Education.nrmSubject >= startval)
    splitProcess(q, addSubjects, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_subjects')
