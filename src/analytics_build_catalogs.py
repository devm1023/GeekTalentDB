import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from sqlalchemy.sql.expression import literal_column
from geoalchemy2.functions import ST_AsText
from logger import Logger
import sys
from windowquery import splitProcess, processDb
from textnormalization import splitNrmName


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
    
    liq = cndb.query(LIProfileSkill.nrmName.label('nrm'),
                     LIProfileSkill.name.label('raw')) \
              .filter(LIProfileSkill.nrmName >= fromskill)
    inq = cndb.query(INProfileSkill.nrmName.label('nrm'),
                     INProfileSkill.name.label('raw')) \
              .filter(INProfileSkill.nrmName >= fromskill)
    if toskill is not None:
        liq = liq.filter(LIProfileSkill.nrmName < toskill)
        inq = inq.filter(INProfileSkill.nrmName < toskill)
    subq = liq.union_all(inq).subquery()
    q = cndb.query(subq.c.nrm, subq.c.raw, func.count()) \
            .group_by(subq.c.nrm, subq.c.raw) \
            .order_by(subq.c.nrm)

    def addProfileSkill(rec):
        nrmName, bestname, liprofileCount = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'language'          : splitNrmName(nrmName)[0],
            'name'              : bestname,
            'liprofileCount'    : liprofileCount,
            'liexperienceCount' : 0,
        }, analyticsdb.Skill)

    processDb(entities(q), addProfileSkill, andb, logger=logger)

    liq = cndb.query(LIExperienceSkill.liexperienceId,
                     LIProfileSkill.nrmName.label('nrm')) \
              .join(LIProfileSkill) \
              .filter(LIProfileSkill.nrmName >= fromskill)
    inq = cndb.query(INExperienceSkill.inexperienceId,
                     INProfileSkill.nrmName.label('nrm')) \
              .join(INProfileSkill) \
              .filter(INProfileSkill.nrmName >= fromskill)
    if toskill is not None:
        liq = liq.filter(LIProfileSkill.nrmName < toskill)
        inq = inq.filter(INProfileSkill.nrmName < toskill)
    subq = liq.union_all(inq).subquery()
    q = cndb.query(func.count(), subq.c.nrm) \
            .group_by(subq.c.nrm)

    def addLIExperienceSkill(rec):
        liexperienceCount, nrmName = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'liexperienceCount' : liexperienceCount,
        }, analyticsdb.Skill)

    processDb(q, addLIExperienceSkill, andb, logger=logger)


def addTitles(jobid, fromtitle, totitle):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq1 = cndb.query(LIProfile.nrmTitle.label('nrm'),
                      LIProfile.parsedTitle.label('parsed'),
                      literal_column('1').label('type')) \
               .filter(LIProfile.nrmTitle >= fromtitle)
    liq2 = cndb.query(LIExperience.nrmTitle.label('nrm'),
                      LIExperience.parsedTitle.label('parsed'),
                      literal_column('2').label('type')) \
               .filter(LIExperience.nrmTitle >= fromtitle)
    inq1 = cndb.query(INProfile.nrmTitle.label('nrm'),
                      INProfile.parsedTitle.label('parsed'),
                      literal_column('1').label('type')) \
               .filter(INProfile.nrmTitle >= fromtitle)
    inq2 = cndb.query(INExperience.nrmTitle.label('nrm'),
                      INExperience.parsedTitle.label('parsed'),
                      literal_column('2').label('type')) \
               .filter(INExperience.nrmTitle >= fromtitle)
    if totitle is not None:
        liq1 = liq1.filter(LIProfile.nrmTitle < totitle)
        liq2 = liq2.filter(LIExperience.nrmTitle < totitle)
        inq1 = inq1.filter(INProfile.nrmTitle < totitle)
        inq2 = inq2.filter(INExperience.nrmTitle < totitle)
    subq = liq1.union_all(liq2).union_all(inq1).union_all(inq2).subquery()
    nrmcol, parsedcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, parsedcol, typecol, func.count()) \
            .group_by(nrmcol, parsedcol, typecol) \
            .order_by(nrmcol)

    def addTitle(rec):
        nrmName, name, liprofileCount, liexperienceCount = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'language'          : splitNrmName(nrmName)[0],
            'name'              : name,
            'liprofileCount'    : liprofileCount,
            'liexperienceCount' : liexperienceCount,
            }, analyticsdb.Title)
    
    processDb(entities2(q), addTitle, andb, logger=logger)


def addCompanies(jobid, fromcompany, tocompany):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq1 = cndb.query(LIProfile.nrmCompany.label('nrm'),
                      LIProfile.company.label('raw'),
                      literal_column('1').label('type')) \
               .filter(LIProfile.nrmCompany >= fromcompany)
    liq2 = cndb.query(LIExperience.nrmCompany.label('nrm'),
                      LIExperience.company.label('raw'),
                      literal_column('2').label('type')) \
               .filter(LIExperience.nrmCompany >= fromcompany)
    inq1 = cndb.query(INProfile.nrmCompany.label('nrm'),
                      INProfile.company.label('raw'),
                      literal_column('1').label('type')) \
               .filter(INProfile.nrmCompany >= fromcompany)
    inq2 = cndb.query(INExperience.nrmCompany.label('nrm'),
                      INExperience.company.label('raw'),
                      literal_column('2').label('type')) \
               .filter(INExperience.nrmCompany >= fromcompany)
    if tocompany is not None:
        liq1 = liq1.filter(LIProfile.nrmCompany < tocompany)
        liq2 = liq2.filter(LIExperience.nrmCompany < tocompany)
        inq1 = inq1.filter(INProfile.nrmCompany < tocompany)
        inq2 = inq2.filter(INExperience.nrmCompany < tocompany)
    subq = liq1.union_all(liq2).union_all(inq1).union_all(inq2).subquery()
    nrmcol, rawcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, typecol, func.count()) \
            .group_by(nrmcol, rawcol, typecol) \
            .order_by(nrmcol)

    def addCompany(rec):
        nrmName, name, liprofileCount, liexperienceCount = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'language'          : splitNrmName(nrmName)[0],
            'name'              : name,
            'liprofileCount'    : liprofileCount,
            'liexperienceCount' : liexperienceCount,
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
    q = q.group_by(LIProfile.nrmSector, LIProfile.sector) \
         .order_by(LIProfile.nrmSector)

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
    
    liq = cndb.query(LIEducation.nrmInstitute.label('nrm'),
                     LIEducation.institute.label('raw')) \
              .filter(LIEducation.nrmInstitute >= frominstitute)
    inq = cndb.query(INEducation.nrmInstitute.label('nrm'),
                     INEducation.institute.label('raw')) \
              .filter(INEducation.nrmInstitute >= frominstitute)
    if toinstitute is not None:
        liq = liq.filter(LIEducation.nrmInstitute < toinstitute)
        inq = inq.filter(LIEducation.nrmInstitute < toinstitute)
    subq = liq.union_all(inq).subquery()
    nrmcol, rawcol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, func.count()) \
        .group_by(nrmcol, rawcol) \
        .order_by(nrmcol)

    def addInstitute(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'language'        : splitNrmName(nrmName)[0],
            'name'            : name,
            'count'           : count,
            }, analyticsdb.Institute)
    
    processDb(entities(q), addInstitute, andb, logger=logger)


def addDegrees(jobid, fromdegree, todegree):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq = cndb.query(LIEducation.nrmDegree.label('nrm'),
                     LIEducation.degree.label('raw')) \
              .filter(LIEducation.nrmDegree >= fromdegree)
    inq = cndb.query(INEducation.nrmDegree.label('nrm'),
                     INEducation.degree.label('raw')) \
              .filter(INEducation.nrmDegree >= fromdegree)
    if todegree is not None:
        liq = liq.filter(LIEducation.nrmDegree < todegree)
        inq = inq.filter(LIEducation.nrmDegree < todegree)
    subq = liq.union_all(inq).subquery()
    nrmcol, rawcol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, func.count()) \
        .group_by(nrmcol, rawcol) \
        .order_by(nrmcol)

    def addDegree(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'language'        : splitNrmName(nrmName)[0],
            'name'            : name,
            'count'           : count,
            }, analyticsdb.Degree)
    
    processDb(entities(q), addDegree, andb, logger=logger)


def addSubjects(jobid, fromsubject, tosubject):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq = cndb.query(LIEducation.nrmSubject.label('nrm'),
                     LIEducation.subject.label('raw')) \
              .filter(LIEducation.nrmSubject >= fromsubject)
    inq = cndb.query(INEducation.nrmSubject.label('nrm'),
                     INEducation.subject.label('raw')) \
              .filter(INEducation.nrmSubject >= fromsubject)
    if tosubject is not None:
        liq = liq.filter(LIEducation.nrmSubject < tosubject)
        inq = inq.filter(INEducation.nrmSubject < tosubject)
    subq = liq.union_all(inq).subquery()
    nrmcol, rawcol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, func.count()) \
        .group_by(nrmcol, rawcol) \
        .order_by(nrmcol)

    def addSubject(rec):
        nrmName, name, count = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'language'        : splitNrmName(nrmName)[0],
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
    liq = cndb.query(LIProfileSkill.nrmName) \
              .filter(LIProfileSkill.nrmName != None)
    inq = cndb.query(INProfileSkill.nrmName) \
              .filter(INProfileSkill.nrmName != None)
    if startval:
        liq = liq.filter(LIProfileSkill.nrmName >= startval)
        inq = inq.filter(INProfileSkill.nrmName >= startval)
    q = liq.union_all(inq)
    splitProcess(q, addSkills, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_skills')

if catalog is None or catalog == 'titles':
    logger.log('\nBuilding titles catalog.\n')
    liq1 = cndb.query(LIProfile.nrmTitle).filter(LIProfile.nrmTitle != None)
    liq2 = cndb.query(LIExperience.nrmTitle) \
               .filter(LIExperience.nrmTitle != None)
    inq1 = cndb.query(INProfile.nrmTitle).filter(INProfile.nrmTitle != None)
    inq2 = cndb.query(INExperience.nrmTitle) \
               .filter(INExperience.nrmTitle != None)
    if startval:
        liq1 = liq1.filter(LIProfile.nrmTitle >= startval)
        liq2 = liq2.filter(LIExperience.nrmTitle >= startval)
        inq1 = inq1.filter(INProfile.nrmTitle >= startval)
        inq2 = inq2.filter(INExperience.nrmTitle >= startval)
    q = liq1.union_all(liq2).union_all(inq1).union_all(inq2)
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
    liq1 = cndb.query(LIProfile.nrmCompany).filter(LIProfile.nrmCompany != None)
    liq2 = cndb.query(LIExperience.nrmCompany) \
               .filter(LIExperience.nrmCompany != None)
    inq1 = cndb.query(INProfile.nrmCompany).filter(INProfile.nrmCompany != None)
    inq2 = cndb.query(INExperience.nrmCompany) \
               .filter(INExperience.nrmCompany != None)
    if startval:
        liq1 = liq1.filter(LIProfile.nrmCompany >= startval)
        liq2 = liq2.filter(LIExperience.nrmCompany >= startval)
        inq1 = inq1.filter(INProfile.nrmCompany >= startval)
        inq2 = inq2.filter(INExperience.nrmCompany >= startval)
    q = liq1.union_all(liq2).union_all(inq1).union_all(inq2)
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
    liq = cndb.query(LIEducation.nrmInstitute) \
              .filter(LIEducation.nrmInstitute != None)
    inq = cndb.query(INEducation.nrmInstitute) \
              .filter(INEducation.nrmInstitute != None)
    if startval:
        liq = liq.filter(LIEducation.nrmInstitute >= startval) 
        inq = inq.filter(INEducation.nrmInstitute >= startval)
    q = liq.union_all(inq)
    splitProcess(q, addInstitutes, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_institutes')

if catalog is None or catalog == 'degrees':
    logger.log('\nBuilding degrees catalog.\n')
    liq = cndb.query(LIEducation.nrmDegree) \
              .filter(LIEducation.nrmDegree != None)
    inq = cndb.query(INEducation.nrmDegree) \
              .filter(INEducation.nrmDegree != None)
    if startval:
        liq = liq.filter(LIEducation.nrmDegree >= startval)
        inq = inq.filter(INEducation.nrmDegree >= startval)
    q = liq.union_all(inq)
    splitProcess(q, addDegrees, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_degrees')

if catalog is None or catalog == 'subjects':
    logger.log('\nBuilding subjects catalog.\n')
    liq = cndb.query(LIEducation.nrmSubject) \
              .filter(LIEducation.nrmSubject != None)
    inq = cndb.query(INEducation.nrmSubject) \
              .filter(INEducation.nrmSubject != None)
    if startval:
        liq = liq.filter(LIEducation.nrmSubject >= startval)
        inq = inq.filter(INEducation.nrmSubject >= startval)
    q = liq.union_all(inq)
    splitProcess(q, addSubjects, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_subjects')
