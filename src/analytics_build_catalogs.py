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
import argparse


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

def addSkills():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq = cndb.query(LIProfileSkill.nrmName,
                     LIProfileSkill.name)
    inq = cndb.query(INProfileSkill.nrmName,
                     INProfileSkill.name)
    subq = liq.union_all(inq).subquery()
    nrmcol, rawcol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, func.count()) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def addProfileSkill(rec):
        nrmName, bestname, liprofileCount = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'language'          : splitNrmName(nrmName)[0],
            'name'              : bestname,
            'profileCount'      : liprofileCount,
            'experienceCount'   : 0,
        }, analyticsdb.Skill)

    logger.log('Scanning profiles.\n')
    processDb(entities(q), addProfileSkill, andb, logger=logger)

    liq = cndb.query(LIExperienceSkill.liexperienceId,
                     LIProfileSkill.nrmName.label('nrm')) \
              .join(LIProfileSkill)
    inq = cndb.query(INExperienceSkill.inexperienceId,
                     INProfileSkill.nrmName.label('nrm')) \
              .join(INProfileSkill)
    subq = liq.union_all(inq).subquery()
    idcol, nrmcol = tuple(subq.columns)
    q = cndb.query(func.count(), nrmcol) \
            .group_by(nrmcol)
    
    def addLIExperienceSkill(rec):
        experienceCount, nrmName = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'experienceCount'   : experienceCount,
        }, analyticsdb.Skill)

    logger.log('Scanning experiences.\n')
    processDb(q, addLIExperienceSkill, andb, logger=logger)


def addTitles():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq1 = cndb.query(LIProfile.nrmTitle,
                      LIProfile.parsedTitle,
                      literal_column('1').label('type'))
    liq2 = cndb.query(LIExperience.nrmTitle,
                      LIExperience.parsedTitle,
                      literal_column('2').label('type'))
    inq1 = cndb.query(INProfile.nrmTitle,
                      INProfile.parsedTitle,
                      literal_column('1').label('type'))
    inq2 = cndb.query(INExperience.nrmTitle,
                      INExperience.parsedTitle,
                      literal_column('2').label('type'))
    subq = liq1.union_all(liq2).union_all(inq1).union_all(inq2).subquery()
    nrmcol, parsedcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, parsedcol, typecol, func.count()) \
            .group_by(nrmcol, parsedcol, typecol) \
            .order_by(nrmcol)

    def addTitle(rec):
        nrmName, name, profileCount, experienceCount = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'language'          : splitNrmName(nrmName)[0],
            'name'              : name,
            'profileCount'      : profileCount,
            'experienceCount'   : experienceCount,
            }, analyticsdb.Title)
    
    processDb(entities2(q), addTitle, andb, logger=logger)


def addCompanies():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq1 = cndb.query(LIProfile.nrmCompany,
                      LIProfile.company,
                      literal_column('1').label('type'))
    liq2 = cndb.query(LIExperience.nrmCompany,
                      LIExperience.company,
                      literal_column('2').label('type'))
    inq1 = cndb.query(INProfile.nrmCompany,
                      INProfile.company,
                      literal_column('1').label('type'))
    inq2 = cndb.query(INExperience.nrmCompany,
                      INExperience.company,
                      literal_column('2').label('type'))
    subq = liq1.union_all(liq2).union_all(inq1).union_all(inq2).subquery()
    nrmcol, rawcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, typecol, func.count()) \
            .group_by(nrmcol, rawcol, typecol) \
            .order_by(nrmcol)

    def addCompany(rec):
        nrmName, name, profileCount, experienceCount = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'language'          : splitNrmName(nrmName)[0],
            'name'              : name,
            'profileCount'      : profileCount,
            'experienceCount'   : experienceCount,
            }, analyticsdb.Company)
    
    processDb(entities2(q), addCompany, andb, logger=logger)

def addSectors():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile.nrmSector, LIProfile.sector, \
                   func.count(LIProfile.id)) \
            .filter(LIProfile.nrmSector != None)
    q = q.group_by(LIProfile.nrmSector, LIProfile.sector) \
         .order_by(LIProfile.nrmSector)

    def addSector(rec):
        nrmName, name, liCount = rec
        andb.addFromDict({
            'nrmName'         : nrmName,
            'name'            : name,
            'liCount'         : liCount,
        }, analyticsdb.Sector)

    processDb(q, addSector, andb, logger=logger) 


def addLocations():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Location.placeId, Location.name, ST_AsText(Location.geo)) \
            .filter(Location.placeId != None)
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


def addInstitutes():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq = cndb.query(LIEducation.nrmInstitute,
                     LIEducation.institute)
    inq = cndb.query(INEducation.nrmInstitute,
                     INEducation.institute)
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


def addDegrees():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq = cndb.query(LIEducation.nrmDegree,
                     LIEducation.degree)
    inq = cndb.query(INEducation.nrmDegree,
                     INEducation.degree)
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


def addSubjects():
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    liq = cndb.query(LIEducation.nrmSubject,
                     LIEducation.subject)
    inq = cndb.query(INEducation.nrmSubject,
                     INEducation.subject)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('catalog', help=
                        'The catalog to build. If omitted all are built.',
                        choices=['skills', 'titles', 'sectors', 'companies',
                                 'locations', 'institutes', 'degrees',
                                 'subjects'],
                        default=None, nargs='?')
    args = parser.parse_args()
    catalog = args.catalog
    
    cndb = CanonicalDB(conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    if catalog is None or catalog == 'skills':
        logger.log('\nBuilding skills catalog.\n')
        addSkills()

    if catalog is None or catalog == 'titles':
        logger.log('\nBuilding titles catalog.\n')
        addTitles()
        
    if catalog is None or catalog == 'sectors':
        logger.log('\nBuilding sectors catalog.\n')
        addSectors()

    if catalog is None or catalog == 'companies':
        logger.log('\nBuilding companies catalog.\n')
        addCompanies()

    if catalog is None or catalog == 'locations':
        logger.log('\nBuilding locations catalog.\n')
        addLocations()

    if catalog is None or catalog == 'institutes':
        logger.log('\nBuilding institutes catalog.\n')
        addInstitutes()

    if catalog is None or catalog == 'degrees':
        logger.log('\nBuilding degrees catalog.\n')
        addDegrees()

    if catalog is None or catalog == 'subjects':
        logger.log('\nBuilding subjects catalog.\n')
        addSubjects()
