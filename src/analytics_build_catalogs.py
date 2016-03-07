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
from collections import OrderedDict
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

def addSkills(batchsize, sourceId):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if sourceId == 'linkedin':
        nrmcol = LIProfileSkill.nrmName
        rawcol = LIProfileSkill.name
    elif sourceId == 'indeed':
        nrmcol = INProfileSkill.nrmName
        rawcol = INProfileSkill.name
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def addProfileSkill(rec):
        nrmName, bestname, liprofileCount = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'           : nrmName,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : bestname,
            'profileCount'      : liprofileCount,
            'subDocumentCount'  : 0,
        }, analyticsdb.Entity)

    logger.log('Scanning profiles.\n')
    processDb(entities(q), addProfileSkill, andb, batchsize=batchsize,
              logger=logger)

    if sourceId == 'linkedin':
        jointable = LIProfileSkill
        nrmcol    = LIProfileSkill.nrmName
        idcol     = LIExperienceSkill.liexperienceId
    elif sourceId == 'indeed':
        jointable = INProfileSkill
        nrmcol    = INProfileSkill.nrmName
        idcol     = INExperienceSkill.inexperienceId
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))

    q = cndb.query(func.count(idcol), nrmcol) \
            .join(jointable) \
            .filter(nrmcol != None) \
            .group_by(nrmcol)
    
    def addLIExperienceSkill(rec):
        subDocumentCount, nrmName = rec
        andb.addFromDict({
            'nrmName'           : nrmName,
            'subDocumentCount'  : subDocumentCount,
        }, analyticsdb.Entity)

    logger.log('Scanning experiences.\n')
    processDb(q, addLIExperienceSkill, andb, batchsize=batchsize, logger=logger)


def addTitles(batchsize, sourceId):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if sourceId == 'linkedin':
        nrmcol1    = LIProfile.nrmTitle
        parsedcol1 = LIProfile.parsedTitle
        nrmcol2    = LIExperience.nrmTitle
        parsedcol2 = LIExperience.parsedTitle
    elif sourceId == 'indeed':
        nrmcol1    = INProfile.nrmTitle
        parsedcol1 = INProfile.parsedTitle
        nrmcol2    = INExperience.nrmTitle
        parsedcol2 = INExperience.parsedTitle
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))
    
    q1 = cndb.query(nrmcol1, parsedcol1, literal_column('1').label('type')) \
             .filter(nrmcol1 != None)
    q2 = cndb.query(nrmcol2, parsedcol2, literal_column('2').label('type')) \
             .filter(nrmcol2 != None)
    subq = q1.union_all(q2).subquery()
    nrmcol, parsedcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, parsedcol, typecol, func.count()) \
            .group_by(nrmcol, parsedcol, typecol) \
            .order_by(nrmcol)

    def addTitle(rec):
        nrmName, name, profileCount, subDocumentCount = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'           : nrmName,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : name,
            'profileCount'      : profileCount,
            'subDocumentCount'  : subDocumentCount,
            }, analyticsdb.Entity)
    
    processDb(entities2(q), addTitle, andb, batchsize=batchsize, logger=logger)


def addCompanies(batchsize, sourceId):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    if sourceId == 'linkedin':
        nrmcol1 = LIProfile.nrmCompany
        rawcol1 = LIProfile.company
        nrmcol2 = LIExperience.nrmCompany
        rawcol2 = LIExperience.company
    elif sourceId == 'indeed':
        nrmcol1 = INProfile.nrmCompany
        rawcol1 = INProfile.company
        nrmcol2 = INExperience.nrmCompany
        rawcol2 = INExperience.company
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))

    q1 = cndb.query(nrmcol1, rawcol1, literal_column('1').label('type')) \
             .filter(nrmcol1 != None)
    q2 = cndb.query(nrmcol2, rawcol2, literal_column('2').label('type')) \
             .filter(nrmcol2 != None)
    subq = q1.union_all(q2).subquery()
    nrmcol, rawcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, typecol, func.count()) \
            .group_by(nrmcol, rawcol, typecol) \
            .order_by(nrmcol)

    def addCompany(rec):
        nrmName, name, profileCount, subDocumentCount = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'           : nrmName,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : name,
            'profileCount'      : profileCount,
            'subDocumentCount'   : subDocumentCount,
            }, analyticsdb.Entity)
    
    processDb(entities2(q), addCompany, andb, batchsize=batchsize,
              logger=logger)

def addSectors(batchsize):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile.nrmSector, LIProfile.sector, \
                   func.count(LIProfile.id)) \
            .filter(LIProfile.nrmSector != None)
    q = q.group_by(LIProfile.nrmSector, LIProfile.sector) \
         .order_by(LIProfile.nrmSector)

    def addSector(rec):
        nrmName, name, count = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'         : nrmName,
            'type'            : tpe,
            'source'          : source,
            'language'        : language,
            'name'            : name,
            'profileCount'    : count,
        }, analyticsdb.Entity)

    processDb(q, addSector, andb, batchsize=batchsize, logger=logger) 


def addLocations(batchsize):
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

    processDb(q, addLocation, andb, batchsize=batchsize, logger=logger) 


def addInstitutes(batchsize, sourceId):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    if sourceId == 'linkedin':
        nrmcol = LIEducation.nrmInstitute
        rawcol = LIEducation.institute
    elif sourceId == 'indeed':
        nrmcol = INEducation.nrmInstitute
        rawcol = INEducation.institute
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def addInstitute(rec):
        nrmName, name, count = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'          : nrmName,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'subDocumentCount' : count,
            }, analyticsdb.Entity)
    
    processDb(entities(q), addInstitute, andb, batchsize=batchsize,
              logger=logger)


def addDegrees(batchsize, sourceId):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    if sourceId == 'linkedin':
        nrmcol = LIEducation.nrmDegree
        rawcol = LIEducation.degree
    elif sourceId == 'indeed':
        nrmcol = INEducation.nrmDegree
        rawcol = INEducation.degree
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def addDegree(rec):
        nrmName, name, count = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'          : nrmName,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'subDocumentCount' : count,
            }, analyticsdb.Entity)
    
    processDb(entities(q), addDegree, andb, batchsize=batchsize, logger=logger)


def addSubjects(batchsize, sourceId):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    if sourceId == 'linkedin':
        nrmcol = LIEducation.nrmSubject
        rawcol = LIEducation.subject
    elif sourceId == 'indeed':
        nrmcol = INEducation.nrmSubject
        rawcol = INEducation.subject
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(sourceId))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def addSubject(rec):
        nrmName, name, count = rec
        tpe, source, language, _ = splitNrmName(nrmName)
        andb.addFromDict({
            'nrmName'          : nrmName,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'subDocumentCount' : count,
            }, analyticsdb.Entity)
    
    processDb(entities(q), addSubject, andb, batchsize=batchsize, logger=logger)


if __name__ == '__main__':
    allcatalogs = OrderedDict([
        ('skills'     , addSkills),
        ('titles'     , addTitles),
        ('sectors'    , addSectors),
        ('companies'  , addCompanies),
        ('locations'  , addLocations),
        ('institutes' , addInstitutes),
        ('degrees'    , addDegrees),
        ('subjects'   , addSubjects),
    ])
    allsources = ['linkedin', 'indeed']
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', help=
                        'The catalog to build. If omitted all are built.',
                        choices=list(allcatalogs.keys()), default=None)
    parser.add_argument('--source', help=
                        'The data source to build catalogs from. '
                        'If omitted all are built.',
                        choices=allsources, default=None)
    parser.add_argument('--batchsize', type=int, default=10000, help=
                        'Number of rows to commit in one batch.')
    args = parser.parse_args()
    catalogs = list(allcatalogs.keys()) if args.catalog is None \
               else [args.catalog]
    sources = allsources if args.source is None else [args.source]
    batchsize = args.batchsize
    
    logger = Logger(sys.stdout)

    for catalog in catalogs:
        addfunc = allcatalogs[catalog]
        logger.log('\nBuilding {0:s} catalog.\n'.format(catalog))
        if catalog in ['sectors', 'locations']:
            addfunc(batchsize)
        else:
            for source in sources:
                logger.log('Processing {0:s} data.\n'.format(source))
                addfunc(batchsize, source)
