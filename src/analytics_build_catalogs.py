import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from sqlalchemy.sql.expression import literal_column
from geoalchemy2.functions import ST_AsText
from logger import Logger
import sys
from windowquery import split_process, process_db
from textnormalization import split_nrm_name
from collections import OrderedDict
import argparse


def entities(q):
    currententity = None
    maxcount = 0
    profilecount = 0
    bestname = None
    for nrm_name, name, count in q:
        if nrm_name != currententity:
            if bestname:
                yield currententity, bestname, profilecount
            maxcount = 0
            profilecount = 0
            bestname = None
            currententity = nrm_name
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
    for nrm_name, name, tpe, count in q:
        if nrm_name != currententity:
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
            currententity = nrm_name
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

def add_skills(batchsize, source_id):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIProfileSkill.nrm_name
        rawcol = LIProfileSkill.name
    elif source_id == 'indeed':
        nrmcol = INProfileSkill.nrm_name
        rawcol = INProfileSkill.name
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_profile_skill(rec):
        nrm_name, bestname, liprofile_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'           : nrm_name,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : bestname,
            'profile_count'      : liprofile_count,
            'sub_document_count'  : 0,
        }, analyticsdb.Entity)

    logger.log('Scanning profiles.\n')
    process_db(entities(q), add_profile_skill, andb, batchsize=batchsize,
              logger=logger)

    if source_id == 'linkedin':
        jointable = LIProfileSkill
        nrmcol    = LIProfileSkill.nrm_name
        idcol     = LIExperienceSkill.liexperience_id
    elif source_id == 'indeed':
        jointable = INProfileSkill
        nrmcol    = INProfileSkill.nrm_name
        idcol     = INExperienceSkill.inexperience_id
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(func.count(idcol), nrmcol) \
            .join(jointable) \
            .filter(nrmcol != None) \
            .group_by(nrmcol)

    def add_liexperience_skill(rec):
        sub_document_count, nrm_name = rec
        andb.add_from_dict({
            'nrm_name'           : nrm_name,
            'sub_document_count'  : sub_document_count,
        }, analyticsdb.Entity)

    logger.log('Scanning experiences.\n')
    process_db(q, add_liexperience_skill, andb, batchsize=batchsize, logger=logger)


def add_titles(batchsize, source_id):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol1    = LIProfile.nrm_title
        parsedcol1 = LIProfile.parsed_title
        nrmcol2    = LIExperience.nrm_title
        parsedcol2 = LIExperience.parsed_title
    elif source_id == 'indeed':
        nrmcol1    = INProfile.nrm_title
        parsedcol1 = INProfile.parsed_title
        nrmcol2    = INExperience.nrm_title
        parsedcol2 = INExperience.parsed_title
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q1 = cndb.query(nrmcol1, parsedcol1, literal_column('1').label('type')) \
             .filter(nrmcol1 != None)
    q2 = cndb.query(nrmcol2, parsedcol2, literal_column('2').label('type')) \
             .filter(nrmcol2 != None)
    subq = q1.union_all(q2).subquery()
    nrmcol, parsedcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, parsedcol, typecol, func.count()) \
            .group_by(nrmcol, parsedcol, typecol) \
            .order_by(nrmcol)

    def add_title(rec):
        nrm_name, name, profile_count, sub_document_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'           : nrm_name,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : name,
            'profile_count'      : profile_count,
            'sub_document_count'  : sub_document_count,
            }, analyticsdb.Entity)

    process_db(entities2(q), add_title, andb, batchsize=batchsize, logger=logger)


def add_companies(batchsize, source_id):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol1 = LIProfile.nrm_company
        rawcol1 = LIProfile.company
        nrmcol2 = LIExperience.nrm_company
        rawcol2 = LIExperience.company
    elif source_id == 'indeed':
        nrmcol1 = INProfile.nrm_company
        rawcol1 = INProfile.company
        nrmcol2 = INExperience.nrm_company
        rawcol2 = INExperience.company
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q1 = cndb.query(nrmcol1, rawcol1, literal_column('1').label('type')) \
             .filter(nrmcol1 != None)
    q2 = cndb.query(nrmcol2, rawcol2, literal_column('2').label('type')) \
             .filter(nrmcol2 != None)
    subq = q1.union_all(q2).subquery()
    nrmcol, rawcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, typecol, func.count()) \
            .group_by(nrmcol, rawcol, typecol) \
            .order_by(nrmcol)

    def add_company(rec):
        nrm_name, name, profile_count, sub_document_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'           : nrm_name,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : name,
            'profile_count'      : profile_count,
            'sub_document_count'   : sub_document_count,
            }, analyticsdb.Entity)

    process_db(entities2(q), add_company, andb, batchsize=batchsize,
              logger=logger)

def add_sectors(batchsize):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(LIProfile.nrm_sector, LIProfile.sector, \
                   func.count(LIProfile.id),
                   func.sum(LIProfile.n_experiences)) \
            .filter(LIProfile.nrm_sector != None)
    q = q.group_by(LIProfile.nrm_sector, LIProfile.sector) \
         .order_by(LIProfile.nrm_sector)

    def add_sector(rec):
        nrm_name, name, count, sub_document_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'        : nrm_name,
            'type'            : tpe,
            'source'          : source,
            'language'        : language,
            'name'            : name,
            'profile_count'   : count,
            'sub_document_count' : sub_document_count,
        }, analyticsdb.Entity)

    process_db(q, add_sector, andb, batchsize=batchsize, logger=logger)


def add_locations(batchsize):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(Location.place_id, Location.name, ST_AsText(Location.geo)) \
            .filter(Location.place_id != None)
    q = q.distinct().order_by(Location.place_id)

    def add_location(rec):
        from copy import deepcopy
        place_id, name, geo = rec
        andb.add_from_dict({
            'place_id'         : place_id,
            'name'            : name,
            'geo'             : geo,
        }, analyticsdb.Location)

    process_db(q, add_location, andb, batchsize=batchsize, logger=logger)


def add_institutes(batchsize, source_id):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIEducation.nrm_institute
        rawcol = LIEducation.institute
    elif source_id == 'indeed':
        nrmcol = INEducation.nrm_institute
        rawcol = INEducation.institute
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_institute(rec):
        nrm_name, name, count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'          : nrm_name,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'sub_document_count' : count,
            }, analyticsdb.Entity)

    process_db(entities(q), add_institute, andb, batchsize=batchsize,
              logger=logger)


def add_degrees(batchsize, source_id):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIEducation.nrm_degree
        rawcol = LIEducation.degree
    elif source_id == 'indeed':
        nrmcol = INEducation.nrm_degree
        rawcol = INEducation.degree
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_degree(rec):
        nrm_name, name, count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'          : nrm_name,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'sub_document_count' : count,
            }, analyticsdb.Entity)

    process_db(entities(q), add_degree, andb, batchsize=batchsize, logger=logger)


def add_subjects(batchsize, source_id):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIEducation.nrm_subject
        rawcol = LIEducation.subject
    elif source_id == 'indeed':
        nrmcol = INEducation.nrm_subject
        rawcol = INEducation.subject
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_subject(rec):
        nrm_name, name, count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        andb.add_from_dict({
            'nrm_name'          : nrm_name,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'sub_document_count' : count,
            }, analyticsdb.Entity)

    process_db(entities(q), add_subject, andb, batchsize=batchsize, logger=logger)


allcatalogs = OrderedDict([
    ('skills'     , add_skills),
    ('titles'     , add_titles),
    ('sectors'    , add_sectors),
    ('companies'  , add_companies),
    ('locations'  , add_locations),
    ('institutes' , add_institutes),
    ('degrees'    , add_degrees),
    ('subjects'   , add_subjects),
])

allsources = ['linkedin', 'indeed']

    
def main(args):
    catalogs = list(allcatalogs.keys()) if args.catalog is None \
               else [args.catalog]
    sources = allsources if args.source is None else [args.source]
    batchsize = args.batch_size

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
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', help=
                        'The catalog to build. If omitted all are built.',
                        choices=list(allcatalogs.keys()), default=None)
    parser.add_argument('--source', help=
                        'The data source to build catalogs from. '
                        'If omitted all are built.',
                        choices=allsources, default=None)
    parser.add_argument('--batchsize', type=int, default=1000, help=
                        'Number of rows to commit in one batch.')
    args = parser.parse_args()
    main(args)
