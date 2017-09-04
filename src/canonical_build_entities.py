from canonicaldb import *
from dbtools import dict_from_row
from sqlalchemy import func
from sqlalchemy.sql.expression import literal_column
from geoalchemy2.functions import ST_AsText
from geoalchemy2.shape import to_shape
from logger import Logger
import sys
from windowquery import split_process, process_db
from textnormalization import split_nrm_name
from nuts import NutsRegions
from shapely.geometry import Polygon
from shapely import wkt
from collections import OrderedDict
import argparse


def entities(q, countcols=1):
    currententity = None
    maxcount = 0
    totalcounts = tuple(0 for i in range(countcols))
    bestname = None
    for row in q:
        nrm_name = row[0]
        name = row[1]
        counts = row[2:]
        if nrm_name != currententity:
            if bestname:
                yield (currententity, bestname) + totalcounts
            maxcount = 0
            totalcounts = tuple(0 for i in range(countcols))
            bestname = None
            currententity = nrm_name
        if counts[0] > maxcount:
            bestname = name
            maxcount = counts[0]
        totalcounts = tuple(t+c for t,c in zip(totalcounts, counts))

    if bestname:
        yield (currententity, bestname) + totalcounts

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
    cndb = CanonicalDB()
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIProfileSkill.nrm_name
        rawcol = LIProfileSkill.name
    elif source_id == 'indeed':
        nrmcol = INProfileSkill.nrm_name
        rawcol = INProfileSkill.name
    elif source_id == 'adzuna':
        nrmcol = ADZJobSkill.nrm_name
        rawcol = ADZJobSkill.name
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_profile_skill(rec):
        nrm_name, bestname, liprofile_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        cndb.add_from_dict({
            'nrm_name'           : nrm_name,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : bestname,
            'profile_count'      : liprofile_count,
            'sub_document_count'  : 0,
        }, Entity)

    logger.log('Scanning profiles.\n')
    process_db(entities(q), add_profile_skill, cndb, batchsize=batchsize,
              logger=logger)

    if source_id == 'linkedin':
        jointable = LIProfileSkill
        nrmcol    = LIProfileSkill.nrm_name
        idcol     = LIExperienceSkill.liexperience_id
    elif source_id == 'indeed':
        jointable = INProfileSkill
        nrmcol    = INProfileSkill.nrm_name
        idcol     = INExperienceSkill.inexperience_id
    elif source_id == 'adzuna':
        # no experiences
        return
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(func.count(idcol), nrmcol) \
            .join(jointable) \
            .filter(nrmcol != None) \
            .group_by(nrmcol)

    def add_liexperience_skill(rec):
        sub_document_count, nrm_name = rec
        cndb.add_from_dict({
            'nrm_name'           : nrm_name,
            'sub_document_count'  : sub_document_count,
        }, Entity)

    logger.log('Scanning experiences.\n')
    process_db(q, add_liexperience_skill, cndb, batchsize=batchsize,
               logger=logger)


def add_titles(batchsize, source_id):
    cndb = CanonicalDB()
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
    elif source_id == 'adzuna':
        nrmcol1    = ADZJob.nrm_title
        parsedcol1 = ADZJob.parsed_title
        nrmcol2    = None
        parsedcol2 = None
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q1 = cndb.query(nrmcol1, parsedcol1, literal_column('1').label('type')) \
             .filter(nrmcol1 != None)

    if nrmcol2 is not None:
        q2 = cndb.query(nrmcol2, parsedcol2, literal_column('2').label('type')) \
                .filter(nrmcol2 != None)
        subq = q1.union_all(q2).subquery()
    else:
        subq = q1.subquery()

    nrmcol, parsedcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, parsedcol, typecol, func.count()) \
            .group_by(nrmcol, parsedcol, typecol) \
            .order_by(nrmcol)

    def add_title(rec):
        nrm_name, name, profile_count, sub_document_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        cndb.add_from_dict({
            'nrm_name'           : nrm_name,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : name,
            'profile_count'      : profile_count,
            'sub_document_count'  : sub_document_count,
            }, Entity)

    process_db(entities2(q), add_title, cndb, batchsize=batchsize,
               logger=logger)


def add_companies(batchsize, source_id):
    cndb = CanonicalDB()
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
    elif source_id == 'adzuna':
        nrmcol1 = ADZJob.nrm_company
        rawcol1 = ADZJob.company
        nrmcol2 = None
        rawcol2 = None
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q1 = cndb.query(nrmcol1, rawcol1, literal_column('1').label('type')) \
             .filter(nrmcol1 != None)

    if nrmcol2 is not None:
        q2 = cndb.query(nrmcol2, rawcol2, literal_column('2').label('type')) \
                .filter(nrmcol2 != None)
        subq = q1.union_all(q2).subquery()
    else:
        subq = q1.subquery()

    nrmcol, rawcol, typecol = tuple(subq.columns)
    q = cndb.query(nrmcol, rawcol, typecol, func.count()) \
            .group_by(nrmcol, rawcol, typecol) \
            .order_by(nrmcol)

    def add_company(rec):
        nrm_name, name, profile_count, sub_document_count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        cndb.add_from_dict({
            'nrm_name'           : nrm_name,
            'type'              : tpe,
            'source'            : source,
            'language'          : language,
            'name'              : name,
            'profile_count'      : profile_count,
            'sub_document_count'   : sub_document_count,
            }, Entity)

    process_db(entities2(q), add_company, cndb, batchsize=batchsize,
              logger=logger)

def add_sectors(batchsize):
    cndb = CanonicalDB()
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
        cndb.add_from_dict({
            'nrm_name'        : nrm_name,
            'type'            : tpe,
            'source'          : source,
            'language'        : language,
            'name'            : name,
            'profile_count'   : count,
            'sub_document_count' : sub_document_count,
        }, Entity)

    process_db(entities(q, countcols=2), add_sector, cndb,
               batchsize=batchsize, logger=logger)


def add_institutes(batchsize, source_id):
    cndb = CanonicalDB()
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIEducation.nrm_institute
        rawcol = LIEducation.institute
    elif source_id == 'indeed':
        nrmcol = INEducation.nrm_institute
        rawcol = INEducation.institute
    elif source_id == 'adzuna':
        # no institutes
        return
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_institute(rec):
        nrm_name, name, count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        cndb.add_from_dict({
            'nrm_name'          : nrm_name,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'sub_document_count' : count,
            }, Entity)

    process_db(entities(q), add_institute, cndb, batchsize=batchsize,
              logger=logger)


def add_degrees(batchsize, source_id):
    cndb = CanonicalDB()
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIEducation.nrm_degree
        rawcol = LIEducation.degree
    elif source_id == 'indeed':
        nrmcol = INEducation.nrm_degree
        rawcol = INEducation.degree
    elif source_id == 'adzuna':
        # no degrees
        return
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_degree(rec):
        nrm_name, name, count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        cndb.add_from_dict({
            'nrm_name'          : nrm_name,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'sub_document_count' : count,
            }, Entity)

    process_db(entities(q), add_degree, cndb, batchsize=batchsize,
               logger=logger)


def add_subjects(batchsize, source_id):
    cndb = CanonicalDB()
    logger = Logger(sys.stdout)

    if source_id == 'linkedin':
        nrmcol = LIEducation.nrm_subject
        rawcol = LIEducation.subject
    elif source_id == 'indeed':
        nrmcol = INEducation.nrm_subject
        rawcol = INEducation.subject
    elif source_id == 'adzuna':
        # no subjects
        return
    else:
        raise ValueError('Invalid source type `{0:s}`.'.format(source_id))

    q = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

    def add_subject(rec):
        nrm_name, name, count = rec
        tpe, source, language, _ = split_nrm_name(nrm_name)
        cndb.add_from_dict({
            'nrm_name'          : nrm_name,
            'type'             : tpe,
            'source'           : source,
            'language'         : language,
            'name'             : name,
            'sub_document_count' : count,
            }, Entity)

    process_db(entities(q), add_subject, cndb, batchsize=batchsize,
               logger=logger)


allcatalogs = OrderedDict([
    ('skills'     , add_skills),
    ('titles'     , add_titles),
    ('sectors'    , add_sectors),
    ('companies'  , add_companies),
    ('institutes' , add_institutes),
    ('degrees'    , add_degrees),
    ('subjects'   , add_subjects),
])

allsources = ['linkedin', 'indeed', 'adzuna']

    
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
    parser.add_argument('--batch-size', type=int, default=1000, help=
                        'Number of rows to commit in one batch.')
    args = parser.parse_args()
    main(args)
