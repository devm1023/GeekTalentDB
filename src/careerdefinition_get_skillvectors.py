import conf
from canonicaldb import *
from logger import Logger
from textnormalization import normalized_entity, split_nrm_name
from entitycloud import relevance_scores
from entity_mapper import EntityMapper
from sqlalchemy import func
from pgvalues import in_values
import csv
from math import sqrt
import argparse


def _iter_items(keys, d):
    for key in keys:
        if key in d:
            yield key, d[key]


def get_total_counts(cndb, logger, profile_table, skill_table, language, nuts0, mincount, analysis_sector,
                     additional_filters):
    # handle differences between jobs and profiles
    is_job = profile_table is ADZJob or profile_table is INJob
    # job location data is inline
    loc_table = profile_table if is_job else Location
    # closest thing we have to a sector field for jobs
    sector_field = profile_table.analysis_category if is_job else profile_table.nrm_sector

    # bespoke field for subset ana;ysis of people (LinkedIn)
    if analysis_sector is not None:
        sector_field = profile_table.analysis_category

    logger.log('Counting profiles.\n')

    common_filters = [
        profile_table.language == language,
        loc_table.nuts0 == nuts0,
        *additional_filters
    ]

    # total number of profiles
    totalq_nosf = cndb.query(profile_table.id) \
        .filter(*common_filters)

    # total number of profiles with a non-null sector
    totalq_sf = cndb.query(profile_table.id) \
        .filter(sector_field != None,
                *common_filters)

    # join location if needed
    if loc_table is not profile_table:
        totalq_nosf = totalq_nosf.join(Location, Location.nrm_name == profile_table.nrm_location)
        totalq_sf = totalq_sf.join(Location, Location.nrm_name == profile_table.nrm_location)

    totalc_sf = totalq_sf.count()
    totalc_nosf = totalq_nosf.count()

    logger.log('Counting skills.\n')
    countcol = func.count().label('counts')

    # total counts of skills
    skillq_nosf = cndb.query(skill_table.nrm_name, countcol) \
        .join(profile_table) \
        .filter(*common_filters) \
        .group_by(skill_table.nrm_name) \
        .having(countcol >= mincount)

    # total counts of skills with a non-null sector
    skillq_sf = cndb.query(skill_table.nrm_name, countcol) \
        .join(profile_table) \
        .filter(*common_filters,
                sector_field != None) \
        .group_by(skill_table.nrm_name) \
        .having(countcol >= mincount)

    # join location if needed
    if loc_table is not profile_table:
        skillq_nosf = skillq_nosf.join(Location, Location.nrm_name == profile_table.nrm_location)
        skillq_sf = skillq_sf.join(Location, Location.nrm_name == profile_table.nrm_location)

    skillcounts_nosf = dict(skillq_nosf)
    skillcounts_sf = dict(skillq_sf)

    return (totalc_sf, totalc_nosf, skillcounts_sf, skillcounts_nosf)


def skillvectors(profile_table, skill_table, source, titles, mappings, language='en', nuts0='UK', mincount=1,
                 analysis_sector=None, total_counts=None, additional_filters=[]):
    logger = Logger()
    cndb = CanonicalDB()
    logger = Logger()
    mapper = EntityMapper(cndb, mappings)

    is_job = profile_table is ADZJob or profile_table is INJob
    # job location data is inline
    loc_table = profile_table if is_job else Location
    title_field = profile_table.nrm_title if is_job else profile_table.nrm_curr_title

    # get totals
    if total_counts is None:
        total_counts = get_total_counts(cndb, logger, profile_table, skill_table, language, nuts0, mincount,
                                        analysis_sector, additional_filters)

    totalc_sf, totalc_nosf, skillcounts_sf, skillcounts_nosf = total_counts

    skillvectors = []
    newtitles = []
    titlecounts = []

    # filters used by all queries
    common_filters = [
        profile_table.language == language,
        loc_table.nuts0 == nuts0,
        *additional_filters
    ]

    for sector, title, sector_filter in titles:
        logger.log('Processing: {0:s}\n'.format(title))

        nrm_title = normalized_entity('job_title' if is_job else 'title', source, language, title)

        if nrm_title is None:
            continue

        nrm_sector = analysis_sector if is_job else normalized_entity('sector', source, language, sector)
        logger.log('\nNormalised Sector : ' + str(nrm_sector))
        if sector_filter:
            totalc = totalc_sf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_sf)

            if is_job:
                common_filters.append(
                    profile_table.analysis_category == analysis_sector)
            else:
                # Analysis sector for bespoke analysis where data is not within one nrm_sector
                if analysis_sector is not None:
                    common_filters.append(
                        profile_table.analysis_sector == analysis_sector)
                else:
                    common_filters.append(
                        profile_table.nrm_sector.in_(mapper.inv(nrm_sector)))
        else:
            totalc = totalc_nosf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_nosf)

        # get job titles to scan for skills
        similar_titles = set()
        for nrm_title in mapper.inv(nrm_title, nrm_sector=nrm_sector):
            similar_titles.add(nrm_title)
            tpe, source, language, words = split_nrm_name(nrm_title)
            if len(words.split()) > 1:
                for entity, _, _, _ in cndb.find_entities(
                        tpe, source, language, words, normalize=False):
                    similar_titles.add(entity)

        # get count for this title
        titleq = cndb.query(profile_table.id) \
            .filter(*common_filters,
                    in_values(title_field,
                              similar_titles)
                    )
        # get counts for skills for this titls
        coincidenceq = cndb.query(skill_table.nrm_name, func.count()) \
            .join(profile_table) \
            .filter(*common_filters,
                    in_values(title_field,
                              similar_titles)
                    )
        # join location if needed from Profile (job) or Location table (linkedin)
        if loc_table is not profile_table:
            titleq = titleq.join(Location, Location.nrm_name == profile_table.nrm_location)
            coincidenceq = coincidenceq.join(Location, Location.nrm_name == profile_table.nrm_location)

        titlec = titleq.count()
        logger.log('\nTotal Count : ' + str(totalc))
        logger.log('\nTitle Count : ' + str(titlec))
        logger.log('\nEntitesq : ' + str(entitiesq))
        logger.log('\nCoincidenceq : ' + str(coincidenceq))
        skillvector = {}
        for nrm_skill, skillc, titleskillc, _, _ in \
                relevance_scores(totalc, titlec, entitiesq, coincidenceq,
                                 entitymap=mapper):
            if skillc < mincount:
                continue
            # skillvector[nrm_skill] = titleskillc/totalc*log(totalc/skillc)
            skillvector[nrm_skill] = titleskillc / totalc

        if skillvector:
            norm = sqrt(sum(v ** 2 for v in skillvector.values()))
            skillvector = dict((s, v / norm) for s, v in skillvector.items())
            newtitles.append((sector, title, sector_filter))
            titlecounts.append(titlec)
            skillvectors.append(skillvector)
    return newtitles, titlecounts, skillvectors


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file',
                        help='CSV file with sectors and job titles.')
    parser.add_argument('output_file',
                        help='Name of the CSV file to generate.')
    parser.add_argument('--mappings',
                        help='CSV file holding entity mappings.')
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for skills.')
    parser.add_argument('--source', choices=['linkedin', 'indeed', 'adzuna'], default='linkedin',
                        help='The data source to process.')
    parser.add_argument('--analysis-sector', default=None,
                        help='Bespoke Analysis')
    args = parser.parse_args()

    logger = Logger()

    titles = []
    with open(args.input_file, 'r') as input_file:
        csvreader = csv.reader(input_file)
        for row in csvreader:
            if len(row) < 3:
                continue
            sector_filter = bool(int(row[2]))
            titles.append((row[0], row[1], sector_filter))

    if args.source == 'linkedin':
        profile_table = LIProfile
        skill_table = LIProfileSkill
    elif args.source == 'indeed':
        profile_table = INProfile
        skill_table = INProfileSkill
    elif args.source == 'adzuna':
        profile_table = ADZJob
        skill_table = ADZJobSkill

    titles, titlecounts, skillvectors \
        = skillvectors(profile_table, skill_table, args.source, titles, args.mappings, 'en', 'UK', args.min_count,
                       args.analysis_sector)

    with open(args.output_file, 'w') as outputfile:
        csvwriter = csv.writer(outputfile)
        for title, count, skillvector in zip(titles, titlecounts, skillvectors):
            logger.log(str(title[1]))
            csvwriter.writerow([
                't', title[0], title[1], int(title[2]), count])
            skillvector = list(skillvector.items())
            skillvector.sort(key=lambda x: -x[-1])
            for skill, frac in skillvector:
                csvwriter.writerow(['s', skill, frac])