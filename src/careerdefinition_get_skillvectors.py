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


def get_total_counts(cndb, logger, profile_table, skill_table, mincount):

    is_job = profile_table is ADZJob or profile_table is INJob

    logger.log('Counting profiles.\n')


    if is_job:
        totalc_nosf = cndb.query(profile_table.id) \
                          .filter(profile_table.language == 'en') \
                          .count()
        # every Adzuna job has a category
        totalc_sf = totalc_nosf
    else:
        totalc_nosf = cndb.query(profile_table.id) \
                          .join(Location,
                                Location.nrm_name == profile_table.nrm_location) \
                          .filter(profile_table.language == 'en',
                                  Location.nuts0 == 'UK') \
                          .count()
        totalc_sf = cndb.query(profile_table.id) \
                        .join(Location,
                            Location.nrm_name == profile_table.nrm_location) \
                        .filter(profile_table.nrm_sector != None,
                                profile_table.language == 'en',
                                Location.nuts0 == 'UK') \
                        .count()

    logger.log('Counting skills.\n')
    countcol = func.count().label('counts')

    if is_job:
        q = cndb.query(skill_table.nrm_name, countcol) \
                .join(profile_table) \
                .filter(profile_table.language == 'en') \
                .group_by(skill_table.nrm_name) \
                .having(countcol >= mincount)
        skillcounts_nosf = dict(q)
        skillcounts_sf = skillcounts_nosf
    else:
        q = cndb.query(skill_table.nrm_name, countcol) \
                .join(profile_table) \
                .join(Location,
                      Location.nrm_name == profile_table.nrm_location) \
                .filter(Location.nuts0 == 'UK',
                        profile_table.language == 'en') \
                .group_by(skill_table.nrm_name) \
                .having(countcol >= mincount)
        skillcounts_nosf = dict(q)

        q = cndb.query(skill_table.nrm_name, countcol) \
                .join(profile_table) \
                .join(Location,
                    Location.nrm_name == profile_table.nrm_location) \
                .filter(Location.nuts0 == 'UK',
                        profile_table.language == 'en',
                        profile_table.nrm_sector != None) \
                .group_by(skill_table.nrm_name) \
                .having(countcol >= mincount)
        skillcounts_sf = dict(q)

    return (totalc_sf, totalc_nosf, skillcounts_sf, skillcounts_nosf)

def skillvectors(profile_table, skill_table, source, titles, mappings, mincount=1):
    cndb = CanonicalDB()
    logger = Logger()
    mapper = EntityMapper(cndb, mappings)

    is_job = profile_table is ADZJob or profile_table is INJob

    # get totals
    totalc_sf, totalc_nosf, skillcounts_sf, skillcounts_nosf = get_total_counts(cndb, logger, profile_table, skill_table, mincount)

    skillvectors = []
    newtitles = []
    titlecounts = []

    for sector, title, sector_filter in titles:
        logger.log('Processing: {0:s}\n'.format(title))

        nrm_title = normalized_entity('job_title' if is_job else 'title', source, 'en', title)

        if nrm_title is None:
            continue

        nrm_sector = normalized_entity('sector', source, 'en', sector)
        
        if sector_filter:
            totalc = totalc_sf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_sf)
            sector_clause = (
                profile_table.nrm_sector.in_(mapper.inv(nrm_sector)),)
        else:
            totalc = totalc_nosf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_nosf)
            sector_clause = tuple()

        # get job titles to scan for skills
        similar_titles = set()
        for nrm_title in mapper.inv(nrm_title, nrm_sector=nrm_sector):
            similar_titles.add(nrm_title)
            tpe, source, language, words = split_nrm_name(nrm_title)
            if len(words.split()) > 1:
                for entity, _, _, _ in cndb.find_entities(
                        tpe, source, language, words, normalize=False):
                    similar_titles.add(entity)
        
        if is_job:
            titlec = cndb.query(profile_table.id) \
                         .filter(profile_table.language == 'en',
                                 in_values(profile_table.nrm_title,
                                           similar_titles),
                                 *sector_clause) \
                         .count()
            coincidenceq = cndb.query(skill_table.nrm_name, func.count()) \
                               .join(profile_table) \
                               .filter(profile_table.language == 'en',
                                       in_values(profile_table.nrm_title,
                                                 similar_titles),
                                       *sector_clause)
        else:
            titlec = cndb.query(profile_table.id) \
                         .join(Location,
                               Location.nrm_name == profile_table.nrm_location) \
                         .filter(profile_table.language == 'en',
                                 Location.nuts0 == 'UK',
                                 in_values(profile_table.nrm_curr_title,
                                           similar_titles),
                                 *sector_clause) \
                         .count()
            coincidenceq = cndb.query(skill_table.nrm_name, func.count()) \
                               .join(profile_table) \
                               .join(Location,
                                     Location.nrm_name == profile_table.nrm_location) \
                               .filter(profile_table.language == 'en',
                                       Location.nuts0 == 'UK',
                                       in_values(profile_table.nrm_curr_title,
                                                 similar_titles),
                                       *sector_clause)
        
        skillvector = {}
        for nrm_skill, skillc, titleskillc, _, _ in \
            relevance_scores(totalc, titlec, entitiesq, coincidenceq,
                             entitymap=mapper):
            if skillc < mincount:
                continue
            # skillvector[nrm_skill] = titleskillc/totalc*log(totalc/skillc)
            skillvector[nrm_skill] = titleskillc/totalc
        if skillvector:
            norm = sqrt(sum(v**2 for v in skillvector.values()))
            skillvector = dict((s, v/norm) for s, v in skillvector.items())
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
        = skillvectors(profile_table, skill_table, args.source, titles, args.mappings, args.min_count)

    with open(args.output_file, 'w') as outputfile:
        csvwriter = csv.writer(outputfile)
        for title, count, skillvector in zip(titles, titlecounts, skillvectors):
            csvwriter.writerow([
                't', title[0], title[1], int(title[2]), count])
            skillvector = list(skillvector.items())
            skillvector.sort(key=lambda x: -x[-1])
            for skill, frac in skillvector:
                csvwriter.writerow(['s', skill, frac])
