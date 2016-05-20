import conf
from analyticsdb import *
from logger import Logger
from textnormalization import normalized_entity, split_nrm_name
from analytics_get_entitycloud import relevance_scores
from entity_mapper import EntityMapper
from sqlalchemy import func
import csv
from math import sqrt
import argparse


def _iter_items(keys, d):
    for key in keys:
        if key in d:
            yield key, d[key]


def skillvectors(titles, mappings, mincount=1):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger()
    mapper = EntityMapper(andb, mappings)

    logger.log('Counting profiles.\n')
    totalc_nosf = andb.query(LIProfile.id) \
                      .join(Location) \
                      .filter(LIProfile.language == 'en',
                              Location.nuts0 == 'UK') \
                      .count()
    totalc_sf = andb.query(LIProfile.id) \
                    .join(Location) \
                    .filter(LIProfile.nrm_sector != None,
                            LIProfile.language == 'en',
                            Location.nuts0 == 'UK') \
                    .count()

    logger.log('Counting skills.\n')
    countcol = func.count().label('counts')
    q = andb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en') \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= mincount)
    skillcounts_nosf = dict(q)
    q = andb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_sector != None) \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= mincount)
    skillcounts_sf = dict(q)

    skillvectors = []
    newtitles = []
    titlecounts = []
    for sector, title, sector_filter in titles:
        logger.log('Processing: {0:s}\n'.format(title))
        nrm_title = normalized_entity('title', 'linkedin', 'en', title)
        nrm_sector = normalized_entity('sector', 'linkedin', 'en', sector)
        
        if sector_filter:
            totalc = totalc_sf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_sf)
            sector_clause = (
                LIProfile.nrm_sector.in_(mapper.inv(nrm_sector)),)
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
                for entity, _, _, _ in andb.find_entities(
                        tpe, source, language, words, normalize=False):
                    similar_titles.add(entity)
            
        titlec = andb.query(LIProfile.id) \
                     .join(Location) \
                     .filter(LIProfile.language == 'en',
                             Location.nuts0 == 'UK',
                             LIProfile.nrm_curr_title.in_(similar_titles),
                             *sector_clause) \
                     .count()
        coincidenceq = andb.query(LIProfileSkill.nrm_name, func.count()) \
                           .join(LIProfile) \
                           .join(Location) \
                           .filter(LIProfile.language == 'en',
                                   Location.nuts0 == 'UK',
                                   LIProfile.nrm_curr_title.in_(similar_titles),
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
            skillvector = dict((s, v/norm) for s,v in skillvector.items())
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

    titles, titlecounts, skillvectors \
        = skillvectors(titles, args.mappings, args.min_count)

    with open(args.output_file, 'w') as outputfile:
        csvwriter = csv.writer(outputfile)
        for title, count, skillvector in zip(titles, titlecounts, skillvectors):
            csvwriter.writerow([
                't', title[0], title[1], int(title[2]), count])
            skillvector = list(skillvector.items())
            skillvector.sort(key=lambda x: -x[-1])
            for skill, frac in skillvector:
                csvwriter.writerow(['s', skill, frac])

    
