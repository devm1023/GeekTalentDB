import conf
from logger import Logger
from canonicaldb import *
from entitycloud import entity_cloud, relevance_score
from textnormalization import normalized_entity, make_nrm_name, split_nrm_name
from windowquery import collapse
from pgvalues import in_values
from careerdefinitiondb import CareerDefinitionDB, Sector
from entity_mapper import EntityMapper
from sqlalchemy import func, or_
import csv
import argparse


def _get_items(keys, d):
    for key in keys:
        if key in d:
            yield key, d[key]


def _merged_entity_cloud(
        cloud1, name_field_1, total_count_field_1, category_count_field_1,
        entity_count_field_1, count_field_1,
        cloud2, name_field_2, total_count_field_2, category_count_field_2,
        entity_count_field_2, count_field_2):
    entities = {}
    for entity1 in cloud1:
        entities[entity1[name_field_1]] = {
            name_field_1           : entity1[name_field_1],
            total_count_field_1    : entity1[total_count_field_1],
            category_count_field_1 : entity1[category_count_field_1],
            entity_count_field_1   : entity1[entity_count_field_1],
            count_field_1          : entity1[count_field_1]}
    for entity2 in cloud2:
        name = entity2[name_field_2]
        if name in entities:
            entity = entities[name]
            if entity[total_count_field_1] != entity2[total_count_field_2]:
                raise ValueError('Fields {0:s} and {1:s} must match.' \
                                 .format(total_count_field_1,
                                         total_count_field_2))
            if entity[entity_count_field_1] != entity2[entity_count_field_2]:
                raise ValueError('Fields {0:s} and {1:s} must match.' \
                                 .format(entity_count_field_1,
                                         entity_count_field_2))
            entity[category_count_field_1] += entity2[category_count_field_2]
            entity[count_field_1] += entity2[count_field_2]
        else:
            entities[name] = {
                name_field_1           : entity2[name_field_2],
                total_count_field_1    : entity2[total_count_field_2],
                category_count_field_1 : entity2[category_count_field_2],
                entity_count_field_1   : entity2[entity_count_field_2],
                count_field_1          : entity2[count_field_2]}
            
    entities = list(entities.values())
    for entity in entities:
        entity['relevance_score'], _ = relevance_score(
            entity[total_count_field_1], entity[category_count_field_1],
            entity[entity_count_field_1], entity[count_field_1])
    entities.sort(key=lambda e: e['relevance_score'])
    return entities


def get_skill_cloud(cndb, mapper, nrm_sector, profilec, categoryc,
                    skillcounts, titles_nosf, titles_sf, sigma, limit):
    countcol = func.count().label('counts')
    entityq = lambda entities: _get_items(entities, skillcounts)
    coincidenceq = cndb.query(LIProfileSkill.nrm_name, countcol) \
                       .join(LIProfile) \
                       .join(Location,
                             Location.nrm_name == LIProfile.nrm_location) \
                       .filter(Location.nuts0 == 'UK',
                               LIProfile.language == 'en')
    sectors = mapper.inv(nrm_sector)
    if titles_nosf is not None:
        titles_filter = []
        if titles_nosf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_nosf))
        if titles_sf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_sf) & \
                                 LIProfile.nrm_sector.in_(sectors))
        coincidenceq = coincidenceq.filter(or_(*titles_filter))
        entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
        category_count_col = 'title_count'
    else:
        coincidenceq = coincidenceq.filter(LIProfile.nrm_sector.in_(sectors))
        entitymap = mapper
        category_count_col = 'sector_count'
        
    skillcloud = entity_cloud(profilec, categoryc, entityq, coincidenceq,
                              sigma=sigma, limit=limit, entitymap=entitymap)
    result = []
    for skill, skillc, count, score, _ in skillcloud:
        result.append({
            'skill_name' : mapper.name(skill),
            'total_count' : profilec,
            category_count_col : categoryc,
            'skill_count' : skillc,
            'count' : count,
            'relevance_score' : score,
            'visible' : True,
        })
    return result


def get_company_cloud(cndb, mapper, nrm_sector, profilec, categoryc,
                      companycounts, titles_nosf, titles_sf, sigma, limit):
    countcol = func.count().label('counts')
    entityq = lambda entities: _get_items(entities, companycounts)
    coincidenceq = cndb.query(LIProfile.nrm_company, countcol) \
                       .join(Location,
                             Location.nrm_name == LIProfile.nrm_location) \
                       .filter(Location.nuts0 == 'UK',
                               LIProfile.language == 'en',
                               LIProfile.nrm_company != None)
    sectors = mapper.inv(nrm_sector)
    if titles_nosf is not None:
        titles_filter = []
        if titles_nosf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_nosf))
        if titles_sf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_sf) & \
                                 LIProfile.nrm_sector.in_(sectors))
        coincidenceq = coincidenceq.filter(or_(*titles_filter))
        entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
        category_count_col = 'title_count'
    else:
        coincidenceq = coincidenceq.filter(LIProfile.nrm_sector.in_(sectors))
        entitymap = mapper
        category_count_col = 'sector_count'
        
    companycloud = entity_cloud(profilec, categoryc, entityq, coincidenceq,
                                sigma=sigma, limit=limit, entitymap=entitymap)
    result = []
    for company, companyc, count, score, _ in companycloud:
        result.append({
            'company_name' : mapper.name(company),
            'total_count' : profilec,
            category_count_col : categoryc,
            'company_count' : companyc,
            'count' : count,
            'relevance_score' : score,
            'visible' : True,
        })
    return result


def get_career_steps(cndb, mapper, nrm_sector, nrm_title, titles_nosf,
                     titles_sf, mincount, limit):
    nrm_sector = mapper(nrm_sector)
    previous_titles = {}
    next_titles = {}
    previous_titles_total = 0
    next_titles_total = 0
    q = cndb.query(LIProfile.id,
                   LIProfile.nrm_sector,
                   LIExperience.nrm_title) \
            .join(LIExperience) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIExperience.nrm_title != None,
                    LIExperience.start != None) \
            .order_by(LIProfile.id, LIExperience.start)
    for id, sector, experience_titles in collapse(q, 2):
        mapped_titles = []
        mapped_sector = mapper(sector)
        for title, in experience_titles:
            if (title in titles_nosf) or \
               (title in titles_sf and mapped_sector == nrm_sector):
                mapped_title = nrm_title
            else:
                mapped_title = mapper(title, nrm_sector=mapped_sector)
            if not mapped_title:
                continue
            if not mapped_titles or mapped_titles[-1] != mapped_title:
                mapped_titles.append(mapped_title)
        for i, title in enumerate(mapped_titles):
            if title != nrm_title:
                continue
            if i > 0:
                prev_title = mapped_titles[i-1]
                previous_titles[prev_title] \
                    = previous_titles.get(prev_title, 0) + 1
                previous_titles_total += 1
            if i < len(mapped_titles)-1:
                next_title = mapped_titles[i+1]
                next_titles[next_title] = next_titles.get(next_title, 0) + 1
                next_titles_total += 1
    previous_titles = [
        {'previous_title' : mapper.name(job),
         'count' : count,
         'visible' : True} \
        for job, count in previous_titles.items() if count >= mincount]
    previous_titles.sort(key=lambda t: -t['count'])
    if limit is not None and len(previous_titles) > limit:
        previous_titles = previous_titles[:limit]
    next_titles = [
        {'next_title' : mapper.name(job),
         'count' : count,
         'visible' : True} \
        for job, count in next_titles.items() if count >= mincount]
    next_titles.sort(key=lambda t: -t['count'])
    if limit is not None and len(next_titles) > limit:
        next_titles = next_titles[:limit]

    return previous_titles_total, previous_titles, \
        next_titles_total, next_titles


def get_subjects(cndb, mapper, nrm_sector, titles_nosf, titles_sf,
                 mincount, limit):
    sectors = mapper.inv(nrm_sector)
    countcol = func.count().label('counts')
    q = cndb.query(LIProfile.nrm_last_subject, countcol) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_last_subject != None)
    if titles_nosf is not None:
        titles_filter = []
        if titles_nosf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_nosf))
        if titles_sf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_sf) & \
                                 LIProfile.nrm_sector.in_(sectors))
        q = q.filter(or_(*titles_filter))
    else:
        q = q.filter(LIProfile.nrm_sector.in_(sectors))
    q = q.group_by(LIProfile.nrm_last_subject) \
         .having(countcol >= mincount) \
         .order_by(countcol.desc())

    counts = {}
    total_subject_count = 0
    for subject, count in q:
        subject = mapper(subject, nrm_sector=nrm_sector)
        if not subject:
            continue
        total_subject_count += count
        counts[subject] = counts.get(subject, 0) + count
    counts = list(counts.items())
    counts.sort(key=lambda x: -x[-1])
    if len(counts) > limit:
        counts = counts[:limit]

    results = [{'subject_name' : mapper.name(subject),
                'count' : count,
                'visible' : True} for subject, count in counts]
    return total_subject_count, results


def get_institutes(cndb, mapper, nrm_sector, titles_nosf, titles_sf,
                   mincount, limit):
    sectors = mapper.inv(nrm_sector)
    countcol = func.count().label('counts')
    q = cndb.query(LIProfile.nrm_last_institute, countcol) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_last_institute != None) \
            .group_by(LIProfile.nrm_last_institute) \
            .having(countcol >= mincount) \
            .order_by(countcol.desc())
    if titles_nosf is not None:
        titles_filter = []
        if titles_nosf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_nosf))
        if titles_sf:
            titles_filter.append(in_values(LIProfile.nrm_curr_title,
                                           titles_sf) & \
                                 LIProfile.nrm_sector.in_(sectors))
        q = q.filter(or_(*titles_filter))
    else:
        q = q.filter(LIProfile.nrm_sector.in_(sectors))
    q = q.group_by(LIProfile.nrm_last_institute) \
         .having(countcol >= mincount) \
         .order_by(countcol.desc())

    counts = {}
    total_institute_count = 0
    for institute, count in q:
        institute = mapper(institute, nrm_sector=nrm_sector)
        if not institute:
            continue
        total_institute_count += count
        counts[institute] = counts.get(institute, 0) + count
    counts = list(counts.items())
    counts.sort(key=lambda x: -x[-1])
    if len(counts) > limit:
        counts = counts[:limit]
    
    results = [{'institute_name' : mapper.name(institute),
                'count' : count,
                'visible' : True} for institute, count in counts]
    return total_institute_count, results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mappings',
                        help='Name of a csv file holding entity mappings. '
                        'Columns: type | lang | sector | name | mapped name')
    parser.add_argument('--careers',
                        help='Name of the CSV file holding the careers. '
                        'If absent, only sector-level stats are generated. '
                        'Columns: sector,use sector filter (0/1),title')
    parser.add_argument('--max-entities', type=int, default=50,
                        help='Maximum number of entities in clouds. (Default: 50)')
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for an object to be included '
                        'in a list. (Default: 1)')
    parser.add_argument('--max-skills', type=int,
                        help='Maximum number of skills in skill clouds.')
    parser.add_argument('--max-companies', type=int,
                        help='Maximum number of companies in compnay clouds.')
    parser.add_argument('--max-institutes', type=int,
                        help='Maximum number of institutes in a list.')
    parser.add_argument('--max-subjects', type=int,
                        help='Maximum number of subjects in a list.')
    parser.add_argument('--max-careersteps', type=int,
                        help='Maximum number of career steps in a list.')
    parser.add_argument('--min-skill-count', type=int,
                        help='Minimum count for skills.')
    parser.add_argument('--min-company-count', type=int,
                        help='Minimum count for companies.')
    parser.add_argument('--min-institute-count', type=int,
                        help='Minimum count for educational institutes '
                        'to be added to the list.')
    parser.add_argument('--min-subject-count', type=int,
                        help='Minimum count for education subjects '
                        'to be added to the list.')
    parser.add_argument('--min-careerstep-count', type=int,
                        help='Minimum count for career steps '
                        'to be added to the list.')
    parser.add_argument('--sigma', type=int, default=3,
                        help='Minimal significance of relevance scores. (Default: 3)')
    parser.add_argument('sector',
                        help='Name of the sector to populate')
    args = parser.parse_args()

    if args.max_skills is None:
        args.max_skills = args.max_entities
    if args.max_companies is None:
        args.max_companies = args.max_entities
    if args.max_institutes is None:
        args.max_institutes = args.max_entities
    if args.max_subjects is None:
        args.max_subjects = args.max_entities
    if args.max_careersteps is None:
        args.max_careersteps = args.max_entities

    if args.min_skill_count is None:
        args.min_skill_count = args.min_count
    if args.min_company_count is None:
        args.min_company_count = args.min_count
    if args.min_institute_count is None:
        args.min_institute_count = args.min_count
    if args.min_subject_count is None:
        args.min_subject_count = args.min_count
    if args.min_careerstep_count is None:
        args.min_careerstep_count = args.min_count


    logger = Logger()
    cndb = CanonicalDB(conf.CANONICAL_DB)
    cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)
    mapper = EntityMapper(cndb, args.mappings)

    nrm_sector = normalized_entity('sector', 'linkedin', 'en', args.sector)
    sectors = mapper.inv(nrm_sector)

    # get total number of profiles
    profilec_nosf = cndb.query(LIProfile.id) \
                   .join(Location,
                         Location.nrm_name == LIProfile.nrm_location) \
                   .filter(Location.nuts0 == 'UK',
                           LIProfile.language == 'en') \
                   .count()
    logger.log('TOTAL PROFILE COUNT: {0:d}\n'.format(profilec_nosf))
    profilec_sf = cndb.query(LIProfile.id) \
                   .join(Location,
                         Location.nrm_name == LIProfile.nrm_location) \
                   .filter(Location.nuts0 == 'UK',
                           LIProfile.language == 'en',
                           LIProfile.nrm_sector != None) \
                   .count()
    logger.log('PROFILES WITH SECTOR: {0:d}\n'.format(profilec_sf))

    # get total skill and company counts
    countcol = func.count().label('counts')
    logger.log('Counting skills.\n')
    q = cndb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en') \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= args.min_skill_count)
    skillcounts_nosf = dict(q)
    q = cndb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_sector.in_(mapper.inv(nrm_sector))) \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= args.min_skill_count)
    skillcounts_sf = dict(q)
    logger.log('Counting companies.\n')
    q = cndb.query(LIProfile.nrm_company, countcol) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en') \
            .group_by(LIProfile.nrm_company) \
            .having(countcol >= args.min_company_count)
    companycounts_nosf = dict(q)
    q = cndb.query(LIProfile.nrm_company, countcol) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_sector.in_(mapper.inv(nrm_sector))) \
            .group_by(LIProfile.nrm_company) \
            .having(countcol >= args.min_company_count)
    companycounts_sf = dict(q)

    # assemble sector information
    logger.log('\nGetting sector information.\n')
    logger.log('Counting profiles.\n')
    sectorc = cndb.query(LIProfile.id) \
                       .join(Location,
                             Location.nrm_name == LIProfile.nrm_location) \
                       .filter(Location.nuts0 == 'UK',
                               LIProfile.language == 'en',
                               LIProfile.nrm_sector.in_(
                                   mapper.inv(nrm_sector))) \
                       .count()
    countcol = func.count().label('counts')
    logger.log('Counting skills.\n')
    q = cndb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_sector != None) \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= args.min_skill_count)
    sectorskillcounts = dict(q)
    logger.log('Counting companies.\n')
    q = cndb.query(LIProfile.nrm_company, countcol) \
            .join(Location,
                  Location.nrm_name == LIProfile.nrm_location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_sector != None) \
            .group_by(LIProfile.nrm_company) \
            .having(countcol >= args.min_company_count)
    sectorcompanycounts = dict(q)
    
    sectordict = {
        'name' : mapper.name(nrm_sector),
        'count' : sectorc,
        'total_count' : profilec_sf,
        'visible' : True,
    }
    logger.log('Building skill cloud.\n')
    sectordict['skill_cloud'] = get_skill_cloud(
        cndb, mapper, nrm_sector, profilec_sf, sectorc, sectorskillcounts,
        None, None, args.sigma, args.max_skills)
    logger.log('Building company cloud.\n')
    sectordict['company_cloud'] = get_company_cloud(
        cndb, mapper, nrm_sector, profilec_sf, sectorc, sectorcompanycounts,
        None, None, args.sigma, args.max_companies)
    logger.log('Counting subjects.\n')
    (sectordict['education_subjects_total'],
     sectordict['education_subjects']) = get_subjects(
         cndb, mapper, nrm_sector, None, None,
         args.min_subject_count, args.max_subjects)
    logger.log('Counting institutes.\n')
    (sectordict['education_institutes_total'],
     sectordict['education_institutes']) = get_institutes(
         cndb, mapper, nrm_sector, None, None,
         args.min_institute_count, args.max_institutes)
    sector = cddb.add_from_dict(sectordict, Sector)
    cddb.commit()
    sector_id = sector.id


    # get list of careers to add
    careers = []
    ch_sectors = {}
    with open(args.careers, 'r') as csvfile:
        csvreader = csv.reader(row for row in csvfile \
                               if not row.strip().startswith('#'))
        sector_ids = {args.sector : sector_id}
        for row in csvreader:
            if not row:
                continue
            try:
                sector_field = row[0]
                career = row[1].strip()
                ch_sector_name = None
                if len(row) > 2 and row[2].strip():
                    ch_sector_name = row[2].strip()
                    if ch_sector_name not in ch_sectors:
                        ch_sectors[ch_sector_name] = {
                            'name' : ch_sector_name,
                            'total_count' : profilec_sf,
                            'count' : 0,
                            'visible' : True
                        }
                        cddb.add_from_dict(ch_sectors[ch_sector_name],
                                           Sector, flush=True)
            except (ValueError, IndexError):
                raise IOError('Invalid row in careers file: {0:s}' \
                              .format(str(row)))
            if normalized_entity('sector', 'linkedin', 'en', sector_field) \
               != nrm_sector:
                continue

            nrm_career = normalized_entity('title', 'linkedin', 'en', career)
            careers.append((career, nrm_career, ch_sector_name))

    for career, nrm_career, ch_sector_name in careers:
        logger.log('\nProcessing career: {0:s}\n'.format(career))

        profilec = profilec_nosf
        skillcounts = skillcounts_nosf
        companycounts = companycounts_nosf
        
        # get job titles for career
        logger.log('Getting job titles...')
        titles_sf = set()
        for nrm_title in mapper.inv(nrm_career, nrm_sector=nrm_sector,
                                    sector_specific=True):
            titles_sf.add(nrm_title)
            tpe, source, language, words = split_nrm_name(nrm_title)
            if len(words.split()) > 1:
                for entity, _, _, _ in cndb.find_entities(
                        tpe, source, language, words, normalize=False):
                    titles_sf.add(entity)
        titles_nosf = set()
        for nrm_title in mapper.inv(nrm_career, nrm_sector=nrm_sector):
            if nrm_title in titles_sf:
                continue
            titles_nosf.add(nrm_title)
            tpe, source, language, words = split_nrm_name(nrm_title)
            if len(words.split()) > 1:
                for entity, _, _, _ in cndb.find_entities(
                        tpe, source, language, words, normalize=False):
                    titles_nosf.add(entity)
        logger.log('{0:d} specific, {1:d} unspecific found.\n' \
                   .format(len(titles_nosf), len(titles_sf)))

        # count total number of people in this career
        logger.log('Counting people...')
        q = cndb.query(LIProfile.nrm_sector, LIProfile.nrm_curr_title) \
                .join(Location,
                      Location.nrm_name == LIProfile.nrm_location) \
                .filter(Location.nuts0 == 'UK',
                        LIProfile.language == 'en')
        titlec = 0
        for s, t in q:
            if t in titles_nosf or (s in sectors and t in titles_sf):
                titlec += 1
        logger.log('{0:d} found.\n'.format(titlec))

        # accumulate counts for sector
        if ch_sector_name:
            ch_sectors[ch_sector_name]['count'] += titlec
        
        # initialise career dict
        ch_sector_id = sector_id
        if ch_sector_name:
            ch_sector_id = ch_sectors[ch_sector_name]['id']        
        careerdict = {'title' : career,
                      'sector_id' : ch_sector_id,
                      'total_count' : profilec,
                      'sector_count' : None,
                      'title_count' : None,
                      'count' : titlec,
                      'relevance_score' : None,
                      'visible' : True,
        }
        
        logger.log('Building skill cloud.\n')
        careerdict['skill_cloud'] \
            = get_skill_cloud(cndb, mapper, nrm_sector,
                              profilec, titlec, skillcounts, titles_nosf,
                              titles_sf, args.sigma, args.max_skills)
        if ch_sector_name:
            ch_sectors[ch_sector_name]['skill_cloud'] \
                = _merged_entity_cloud(
                    ch_sectors[ch_sector_name].get('skill_cloud', []),
                    'skill_name', 'total_count', 'sector_count', 'skill_count',
                    'count',
                    careerdict['skill_cloud'],
                    'skill_name', 'total_count', 'title_count', 'skill_count',
                    'count')
        
        logger.log('Building company cloud.\n')
        careerdict['company_cloud'] \
            = get_company_cloud(cndb, mapper, nrm_sector,
                                profilec, titlec, companycounts, titles_nosf,
                                titles_sf, args.sigma, args.max_companies)

        logger.log('Counting education subjects.\n')
        (careerdict['education_subjects_total'],
         careerdict['education_subjects']) = get_subjects(
             cndb, mapper, nrm_sector, titles_nosf, titles_sf,
             args.min_subject_count, args.max_subjects)
        logger.log('Counting educational institutes.\n')
        (careerdict['education_institutes_total'],
         careerdict['education_institutes']) = get_institutes(
             cndb, mapper, nrm_sector, titles_nosf, titles_sf,
             args.min_institute_count, args.max_institutes)

        logger.log('Building career steps.\n')
        (careerdict['previous_titles_total'], careerdict['previous_titles'],
         careerdict['next_titles_total'], careerdict['next_titles']) \
         = get_career_steps(
             cndb, mapper, nrm_sector, nrm_career, titles_nosf, titles_sf,
             args.min_careerstep_count, args.max_careersteps)

        cddb.add_career(careerdict)
        cddb.commit()

    for ch_sector_dict in ch_sectors.values():
        for skill_dict in ch_sector_dict.get('skill_cloud', []):
            skill_dict['visible'] = True
        cddb.add_from_dict(ch_sector_dict, Sector)
    cddb.commit()
