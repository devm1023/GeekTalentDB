import conf
from logger import Logger
from analyticsdb import *
from analytics_get_entitycloud import entity_cloud
from textnormalization import normalized_entity, make_nrm_name, split_nrm_name
from careerdefinitiondb import CareerDefinitionDB
from entity_mapper import EntityMapper
from sqlalchemy import func
import csv
import argparse


def _get_items(keys, d):
    for key in keys:
        if key in d:
            yield key, d[key]

def get_skill_cloud(andb, mapper, nrm_sector, profilec, titlec,
                    skillcounts, titles, sigma, limit):
    countcol = func.count().label('counts')
    entityq = lambda entities: _get_items(entities, skillcounts)
    coincidenceq = andb.query(LIProfileSkill.nrm_name, countcol) \
                       .join(LIProfile) \
                       .join(Location) \
                       .filter(Location.nuts0 == 'UK',
                               LIProfile.language == 'en',
                               LIProfile.nrm_curr_title.in_(titles))
    entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
    skillcloud = entity_cloud(profilec, titlec, entityq, coincidenceq,
                              sigma=sigma, limit=limit, entitymap=entitymap)
    result = []
    for skill, skillc, titleskillc, score, _ in skillcloud:
        result.append({
            'skill_name' : mapper.name(skill, nrm_sector=nrm_sector),
            'total_count' : profilec,
            'title_count' : titlec,
            'skill_count' : skillc,
            'count' : titleskillc,
            'relevance_score' : score,
            'visible' : True,
        })
    return result

def get_company_cloud(andb, mapper, nrm_sector, profilec, titlec,
                      companycounts, titles, sigma, limit):
    countcol = func.count().label('counts')
    entityq = lambda entities: _get_items(entities, companycounts)
    coincidenceq = andb.query(LIProfile.nrm_company, countcol) \
                       .join(Location) \
                       .filter(Location.nuts0 == 'UK',
                               LIProfile.language == 'en',
                               LIProfile.nrm_curr_title.in_(titles))
    entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
    companycloud = entity_cloud(profilec, titlec, entityq, coincidenceq,
                                sigma=sigma, limit=limit, entitymap=entitymap)
    result = []
    for company, companyc, titlecompanyc, score, _ in companycloud:
        result.append({
            'company_name' : mapper.name(company, nrm_sector=nrm_sector),
            'total_count' : profilec,
            'title_count' : titlec,
            'company_count' : companyc,
            'count' : titlecompanyc,
            'relevance_score' : score,
            'visible' : True,
        })
    return result

def get_career_steps(andb, mapper, nrm_sector, nrm_title, titles, mincount=1,
                     limit=None):
    previous_titles = {}
    next_titles = {}
    previous_titles_total = 0
    next_titles_total = 0
    q = andb.query(LIProfile.id,
                   LIExperience.nrm_title) \
            .join(LIExperience) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIExperience.nrm_title.in_(titles),
                    LIExperience.start != None) \
            .order_by(LIProfile.id, LIExperience.start)
    for _, experience_titles in collapse(q):
        mapped_titles = []
        for title, in experience_titles:
            if title in titles:
                mapped_title = nrm_title
            else:
                mapped_title = mapper(title, nrm_sector=nrm_sector)
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
        {'previous_title' : mapper.name(job, nrm_sector=nrm_sector),
         'count' : count,
         'visible' : True} \
        for job, count in previous_titles.items() if count >= mincount]
    previous_titles.sort(key=lambda t: -t['count'])
    if limit is not None and len(previous_titles) > limit:
        previous_titles = previous_titles[:limit]
    next_titles = [
        {'next_title' : mapper.name(job, nrm_sector=nrm_sector),
         'count' : count,
         'visible' : True} \
        for job, count in next_titles.items() if count >= mincount]
    next_titles.sort(key=lambda t: -t['count'])
    if limit is not None and len(next_titles) > limit:
        next_titles = next_titles[:limit]

    return previous_titles_total, previous_titles, \
        next_titles_total, next_titles

def get_subjects(andb, mapper, nrm_sector, titles, mincount=1, limit=None):
    countcol = func.count().label('counts')
    q = andb.query(LIProfile.nrm_last_subject, countcol) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_curr_title.in_(titles),
                    LIProfile.nrm_last_subject != None) \
            .group_by(LIProfile.nrm_last_subject) \
            .having(countcol >= mincount) \
            .order_by(countcol.desc())
    results = []
    total_subject_count = 0
    for subject, count in q:
        total_subject_count += count
        if limit is None or len(results) < limit:
            results.append(
                {'subject_name' : mapper.name(subject, nrm_sector=nrm_sector),
                 'count' : count,
                 'visible' : True})
    return total_subject_count, results

def get_institutes(andb, mapper, nrm_sector, titles, mincount=1, limit=None):
    countcol = func.count().label('counts')
    q = andb.query(LIProfile.nrm_last_institute, countcol) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_curr_title.in_(titles),
                    LIProfile.nrm_last_institute != None) \
            .group_by(LIProfile.nrm_last_institute) \
            .having(countcol >= mincount) \
            .order_by(countcol.desc())
    results = []
    total_institute_count = 0
    for institute, count in q:
        total_institute_count += count
        if limit is None or len(results) < limit:
            results.append(
                {'institute_name' : mapper.name(institute,
                                                nrm_sector=nrm_sector),
                 'count' : count,
                 'visible' : True})
    return total_institute_count, results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sector',
                        help='Name of the sector to populate')
    parser.add_argument('--careers',
                        help='Name of file holding careers. If absent, the'
                        'titles from the mappings file are used.')
    parser.add_argument('--mappings',
                        help='Name of a csv file holding entity mappings. '
                        'Columns: type | lang | sector | name | mapped name')
    parser.add_argument('--max-entities', type=int, default=25,
                        help='Maximum number of entities in clouds.')
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for an object to be included '
                        'in a list.')
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
                        help='Minimal significance of relevance scores.')
    parser.add_argument('--get-descriptions', action='store_true',
                        help='Retreive entity descriptions from IBM Watson.')
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
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)
    mapper = EntityMapper(andb, args.mappings)

    nrm_sector = normalized_entity('sector', 'linkedin', 'en', args.sector)

    # get total number of profiles
    profilec = andb.query(LIProfile.id) \
                   .join(Location) \
                   .filter(Location.nuts0 == 'UK',
                           LIProfile.language == 'en') \
                   .count()
    logger.log('TOTAL PROFILE COUNT: {0:d}\n'.format(profilec))

    # get total skill and company counts
    countcol = func.count().label('counts')
    logger.log('Counting skills.\n')
    q = andb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en') \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= args.min_skill_count)
    skillcounts = dict(q)
    logger.log('Counting companies.\n')
    q = andb.query(LIProfile.nrm_company, countcol) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en') \
            .group_by(LIProfile.nrm_company) \
            .having(countcol >= args.min_company_count)
    companycounts = dict(q)

    # get list of careers to add
    if args.careers:
        careers = []
        with open(args.careers, 'r') as inputfile:
            for line in inputfile:
                career = line.strip()
                nrm_career = normalized_entity(
                    'title', 'linkedin', 'en', career)
                careers.append((career, nrm_career))
    else:
        careers = [(mapper.name(t), t) for t in mapper \
                   if split_nrm_name(t)[0] == 'title']

    for career, nrm_career in careers:
        logger.log('\nProcessing career: {0:s}\n'.format(career))
        
        # get job titles for career
        logger.log('Getting job titles.\n')
        titles = set()
        for nrm_title in mapper.inv(nrm_career, nrm_sector=nrm_sector):
            titles.add(nrm_title)
            tpe, source, language, words = split_nrm_name(nrm_title)
            if len(words.split()) > 1:
                for entity, _, _, _ in andb.find_entities(
                        tpe, source, language, words, normalize=False):
                    titles.add(entity)

        # count total number of people in this career
        logger.log('Counting people.\n')
        titlec = andb.query(LIProfile.id) \
                     .join(Location) \
                     .filter(Location.nuts0 == 'UK',
                             LIProfile.language == 'en',
                             LIProfile.nrm_curr_title.in_(titles)) \
                     .count()

        careerdict = {'title' : career,
                      'linkedin_sector' : args.sector,
                      'total_count' : profilec,
                      'sector_count' : None,
                      'title_count' : None,
                      'count' : titlec,
                      'relevance_score' : None,
                      'visible' : True,
        }
        
        logger.log('Building skill cloud.\n')
        careerdict['skill_cloud'] \
            = get_skill_cloud(andb, mapper, nrm_sector,
                              profilec, titlec, skillcounts, titles,
                              args.sigma, args.max_skills)
        logger.log('Building company cloud.\n')
        careerdict['company_cloud'] \
            = get_company_cloud(andb, mapper, nrm_sector,
                                profilec, titlec, companycounts, titles,
                                args.sigma, args.max_companies)

        logger.log('Counting education subjects.\n')
        (careerdict['education_subjects_total'],
         careerdict['education_subjects']) = get_subjects(
             andb, mapper, nrm_sector, titles,
             args.min_subject_count, args.max_subjects)
        logger.log('Counting educational institutes.\n')
        (careerdict['education_institutes_total'],
         careerdict['education_institutes']) = get_institutes(
             andb, mapper, nrm_sector, titles,
             args.min_institute_count, args.max_institutes)

        logger.log('Building career steps.\n')
        (careerdict['previous_titles_total'], careerdict['previous_titles'],
         careerdict['next_titles_total'], careerdict['next_titles']) \
         = get_career_steps(andb, mapper, nrm_sector, nrm_title, titles,
                            args.min_careerstep_count, args.max_careersteps)

        cddb.add_career(careerdict, get_descriptions=args.get_descriptions)
        cddb.commit()
