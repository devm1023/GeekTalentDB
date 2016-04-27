import conf
from logger import Logger
from analyticsdb import *
from analytics_get_entitycloud import relevance_scores
from careerdefinitiondb import CareerDefinitionDB
from textnormalization import normalized_entity, normalized_sector
from entity_mapper import EntityMapper
from sqlalchemy import func
import csv
import argparse


def count_entities(q, mapper, nrm_sector=None, mincount=1, limit=None):
    if mapper is None:
        mapper = lambda x: x
    currentprofile = None
    lastenddate = None
    lastentity = None
    skip = False
    entitycounts = {}
    for profile_id, enddate, entity in q:
        entity = mapper(entity, nrm_sector=nrm_sector)
        if currentprofile is not None and profile_id != currentprofile:
            if lastentity is not None:
                entitycounts[lastentity] \
                    = entitycounts.get(lastentity, 0) + 1
            lastentity = None
            lastenddate = None
            skip = False
        currentprofile = profile_id
        if not entity or skip:
            continue
        if lastentity is not None and lastenddate is None:
            lastentity = None
            skip = True
            continue
        lastenddate = enddate
        lastentity = entity

    totalcount = sum(entitycounts.values())
    entitycounts = [(mapper.name(e, nrm_sector=nrm_sector), c) \
                    for e, c in entitycounts.items() \
                    if c >= mincount]
    entitycounts.sort(key=lambda x: -x[-1])
    if limit is not None and len(entitycounts) > limit:
        entitycounts = entitycounts[:limit]
    return totalcount, entitycounts

def get_skill_cloud(andb, mapper, profilec, titlec, titles, nrm_sector,
                    sigma, limit):
    countcol = func.count().label('counts')
    entityq = lambda entities: \
              andb.query(LIProfileSkill.nrm_name, countcol) \
                  .join(LIProfile) \
                  .join(Location)
                  .filter(Location.nuts0 == 'UK',
                          LIProfile.language == 'en',
                          LIProfileSkill.nrm_name.in_(entities))
                  .group_by(LIProfileSkill.nrm_name)
    coincidenceq = andb.query(LIProfileSkill.nrm_skill, countcol) \
                       .join(LIProfile) \
                       .join(Location)
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

def get_company_cloud(andb, mapper, profilec, titlec, titles, nrm_sector,
                      sigma, limit):
    countcol = func.count().label('counts')
    entityq = lambda entities: \
              andb.query(LIProfile.nrm_company, countcol) \
                  .join(Location)
                  .filter(Location.nuts0 == 'UK',
                          LIProfile.language == 'en',
                          LIProfile.nrm_company.in_(entities)) \
                  .group_by(LIProfile.nrm_company)
    coincidenceq = andb.query(LIProfile.nrm_company, countcol) \
                       .join(Location)
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

def get_career_steps(andb, mapper, nrm_sector, nrm_title, mincount=1,
                     limit=None):
    nrm_title = mapper(nrm_title, nrm_sector=nrm_sector)
    titles = mapper.inv(nrm_title, nrm_sector=nrm_sector)
    previous_titles = {}
    next_titles = {}
    previous_titles_total = 0
    next_titles_total = 0
    q = andb.query(LIProfile) \
            .join(LIExperience) \
            .filter(LIProfile.nrm_sector == nrm_sector,
                    LIExperience.nrm_title.in_(titles)) \
            .distinct()
    for liprofile in q:
        titles = []
        for experience in liprofile.experiences:
            if not experience.nrm_title or not experience.start:
                continue
            mapped_title = mapper(experience.nrm_title, nrm_sector=nrm_sector)
            if not titles or titles[-1] != mapped_title:
                titles.append(mapped_title)
        for i, title in enumerate(titles):
            if title != nrm_title:
                continue
            if i > 0:
                prev_title = titles[i-1]
                previous_titles[prev_title] \
                    = previous_titles.get(prev_title, 0) + 1
                previous_titles_total += 1
            if i < len(titles)-1:
                next_title = titles[i+1]
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

def get_subjects(andb, mapper, nrm_sector, profile_ids, mincount=1, limit=None):
    if not profile_ids:
        return 0, []
    q = andb.query(LIEducation.liprofile_id, LIEducation.end,
                   LIEducation.nrm_subject) \
            .filter(LIEducation.liprofile_id.in_(profile_ids)) \
            .order_by(LIEducation.liprofile_id, LIEducation.end)
    total_subject_count, subject_counts = count_entities(
        q, mapper, nrm_sector=nrm_sector, mincount=mincount, limit=limit)
    results = []
    for subject, count in subject_counts:
        results.append({'subject_name' : subject.strip(),
                        'count' : count,
                        'visible' : True})
    return total_subject_count, results

def get_institutes(andb, mapper, nrm_sector, profile_ids,
                   mincount=1, limit=None):
    if not profile_ids:
        return 0, []
    q = andb.query(LIEducation.liprofile_id, LIEducation.end,
                   LIEducation.nrm_institute) \
            .filter(LIEducation.liprofile_id.in_(profile_ids)) \
            .order_by(LIEducation.liprofile_id, LIEducation.end)
    total_institute_count, institute_counts = count_entities(
        q, mapper, nrm_sector=nrm_sector, mincount=mincount, limit=limit)
    results = []
    for institute, count in institute_counts:
        results.append({'institute_name' : institute.strip(),
                        'count' : count,
                        'visible' : True})
    return total_institute_count, results

def get_sectors(sectors, filename, mapper):
    sectors = [mapper(normalized_sector(s)) for s in sectors]
    if filename:
        with open(filename, 'r') as sectorfile:
            for line in sectorfile:
                row = line.split('|')
                if not row:
                    continue
                sector = mapper(normalized_sector(row[0]))
                if not sector:
                    continue
                if sector not in sectors:
                    sectors.append(sector)
    return sectors


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
    logger.log('TOTAL PROFILE COUNT: {0:d}\n'.format(experiencec))

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
                   if split_nrm_name(t)[0] != 'title']

    for career, nrm_career in careers:
        # get job titles for career
        titles = []
        for nrm_title in mapper.inv(nrm_career, nrm_sector=nrm_sector):
            titles.append(nrm_title)
            tpe, source, language, words = split_nrm_name(nrm_title)
            if len(words.split()) > 1:
                for entity, _, _, _ in andb.find_entities(
                        tpe, source, language, words, normalize=False):
                    titles.append(entity)

        # count total number of people in this career
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
        
        careerdict['skill_cloud'] \
            = get_skill_cloud(andb, mapper, profilec, titlec, titles,
                              nrm_sector, args.sigma, args.max_skills)
        careerdict['company_cloud'] \
            = get_company_cloud(andb, mapper, profilec, titlec, titles,
                                nrm_sector, args.sigma, args.max_companies)

        q = andb.query(LIProfile.id) \
                .join(Location) \
                .filter(Location.nuts0 == 'UK',
                        LIProfile.language == 'en',
                        LIProfile.nrm_curr_title.in_(titles)) \
                .distinct()
        profile_ids = [id for id, in q]
        (careerdict['education_subjects_total'],
         careerdict['education_subjects']) = get_subjects(
             andb, mapper, nrm_sector, profile_ids,
             args.min_subject_count, args.max_subjects)
        (careerdict['education_institutes_total'],
         careerdict['education_institutes']) = get_institutes(
             andb, mapper, nrm_sector, profile_ids,
             args.min_institute_count, args.max_institutes)

        (careerdict['previous_titles_total'], careerdict['previous_titles'],
         careerdict['next_titles_total'], careerdict['next_titles']) \
         = get_career_steps(andb, mapper, nrm_sector, nrm_title,
                            args.min_careerstep_count, args.max_careersteps)

        cddb.add_career(careerdict, get_descriptions=args.get_descriptions)
        cddb.commit()
