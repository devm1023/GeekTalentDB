import conf
from logger import Logger
from analyticsdb import *
from analytics_get_entitycloud import relevance_scores
from careerdefinitiondb import CareerDefinitionDB
from textnormalization import normalized_entity, normalized_sector
from sqlalchemy import func
import csv
import argparse


class EntityMapper:
    def __init__(self, db, filename):
        self._db = db
        self._nrm_maps = {None : {}}
        self._inv_nrm_maps = {None : {}}
        self._names = {}
        if not filename:
            return
        with open(filename, 'r') as csvfile:
            csvreader = csv.reader(row for row in csvfile \
                                   if not row.strip().startswith('#'))
            rowcount = 0
            for row in csvreader:
                rowcount += 1
                if len(row) != 5:
                    raise IOError('Invalid row in CSV file:\n{0:s}' \
                                  .format(repr(row)))
                type = row[0].strip()
                language = row[1].strip()
                sector = row[2].strip()
                if not sector or type == 'sector':
                    sector = None
                entity1 = row[3].strip()
                entity2 = row[4].strip()
                nrm_sector = normalized_entity('sector', 'linkedin', language,
                                               sector)
                if nrm_sector not in self._nrm_maps:
                    self._nrm_maps[nrm_sector] = {}
                    self._inv_nrm_maps[nrm_sector] = {}
                nrm_map = self._nrm_maps[nrm_sector]
                inv_nrm_map = self._inv_nrm_maps[nrm_sector]
                nrm_entity1 = normalized_entity(type, 'linkedin', language,
                                              entity1)
                nrm_entity2 = normalized_entity(type, 'linkedin', language,
                                              entity2)
                if not nrm_entity1 or not nrm_entity2:
                    raise IOError('Invalid row in CSV file:\n{0:s}' \
                                  .format(repr(row)))
                if nrm_entity1 in nrm_map:
                    raise IOError('Duplicate entry in row in CSV file:\n{0:s}' \
                                  .format(repr(row)))
                nrm_map[nrm_entity1] = nrm_entity2
                if nrm_entity2 not in inv_nrm_map:
                    inv_nrm_map[nrm_entity2] = set()
                inv_nrm_map[nrm_entity2].add(nrm_entity1)
                self._names[nrm_sector, nrm_entity2] = entity2

    def __call__(self, entity, sector=None, nrm_sector=None, language='en'):
        if sector is not None:
            nrm_sector = normalized_entity('sector', 'linkedin', language,
                                           sector)
        if nrm_sector in self._nrm_maps[None]:
            nrm_sector = self._nrm_maps[None][nrm_sector]
        if nrm_sector is not None and nrm_sector in self._nrm_maps:
            nrm_map = self._nrm_maps[nrm_sector]
            if entity in nrm_map:
                return nrm_map[entity]
        nrm_map = self._nrm_maps[None]
        if entity in nrm_map:
            return nrm_map[entity]
        return entity

    def inv(self, entity, sector=None, nrm_sector=None, language='en'):
        if sector is not None:
            nrm_sector = normalized_entity('sector', 'linkedin', language,
                                           sector)
        if nrm_sector in self._nrm_maps[None]:
            nrm_sector = self._nrm_maps[None][nrm_sector]
        inv_nrm_map = self._inv_nrm_maps[None]
        result = set(inv_nrm_map.get(entity, []))
        if nrm_sector is not None:
            inv_nrm_map = self._inv_nrm_maps.get(nrm_sector, {})
            result.update(inv_nrm_map.get(entity, []))
        if not result:
            return set([entity])
        return result

    def name(self, entity, sector=None, nrm_sector=None, language='en'):
        if sector is not None:
            nrm_sector = normalized_entity('sector', 'linkedin', language,
                                           sector)
        if nrm_sector in self._nrm_maps[None]:
            nrm_sector = self._nrm_maps[None][nrm_sector]
        if (nrm_sector, entity) in self._names:
            return self._names[nrm_sector, entity]
        elif (None, entity) in self._names:
            return self._names[None, entity]
        else:
            name = self._db.query(Entity.name) \
                           .filter(Entity.nrm_name == entity) \
                           .first()
            if name is None:
                raise LookupError('Could not find name for entity `{0:s}`.' \
                                  .format(entity))
            return name[0]

def sort_entities(entities, limit=None, min_significance=None):
    entities = list(entities)
    entities.sort(key=lambda x: -x[-2])
    newentities = []
    for row in entities:
        score = row[-2]
        error = row[-1]
        if limit is not None and len(newentities) > limit:
            break
        if min_significance is not None and score < min_significance*error:
            continue
        newentities.append(row)

    return newentities

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

def get_skill_cloud(andb, mapper, experiencec, titlec, nrm_sector, nrm_title,
                    sigma, limit):
    nrm_title = mapper(nrm_title, nrm_sector=nrm_sector)
    titles = mapper.inv(nrm_title, nrm_sector=nrm_sector)
    countcol = func.count().label('counts')
    entityq = lambda entities: \
              andb.query(Entity.nrm_name, Entity.sub_document_count) \
                  .filter(Entity.nrm_name.in_(entities))
    coincidenceq = andb.query(LIExperienceSkill.nrm_skill, countcol) \
                       .join(LIExperience) \
                       .filter(LIExperience.nrm_title.in_(titles))
    entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
    skillcloud = sort_entities(
        relevance_scores(experiencec, titlec, entityq, coincidenceq,
                         entitymap=entitymap),
        min_significance=sigma, limit=limit)
    result = []
    for skill, skillc, sectortitleskillc, score, _ in skillcloud:
        result.append({
            'skill_name' : mapper.name(skill, nrm_sector=nrm_sector),
            'total_count' : experiencec,
            'title_count' : titlec,
            'skill_count' : skillc,
            'count' : sectortitleskillc,
            'relevance_score' : score,
            'visible' : True,
        })
    return result

def get_company_cloud(andb, mapper, experiencec, titlec, nrm_sector, nrm_title,
                      sigma, limit):
    nrm_title = mapper(nrm_title, nrm_sector=nrm_sector)
    titles = mapper.inv(nrm_title, nrm_sector=nrm_sector)
    countcol = func.count().label('counts')
    titlec = andb.query(LIExperience.id) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrm_sector == nrm_sector,
                               LIExperience.nrm_title.in_(titles)) \
                     .count()
    entityq = lambda entities: \
              andb.query(Entity.nrm_name,
                         Entity.sub_document_count) \
                  .filter(Entity.nrm_name.in_(entities))
    coincidenceq = andb.query(LIExperience.nrm_company, countcol) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrm_sector == nrm_sector,
                               LIExperience.nrm_title.in_(titles))
    entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
    companycloud = sort_entities(
        relevance_scores(experiencec, titlec, entityq, coincidenceq,
                         entitymap=entitymap),
        min_significance=sigma, limit=limit)
    result = []
    for company, companyc, titlecompanyc, score, _ in companycloud:
        result.append({
            'company_name' : mapper.name(company, nrm_sector=nrm_sector),
            'total_count' : experiencec,
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
    parser.add_argument('--max-entities', type=int, default=25,
                        help='Maximum number of entities in clouds.')
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for an object to be included '
                        'in a list.')
    parser.add_argument('--max-careers', type=int,
                        help='Maximum number of careers per sector.')
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
    parser.add_argument('--sectors-from',
                        help='Name of file holding sector names.')
    parser.add_argument('--mappings',
                        help='Name of a csv file holding entity mappings. '
                        'Columns: type | lang | sector | name | mapped name')
    parser.add_argument('sector', nargs='*', default=[],
                        help='The LinkedIn sectors to scan.')
    args = parser.parse_args()

    if args.max_careers is None:
        args.max_careers = args.max_entities
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

    experiencec = andb.query(LIExperience.id) \
                      .join(LIProfile) \
                      .filter(LIProfile.nrm_sector != None) \
                      .count()
    logger.log('TOTAL EXPERIENCE COUNT: {0:d}\n'.format(experiencec))

    sectors = get_sectors(args.sector, args.sectors_from, mapper)
    nrm_sectors = {}
    joblists = {}
    sectorcounts = {}
    countcol = func.count().label('counts')
    for nrm_sector in sectors:
        lisectors = mapper.inv(nrm_sector)
        sectorc, = andb.query(func.sum(Entity.sub_document_count)) \
                       .filter(Entity.nrm_name.in_(lisectors)) \
                       .first()
        sectorc = int(sectorc)
        sectorcounts[nrm_sector] = sectorc

        # build skill cloud
        entityq = lambda entities: \
                  andb.query(LIExperienceSkill.nrm_skill, countcol) \
                      .join(LIExperience) \
                      .join(LIProfile) \
                      .filter(LIExperienceSkill.nrm_skill.in_(entities),
                              LIProfile.nrm_sector != None) \
                      .group_by(LIExperienceSkill.nrm_skill)
        coincidenceq = andb.query(LIExperienceSkill.nrm_skill, countcol) \
                           .join(LIExperience) \
                           .join(LIProfile) \
                           .filter(LIProfile.nrm_sector == nrm_sector)
        entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
        for skill, skillc, sectorskillc, score, error in sort_entities(
                relevance_scores(experiencec, sectorc, entityq, coincidenceq,
                                 entitymap=entitymap),
                limit=args.max_skills, min_significance=args.sigma):
            cddb.add_sector_skill({'sector_name' : mapper.name(nrm_sector),
                                   'skill_name' : mapper.name(
                                       skill, nrm_sector=nrm_sector),
                                   'total_count' : experiencec,
                                   'sector_count' : sectorc,
                                   'skill_count' : skillc,
                                   'count' : sectorskillc,
                                   'relevance_score' : score,
                                   'visible' : True})

        # build title cloud
        entityq = lambda entities: \
                  andb.query(LIExperience.nrm_title, countcol) \
                      .join(LIProfile) \
                      .filter(LIExperience.nrm_title.in_(entities),
                              LIProfile.nrm_sector != None) \
                      .group_by(LIExperience.nrm_title)
        coincidenceq = andb.query(LIExperience.nrm_title, countcol) \
                           .join(LIProfile) \
                           .filter(LIProfile.nrm_sector.in_(lisectors))
        entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
        joblists[nrm_sector] = sort_entities(
            relevance_scores(experiencec, sectorc, entityq, coincidenceq,
                             entitymap=entitymap),
            limit=args.max_careers, min_significance=args.sigma)

    for nrm_sector in sectors:
        lisectors = mapper.inv(nrm_sector)
        jobs = joblists[nrm_sector]
        sectorc = sectorcounts[nrm_sector]
        logger.log('{0:d} {1:s}\n'.format(sectorc,
                                          mapper.name(nrm_sector)))
        for nrm_title, titlec, sectortitlec, score, error in jobs:
            title = mapper.name(nrm_title, nrm_sector=nrm_sector)
            logger.log('    {0:>6.2f}% ({1:6.2f}% - {2:6.2f}%) {3:s}\n' \
                  .format(score*100,
                          sectortitlec/sectorc*100.0,
                          (titlec-sectortitlec)/(experiencec-sectorc)*100.0,
                          title))
            careerdict = {'title' : title,
                          'linkedin_sector' : mapper.name(nrm_sector),
                          'total_count' : experiencec,
                          'sector_count' : sectorc,
                          'title_count' : titlec,
                          'count' : sectortitlec,
                          'relevance_score' : score,
                          'visible' : True,
            }

            titles = mapper.inv(nrm_title, nrm_sector=nrm_sector)
            titlec = andb.query(LIExperience.id) \
                         .filter(LIExperience.nrm_title.in_(titles)) \
                         .count()
            
            careerdict['skill_cloud'] \
                = get_skill_cloud(andb, mapper, experiencec, titlec,
                                  nrm_sector, nrm_title,
                                  args.sigma, args.max_skills)
            careerdict['company_cloud'] \
                = get_company_cloud(andb, mapper, experiencec, titlec,
                                    nrm_sector, nrm_title,
                                    args.sigma, args.max_companies)

            q = andb.query(LIProfile.id) \
                    .join(LIExperience) \
                    .filter((LIExperience.nrm_title.in_(titles)) \
                            | (LIProfile.nrm_curr_title.in_(titles))) \
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
