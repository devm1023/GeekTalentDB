import conf
from analyticsdb import *
from analytics_get_entitycloud import relevanceScores
from careerdefinitiondb import CareerDefinitionDB
from textnormalization import normalizedEntity
from sqlalchemy import func
import csv
import argparse

class EntityMapper:
    def __init__(self, db, filename):
        self._db = db
        self._nrmMaps = {None : {}}
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
                if not sector:
                    sector = None
                entity1 = row[3].strip()
                entity2 = row[4].strip()
                nrmSector = normalizedEntity('sector', 'linkedin', language,
                                             sector)
                if nrmSector not in self._nrmMaps:
                    self._nrmMaps[nrmSector] = {}
                nrmMap = self._nrmMaps[nrmSector]
                nrmEntity1 = normalizedEntity(type, 'linkedin', language,
                                              entity1)
                nrmEntity2 = normalizedEntity(type, 'linkedin', language,
                                              entity2)
                if not nrmEntity1 or not nrmEntity2:
                    raise IOError('Invalid row in CSV file:\n{0:s}' \
                                  .format(repr(row)))
                if nrmEntity1 in nrmMap:
                    raise IOError('Duplicate entry in row in CSV file:\n{0:s}' \
                                  .format(repr(row)))
                nrmMap[nrmEntity1] = nrmEntity2
                self._names[nrmSector, nrmEntity2] = entity2

    def __call__(self, entity, sector=None, nrmSector=None, language='en'):
        if sector is not None:
            nrmSector = normalizedEntity('sector', 'linkedin', language,
                                         sector)
        if nrmSector is not None and nrmSector in self._nrmMaps:
            nrmMap = self._nrmMaps[nrmSector]
            if entity in nrmMap:
                return nrmMap[entity]
        nrmMap = self._nrmMaps[None]
        if entity in nrmMap:
            return nrmMap[entity]
        return entity

    def name(self, entity, sector=None, nrmSector=None, language='en'):
        if sector is not None:
            nrmSector = normalizedEntity('sector', 'linkedin', language,
                                         sector)
        nrmSector = self(nrmSector, language=language)
        if (nrmSector, entity) in self._names:
            return self._names[nrmSector, entity]
        elif (None, entity) in self._names:
            return self._names[None, entity]
        else:
            name = self._db.query(Entity.name) \
                           .filter(Entity.nrmName == entity) \
                           .first()
            if name is None:
                raise LookupError('Could not find name for entity `{0:s}`.' \
                                  .format(entity))
            return name[0]

def sortEntities(entities, limit=None, minSignificance=None):
    entities = list(entities)
    entities.sort(key=lambda x: -x[-2])
    newentities = []
    for row in entities:
        score = row[-2]
        error = row[-1]
        if limit is not None and len(newentities) > limit:
            break
        if minSignificance is not None and score < minSignificance*error:
            continue
        newentities.append(row)

    return newentities

def countEntities(q, mapper, nrmSector=None, mincount=1):
    if mapper is None:
        mapper = lambda x: x
    currentprofile = None
    lastenddate = None
    lastentity = None
    skip = False
    entitycounts = {}
    for profileId, enddate, entity in q:
        entity = mapper(entity, nrmSector=nrmSector)
        if currentprofile is not None and profileId != currentprofile:
            if lastentity is not None:
                entitycounts[lastentity] \
                    = entitycounts.get(lastentity, 0) + 1
            lastentity = None
            lastenddate = None
            skip = False
        currentprofile = profileId
        if not entity or skip:
            continue
        if lastentity is not None and lastenddate is None:
            lastentity = None
            skip = True
            continue
        lastenddate = enddate
        lastentity = entity

    entitycounts = [(mapper.name(e, nrmSector=nrmSector), c) \
                    for e, c in entitycounts.items() \
                    if c >= mincount]
    entitycounts.sort(key=lambda x: -x[-1])
    return entitycounts
    
def getSkillCloud(andb, mapper, experiencec, nrmSector, nrmTitle,
                  sigma, limit):
    countcol = func.count().label('counts')
    sectortitlec = andb.query(LIExperience.id) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle) \
                     .count()
    entityq = lambda entities: \
              andb.query(Entity.nrmName,
                         Entity.subDocumentCount) \
                  .filter(Entity.nrmName.in_(entities))
    coincidenceq = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                       .join(LIExperience) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle)
    entitymap = lambda s: mapper(s, nrmSector=nrmSector)
    skillcloud = sortEntities(relevanceScores(experiencec, sectortitlec,
                                              entityq, coincidenceq,
                                              entitymap=entitymap),
                              minSignificance=sigma, limit=limit)
    result = []
    for skill, skillc, sectortitleskillc, score, _ in skillcloud:
        result.append({
            'skillName' : mapper.name(skill, nrmSector=nrmSector),
            'totalCount' : experiencec,
            'titleCount' : sectortitlec,
            'skillCount' : skillc,
            'count' : sectortitleskillc,
            'relevanceScore' : score,
            'visible' : True,
        })
    return result

def getCompanyCloud(andb, mapper, experiencec, nrmSector, nrmTitle,
                    sigma, limit):
    countcol = func.count().label('counts')
    sectortitlec = andb.query(LIExperience.id) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle) \
                     .count()
    entityq = lambda entities: \
              andb.query(Entity.nrmName,
                         Entity.subDocumentCount) \
                  .filter(Entity.nrmName.in_(entities))
    coincidenceq = andb.query(LIExperience.nrmCompany, countcol) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle)
    entitymap = lambda s: mapper(s, nrmSector=nrmSector)
    companycloud = sortEntities(relevanceScores(experiencec, sectortitlec,
                                                entityq, coincidenceq,
                                                entitymap=entitymap),
                                minSignificance=sigma, limit=limit)
    result = []
    for company, companyc, sectortitlecompanyc, score, _ in companycloud:
        result.append({
            'companyName' : mapper.name(company, nrmSector=nrmSector),
            'totalCount' : experiencec,
            'titleCount' : sectortitlec,
            'companyCount' : companyc,
            'count' : sectortitlecompanyc,
            'relevanceScore' : score,
            'visible' : True,
        })
    return result

def getCareerSteps(andb, mapper, nrmSector, nrmTitle, mincount=1, limit=None):
    nrmTitle = mapper(nrmTitle, nrmSector=nrmSector)
    previousTitles = {}
    nextTitles = {}
    q = andb.query(LIProfile) \
            .join(LIExperience) \
            .filter(LIProfile.nrmSector == nrmSector,
                    LIExperience.nrmTitle == nrmTitle) \
            .distinct()
    for liprofile in q:
        titles = []
        for experience in liprofile.experiences:
            if not experience.nrmTitle or not experience.start:
                continue
            mappedTitle = mapper(experience.nrmTitle, nrmSector=nrmSector)
            if not titles or titles[-1] != mappedTitle:
                titles.append(mappedTitle)
        for i, title in enumerate(titles):
            if title != nrmTitle:
                continue
            if i > 0:
                prevTitle = titles[i-1]
                previousTitles[prevTitle] = previousTitles.get(prevTitle, 0) + 1
            if i < len(titles)-1:
                nextTitle = titles[i+1]
                nextTitles[nextTitle] = nextTitles.get(nextTitle, 0) + 1
    previousTitles = [
        {'previousTitle' : mapper.name(job, nrmSector=nrmSector),
         'count' : count,
         'visible' : True} \
        for job, count in previousTitles.items() if count >= mincount]
    previousTitles.sort(key=lambda t: -t['count'])
    if limit is not None and len(previousTitles) > limit:
        previousTitles = previousTitles[:limit]
    nextTitles = [
        {'nextTitle' : mapper.name(job, nrmSector=nrmSector),
         'count' : count,
         'visible' : True} \
        for job, count in nextTitles.items() if count >= mincount]
    nextTitles.sort(key=lambda t: -t['count'])
    if limit is not None and len(nextTitles) > limit:
        nextTitles = nextTitles[:limit]

    return previousTitles, nextTitles

def getSubjects(andb, mapper, nrmSector, profileIds, mincount=1, limit=None):
    if not profileIds:
        return []
    q = andb.query(LIEducation.liprofileId, LIEducation.end,
                   LIEducation.nrmSubject) \
            .filter(LIEducation.liprofileId.in_(profileIds)) \
            .order_by(LIEducation.liprofileId, LIEducation.end)
    results = []
    for subject, count in countEntities(q, mapper, nrmSector=nrmSector,
                                        mincount=mincount):
        results.append({'subjectName' : subject.strip(),
                        'count' : count,
                        'visible' : True})
    results.sort(key=lambda r: -r['count'])
    if limit is not None and len(results) > limit:
        results = results[:limit]
    return results

def getInstitutes(andb, mapper, nrmSector, profileIds, mincount=1, limit=None):
    if not profileIds:
        return []
    q = andb.query(LIEducation.liprofileId, LIEducation.end,
                   LIEducation.nrmInstitute) \
            .filter(LIEducation.liprofileId.in_(profileIds)) \
            .order_by(LIEducation.liprofileId, LIEducation.end)
    results = []
    for institute, count in countEntities(q, mapper, nrmSector=nrmSector,
                                        mincount=mincount):
        results.append({'instituteName' : institute.strip(),
                        'count' : count,
                        'visible' : True})
    results.sort(key=lambda r: -r['count'])
    if limit is not None and len(results) > limit:
        results = results[:limit]
    return results

def getSectors(sectors, filename):
    sectors = list(sectors)
    if filename:
        with open(filename, 'r') as sectorfile:
            for line in sectorfile:
                row = line.split('|')
                if not row:
                    continue
                sector = row[0].strip()
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
        

    andb = AnalyticsDB(conf.ANALYTICS_DB)
    cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)
    mapper = EntityMapper(andb, args.mappings)

    profilec = andb.query(LIProfile.id) \
                   .filter(LIProfile.nrmSector != None) \
                   .count()
    print('TOTAL PROFILE COUNT: {0:d}'.format(profilec))

    sectors = getSectors(args.sector, args.sectors_from)
    nrmSectors = {}
    joblists = {}
    sectorcounts = {}
    countcol = func.count().label('counts')
    for sector in sectors:
        row = andb.query(Entity.nrmName, Entity.profileCount) \
                  .filter(Entity.type == 'sector',
                          Entity.source == 'linkedin',
                          Entity.language == 'en',
                          Entity.name == sector) \
                  .first()
        if not row:
            continue
        nrmSector, sectorc = row
        nrmSectors[sector] = nrmSector
        sectorcounts[sector] = sectorc

        # build skill cloud
        entityq = lambda entities: \
                  andb.query(Entity.nrmName, Entity.profileCount) \
                      .filter(Entity.nrmName.in_(entities))
        coincidenceq = andb.query(LIProfileSkill.nrmName, countcol) \
                           .join(LIProfile) \
                           .filter(LIProfile.nrmSector == nrmSector)
        entitymap = lambda s: mapper(s, nrmSector=nrmSector)
        for skill, skillc, sectorskillc, score, error \
            in sortEntities(relevanceScores(profilec, sectorc, entityq,
                                            coincidenceq, entitymap=entitymap),
                            limit=args.max_skills, minSignificance=args.sigma):
            cddb.addSectorSkill({'sectorName' : sector,
                                 'skillName' : mapper.name(skill,
                                                           nrmSector=nrmSector),
                                 'totalCount' : profilec,
                                 'sectorCount' : sectorc,
                                 'skillCount' : skillc,
                                 'count' : sectorskillc,
                                 'relevanceScore' : score,
                                 'visible' : True})

        # build title cloud
        entityq = lambda entities: \
                  andb.query(LIProfile.nrmTitle, countcol) \
                      .filter(LIProfile.nrmTitle.in_(entities),
                              LIProfile.nrmSector != None) \
                      .group_by(LIProfile.nrmTitle)
        coincidenceq = andb.query(LIProfile.nrmTitle, countcol) \
                           .filter(LIProfile.nrmSector == nrmSector)
        entitymap = lambda s: mapper(s, nrmSector=nrmSector)
        joblists[sector] \
            = sortEntities(relevanceScores(profilec, sectorc, entityq,
                                           coincidenceq, entitymap=entitymap),
                           limit=args.max_careers, minSignificance=args.sigma)

    experiencec = andb.query(LIExperience.id) \
                      .join(LIProfile) \
                      .filter(LIProfile.nrmSector != None) \
                      .count()
    print('TOTAL EXPERIENCE COUNT: {0:d}'.format(experiencec))
    for sector in sectors:
        jobs = joblists[sector]
        print('{0:d} {1:s}'.format(sectorcounts[sector], sector))
        sectorc = sectorcounts[sector]
        nrmSector = nrmSectors[sector]
        for nrmTitle, titlec, sectortitlec, score, error in jobs:
            title = mapper.name(nrmTitle, nrmSector=nrmSector)
            print('    {0:>5.1f}% ({1:5.1f}% - {2:5.1f}%) {3:s}' \
                  .format(score*100,
                          sectortitlec/sectorc*100.0,
                          (titlec-sectortitlec)/(profilec-sectorc)*100.0,
                          title))
            careerdict = {'title' : title,
                          'linkedinSector' : sector,
                          'totalCount' : profilec,
                          'sectorCount' : sectorc,
                          'titleCount' : titlec,
                          'count' : sectortitlec,
                          'relevanceScore' : score,
                          'visible' : True,
            }

            careerdict['skillCloud'] \
                = getSkillCloud(andb, mapper, experiencec, nrmSector, nrmTitle,
                                args.sigma, args.max_skills)
            careerdict['companyCloud'] \
                = getCompanyCloud(andb, mapper, experiencec, nrmSector,
                                  nrmTitle, args.sigma, args.max_companies)

            q = andb.query(LIProfile.id) \
                    .join(LIExperience) \
                    .filter(LIProfile.nrmSector == nrmSector,
                            (LIExperience.nrmTitle == nrmTitle) \
                            | (LIProfile.nrmTitle == nrmTitle)) \
                    .distinct()
            profileIds = [id for id, in q]
            careerdict['educationSubjects'] \
                = getSubjects(andb, mapper, nrmSector, profileIds,
                              args.min_subject_count, args.max_subjects)
            careerdict['educationInstitutes'] \
                = getInstitutes(andb, mapper, nrmSector, profileIds,
                                args.min_institute_count, args.max_institutes)

            previousTitles, nextTitles \
                = getCareerSteps(andb, mapper, nrmSector, nrmTitle,
                                 args.min_careerstep_count,
                                 args.max_careersteps)
            careerdict['previousTitles'] = previousTitles
            careerdict['nextTitles'] = nextTitles

            cddb.addCareer(careerdict, update=True)
            cddb.commit()
