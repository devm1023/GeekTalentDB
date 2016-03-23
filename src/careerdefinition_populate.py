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
        self._nrmMap = {}
        self._names = {}
        if not filename:
            return
        with open(filename, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            rowcount = 0
            for row in csvreader:
                rowcount += 1
                if len(row) != 4:
                    raise IOError('Invalid row {0:d} in CSV file.' \
                                  .format(rowcount))
                type = row[0]
                language = row[1]
                entity1 = normalizedEntity(type, 'linkedin', language, row[2])
                entity2 = normalizedEntity(type, 'linkedin', language, row[3])
                if not entity1 or not entity2:
                    raise IOError('Invalid row {0:d} in CSV file.' \
                                  .format(rowcount))
                if entity1 in self._nrmMap:
                    raise IOError('Duplicate entry in row {0:d} in CSV file.' \
                                  .format(rowcount))
                self._nrmMap[entity1] = entity2
                self._names[entity2] = row[3]

    def __call__(self, entity):
        if entity in self._nrmMap:
            return self._nrmMap[entity]
        return entity

    def name(self, entity):
        if entity in self._names:
            return self._names[entity]
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

def countEntities(q, mapper, mincount=1):
    if mapper is None:
        mapper = lambda x: x
    currentprofile = None
    lastenddate = None
    lastentity = None
    skip = False
    entitycounts = {}
    for profileId, enddate, entity in q:
        entity = mapper(entity)
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

    entitycounts = [(mapper.name(e), c) for e, c in entitycounts.items() \
                    if c >= mincount]
    entitycounts.sort(key=lambda x: -x[-1])
    return entitycounts
    
def getSkillCloud(andb, mapper, experiencec, nrmSector, nrmTitle, sigma):
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
    skillcloud = sortEntities(relevanceScores(experiencec, sectortitlec,
                                              entityq, coincidenceq,
                                              entitymap=mapper),
                              minSignificance=sigma)
    result = []
    for skill, skillc, sectortitleskillc, score, _ in skillcloud:
        result.append({
            'skillName' : mapper.name(skill),
            'totalCount' : experiencec,
            'titleCount' : sectortitlec,
            'skillCount' : skillc,
            'count' : sectortitleskillc,
            'relevanceScore' : score,
        })
    return result

def getCompanyCloud(andb, mapper, experiencec, nrmSector, nrmTitle, sigma):
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
    companycloud = sortEntities(relevanceScores(experiencec, sectortitlec,
                                                entityq, coincidenceq,
                                                entitymap=mapper),
                              minSignificance=sigma)
    result = []
    for company, companyc, sectortitlecompanyc, score, _ in companycloud:
        result.append({
            'companyName' : mapper.name(company),
            'totalCount' : experiencec,
            'titleCount' : sectortitlec,
            'companyCount' : companyc,
            'count' : sectortitlecompanyc,
            'relevanceScore' : score,
        })
    return result

def getCareerSteps(andb, mapper, nrmSector, nrmTitle, mincount=1):
    nrmSector = mapper(nrmSector)
    nrmTitle = mapper(nrmTitle)
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
            mappedTitle = mapper(experience.nrmTitle)
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
        {'previousTitle' : mapper.name(job), 'count' : count} \
        for job, count in previousTitles.items() if count >= mincount]
    nextTitles = [
        {'nextTitle' : mapper.name(job), 'count' : count} \
        for job, count in nextTitles.items() if count >= mincount]

    return previousTitles, nextTitles

def getSubjects(andb, mapper, profileIds, mincount=1):
    if not profileIds:
        return []
    q = andb.query(LIEducation.liprofileId, LIEducation.end,
                   LIEducation.nrmSubject) \
            .filter(LIEducation.liprofileId.in_(profileIds)) \
            .order_by(LIEducation.liprofileId, LIEducation.end)
    results = []
    for subject, count in countEntities(q, mapper, mincount=mincount):
        results.append({'subjectName' : subject,
                       'count' : count})
    return results

def getInstitutes(andb, mapper, profileIds, mincount=1):
    if not profileIds:
        return []
    q = andb.query(LIEducation.liprofileId, LIEducation.end,
                   LIEducation.nrmInstitute) \
            .filter(LIEducation.liprofileId.in_(profileIds)) \
            .order_by(LIEducation.liprofileId, LIEducation.end)
    results = []
    for subject, count in countEntities(q, mapper, mincount=mincount):
        results.append({'instituteName' : subject,
                        'count' : count})
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
    parser.add_argument('--limit', type=int, default=25,
                        help='Maximum number of careers per sector.')
    parser.add_argument('--sigma', type=int, default=3,
                        help='Minimal significance of relevance scores.')
    parser.add_argument('--mincount', type=int, default=1,
                        help='Minimal count for subjects and institutes to '
                        'be included.')
    parser.add_argument('--sectors-from', 
                        help='Name of file holding sector names.')
    parser.add_argument('--mappings',
                        help='Name of a csv file holding entity mappings. '
                        'Columns: type | name | mapped name')
    parser.add_argument('sector', nargs='*', default=[],
                        help='The LinkedIn sectors to scan.')
    args = parser.parse_args()


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
        for skill, skillc, sectorskillc, score, error \
            in relevanceScores(profilec, sectorc, entityq, coincidenceq,
                               entitymap=mapper):
            if score < args.sigma*error:
                continue
            cddb.addSectorSkill({'sectorName' : sector,
                                 'skillName' : mapper.name(skill),
                                 'totalCount' : profilec,
                                 'sectorCount' : sectorc,
                                 'skillCount' : skillc,
                                 'count' : sectorskillc,
                                 'relevanceScore' : score})

        # build title cloud
        entityq = lambda entities: \
                  andb.query(LIProfile.nrmTitle, countcol) \
                      .filter(LIProfile.nrmTitle.in_(entities),
                              LIProfile.nrmSector != None) \
                      .group_by(LIProfile.nrmTitle)
        coincidenceq = andb.query(LIProfile.nrmTitle, countcol) \
                           .filter(LIProfile.nrmSector == nrmSector)
        joblists[sector] \
            = sortEntities(relevanceScores(profilec, sectorc, entityq,
                                           coincidenceq, entitymap=mapper),
                           limit=args.limit, minSignificance=args.sigma)

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
            title = mapper.name(nrmTitle)
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
            }

            careerdict['skillCloud'] \
                = getSkillCloud(andb, mapper, experiencec, nrmSector, nrmTitle,
                                args.sigma)
            careerdict['companyCloud'] \
                = getCompanyCloud(andb, mapper, experiencec, nrmSector,
                                  nrmTitle, args.sigma)

            q = andb.query(LIProfile.id) \
                    .join(LIExperience) \
                    .filter(LIProfile.nrmSector == nrmSector,
                            (LIExperience.nrmTitle == nrmTitle) \
                            | (LIProfile.nrmTitle == nrmTitle)) \
                    .distinct()
            profileIds = [id for id, in q]
            careerdict['educationSubjects'] \
                = getSubjects(andb, mapper, profileIds, args.mincount)
            careerdict['educationInstitutes'] \
                = getInstitutes(andb, mapper, profileIds, args.mincount)

            previousTitles, nextTitles \
                = getCareerSteps(andb, mapper, nrmSector, nrmTitle,
                                 args.mincount)
            careerdict['previousTitles'] = previousTitles
            careerdict['nextTitles'] = nextTitles

            cddb.addCareer(careerdict, update=True)
            cddb.commit()
