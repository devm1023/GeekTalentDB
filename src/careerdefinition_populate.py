import conf
from analyticsdb import *
from analytics_get_entitycloud import relevanceScores
from careerdefinitiondb import CareerDefinitionDB
from sqlalchemy import func
import argparse

SECTORS = [
    'Information Technology and Services',
    'Hospital & Health Care',
    'Financial Services',
]

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

def countEntities(q, mincount=1):
    currentprofile = None
    lastenddate = None
    lastentity = None
    skip = False
    entitycounts = {}
    for profileId, enddate, nrmName, entity in q:
        if currentprofile is not None and profileId != currentprofile:
            if lastentity is not None:
                entitycounts[lastentity] \
                    = entitycounts.get(lastentity, 0) + 1
            lastentity = None
            lastenddate = None
            skip = False
        currentprofile = profileId
        if not nrmName or skip:
            continue
        if lastentity is not None and lastenddate is None:
            lastentity = None
            skip = True
            continue
        lastenddate = enddate
        lastentity = entity

    entitycounts = [(e, c) for e, c in entitycounts.items() if c >= mincount]
    entitycounts.sort(key=lambda x: -x[-1])
    return entitycounts
    
def getSkillCloud(andb, experiencec, nrmSector, nrmTitle, sigma):
    countcol = func.count().label('counts')
    sectortitlec = andb.query(LIExperience.id) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle) \
                     .count()
    entityq = lambda entities: \
              andb.query(Entity.nrmName,
                         Entity.name,
                         Entity.subDocumentCount) \
                  .filter(Entity.nrmName.in_(entities))
    coincidenceq = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                       .join(LIExperience) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle)
    skillcloud = sortEntities(relevanceScores(experiencec, sectortitlec,
                                              entityq, coincidenceq),
                              minSignificance=sigma)
    result = []
    for _, skill, skillc, sectortitleskillc, score, _ in skillcloud:
        result.append({
            'skillName' : skill,
            'totalCount' : experiencec,
            'titleCount' : sectortitlec,
            'skillCount' : skillc,
            'count' : sectortitleskillc,
            'relevanceScore' : score,
        })
    return result

def getCompanyCloud(andb, experiencec, nrmSector, nrmTitle, sigma):
    countcol = func.count().label('counts')
    sectortitlec = andb.query(LIExperience.id) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle) \
                     .count()
    entityq = lambda entities: \
              andb.query(Entity.nrmName,
                         Entity.name,
                         Entity.subDocumentCount) \
                  .filter(Entity.nrmName.in_(entities))
    coincidenceq = andb.query(LIExperience.nrmCompany, countcol) \
                       .join(LIProfile) \
                       .filter(LIProfile.nrmSector == nrmSector,
                               LIExperience.nrmTitle == nrmTitle)
    companycloud = sortEntities(relevanceScores(experiencec, sectortitlec,
                                                entityq, coincidenceq),
                              minSignificance=sigma)
    result = []
    for _, company, companyc, sectortitlecompanyc, score, _ in companycloud:
        result.append({
            'companyName' : company,
            'totalCount' : experiencec,
            'titleCount' : sectortitlec,
            'companyCount' : companyc,
            'count' : sectortitlecompanyc,
            'relevanceScore' : score,
        })
    return result

def getCareerSteps(andb, nrmSector, nrmTitle, mincount=1):
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
            if not titles or titles[-1] != experience.nrmTitle:
                titles.append(experience.nrmTitle)
        for i, title in enumerate(titles):
            if title != nrmTitle:
                continue
            if i > 0:
                prevTitle = titles[i-1]
                previousTitles[prevTitle] = previousTitles.get(prevTitle, 0) + 1
            if i < len(titles)-1:
                nextTitle = titles[i+1]
                nextTitles[nextTitle] = nextTitles.get(nextTitle, 0) + 1
    previousTitles = [(job, count) for job, count in previousTitles.items() \
                      if count >= mincount]
    nextTitles = [(job, count) for job, count in nextTitles.items() \
                  if count >= mincount]
    nrmTitles = set(job for l in [previousTitles, nextTitles] for job, _ in l)
    q = andb.query(Entity.nrmName, Entity.name)
    if nrmTitles:
        q = q.filter(Entity.nrmName.in_(nrmTitles))
    titles = dict(q)
    previousTitles = [{'previousTitle' : titles[t], 'count' : c} \
                      for t, c in previousTitles]
    nextTitles = [{'nextTitle' : titles[t], 'count' : c} for t, c in nextTitles]

    return previousTitles, nextTitles

def getSubjects(andb, profileIds, mincount=1):
    if not profileIds:
        return []
    q = andb.query(LIEducation.liprofileId, LIEducation.end,
                   LIEducation.nrmSubject, Entity.name) \
            .join(Entity, Entity.nrmName == LIEducation.nrmSubject) \
            .filter(LIEducation.liprofileId.in_(profileIds)) \
            .order_by(LIEducation.liprofileId, LIEducation.end)
    results = []
    for subject, count in countEntities(q, mincount=mincount):
        results.append({'subjectName' : subject,
                       'count' : count})
    return results

def getInstitutes(andb, profileIds, mincount=1):
    if not profileIds:
        return []
    q = andb.query(LIEducation.liprofileId, LIEducation.end,
                   LIEducation.nrmInstitute, Entity.name) \
            .join(Entity, Entity.nrmName == LIEducation.nrmInstitute) \
            .filter(LIEducation.liprofileId.in_(profileIds)) \
            .order_by(LIEducation.liprofileId, LIEducation.end)
    results = []
    for subject, count in countEntities(q, mincount=mincount):
        results.append({'instituteName' : subject,
                        'count' : count})
    return results

def getSectors(sectors, filename):
    sectors = set(sectors)
    if filename:
        with open(filename, 'r') as sectorfile:
            for line in sectorfile:
                row = line.split('|')
                if not row:
                    continue
                sector = row[0].strip()
                if not sector:
                    continue
                sectors.add(sector)
    return list(sectors)


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
    parser.add_argument('sector', nargs='*', default=SECTORS,
                        help='The LinkedIn sectors to scan.')
    args = parser.parse_args()


    andb = AnalyticsDB(conf.ANALYTICS_DB)
    cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)

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
        nrmSector, sectorc \
            = andb.query(Entity.nrmName, Entity.profileCount) \
                  .filter(Entity.type == 'sector',
                          Entity.source == 'linkedin',
                          Entity.language == 'en',
                          Entity.name == sector) \
                  .first()
        nrmSectors[sector] = nrmSector
        sectorcounts[sector] = sectorc

        # build skill cloud
        entityq = lambda entities: \
                  andb.query(Entity.nrmName, Entity.name, Entity.profileCount) \
                      .filter(Entity.nrmName.in_(entities))
        coincidenceq = andb.query(LIProfileSkill.nrmName, countcol) \
                           .join(LIProfile) \
                           .filter(LIProfile.nrmSector == nrmSector)
        for nrmSkill, skill, skillc, sectorskillc, score, error \
            in relevanceScores(profilec, sectorc, entityq, coincidenceq):
            if score < args.sigma*error:
                continue
            cddb.addSectorSkill({'sectorName' : sector,
                                 'skillName' : skill,
                                 'totalCount' : profilec,
                                 'sectorCount' : sectorc,
                                 'skillCount' : skillc,
                                 'count' : sectorskillc,
                                 'relevanceScore' : score})

        # build title cloud
        entityq = lambda entities: \
                  andb.query(LIProfile.nrmTitle, Entity.name, countcol) \
                      .join(Entity, Entity.nrmName == LIProfile.nrmTitle) \
                      .filter(LIProfile.nrmTitle.in_(entities),
                              LIProfile.nrmSector != None) \
                      .group_by(LIProfile.nrmTitle, Entity.name)
        coincidenceq = andb.query(LIProfile.nrmTitle, countcol) \
                           .filter(LIProfile.nrmSector == nrmSector)
        joblists[sector] = sortEntities(relevanceScores(profilec, sectorc,
                                                        entityq, coincidenceq),
                                        limit=args.limit,
                                        minSignificance=args.sigma)

    experiencec = andb.query(LIExperience.id) \
                      .join(LIProfile) \
                      .filter(LIProfile.nrmSector != None) \
                      .count()
    print('TOTAL EXPERIENCE COUNT: {0:d}'.format(experiencec))
    for sector, jobs in joblists.items():
        print('{0:d} {1:s}'.format(sectorcounts[sector], sector))
        sectorc = sectorcounts[sector]
        nrmSector = nrmSectors[sector]
        for nrmTitle, title, titlec, sectortitlec, score, error in jobs:
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

            careerdict['skillCloud'] = getSkillCloud(andb, experiencec,
                                                     nrmSector, nrmTitle,
                                                     args.sigma)
            careerdict['companyCloud'] = getCompanyCloud(andb, experiencec,
                                                         nrmSector, nrmTitle,
                                                         args.sigma)

            q = andb.query(LIProfile.id) \
                    .join(LIExperience) \
                    .filter(LIProfile.nrmSector == nrmSector,
                            (LIExperience.nrmTitle == nrmTitle) \
                            | (LIProfile.nrmTitle == nrmTitle)) \
                    .distinct()
            profileIds = [id for id, in q]
            careerdict['educationSubjects'] \
                = getSubjects(andb, profileIds, args.mincount)
            careerdict['educationInstitutes'] \
                = getInstitutes(andb, profileIds, args.mincount)

            previousTitles, nextTitles \
                = getCareerSteps(andb, nrmSector, nrmTitle, args.mincount)
            careerdict['previousTitles'] = previousTitles
            careerdict['nextTitles'] = nextTitles

            cddb.addCareer(careerdict, update=True)
            cddb.commit()
