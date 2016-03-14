import conf
from analyticsdb import *
from analytics_get_entitycloud import relevanceScores
from careerdefinitiondb import CareerDefinitionDB
from sqlalchemy import func

andb = AnalyticsDB(conf.ANALYTICS_DB)


SECTORS = [
    'Information Technology and Services',
    'Hospital & Health Care',
    'Financial Services',
]
LIMIT = 10
MIN_SIGNIFICANCE = 2


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

totalcount = andb.query(LIProfile.id) \
                 .filter(LIProfile.nrmSector != None) \
                 .count()
nrmSectors = {}
joblists = {}
countcol = func.count().label('counts')
for sector in SECTORS:
    nrmSector, categorycount \
        = andb.query(Entity.nrmName, Entity.profileCount) \
              .filter(Entity.type == 'sector',
                      Entity.source == 'linkedin',
                      Entity.language == 'en',
                      Entity.name == sector) \
              .first()
    nrmSectors[sector] = nrmSector
    
    entityq = lambda entities: \
              andb.query(LIProfile.nrmTitle, Entity.name, countcol) \
                  .join(Entity, Entity.nrmName == LIProfile.nrmTitle) \
                  .filter(LIProfile.nrmTitle.in_(entities),
                          LIProfile.nrmSector != None) \
                  .group_by(LIProfile.nrmTitle, Entity.name)
    coincidenceq = andb.query(LIProfile.nrmTitle, countcol) \
                       .filter(LIProfile.nrmSector == nrmSector)
    joblists[sector] = sortEntities(relevanceScores(totalcount, categorycount,
                                                    entityq, coincidenceq),
                                    limit=LIMIT,
                                    minSignificance=MIN_SIGNIFICANCE)

totalcount = andb.query(LIExperience.id) \
                 .join(LIProfile) \
                 .filter(LIProfile.nrmSector != None) \
                 .count()
skillclouds = {}
for sector, jobs in joblists.items():
    print(sector)
    for nrmTitle, title, entitycount, count, score, error in jobs:
        print('    {0:>3.0f}% {1:s}'.format(score*100, title))
        categorycount = andb.query(LIExperience.id) \
                            .join(LIProfile) \
                            .filter(LIProfile.nrmSector == nrmSectors[sector],
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
                           .filter(LIProfile.nrmSector == nrmSectors[sector],
                                   LIExperience.nrmTitle == nrmTitle)
        skillclouds[sector, title] \
            = sortEntities(relevanceScores(totalcount, categorycount,
                                           entityq, coincidenceq),
                           minSignificance=MIN_SIGNIFICANCE)
        for _, skill, _, _, score, _ in skillclouds[sector, title]:
            print('        {0:>3.0f}% {1:s}'.format(score*100, skill))
        
# for sector, jobs in joblists.items():
#     print(sector)
#     for nrmTitle, title, entitycount, count, score, error in jobs:
