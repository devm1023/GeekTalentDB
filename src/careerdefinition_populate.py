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


totalcount = andb.query(LIProfile.id).count()

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
              andb.query(Entity.nrmName, Entity.name, Entity.profileCount) \
                  .filter(Entity.nrmName.in_(entities))
    coincidenceq = andb.query(LIProfile.nrmTitle, countcol) \
                       .filter(LIProfile.nrmSector == nrmSector)
    entities = list(relevanceScores(totalcount, categorycount,
                                    entityq, coincidenceq))
    entities.sort(key=lambda x: -x[-2])
    
    joblists[sector] = []
    for nrmTitle, title, entitycount, count, score, error in entities:
        if len(joblists[sector]) >= LIMIT:
            break
        if score < MIN_SIGNIFICANCE*error:
            continue
        joblists[sector].append((nrmTitle, title, score))
    
for sector, jobs in joblists.items():
    print(sector)
    for nrmTitle, title, score in jobs:
        print('    {0:>3.0f}% {1:s}'.format(score*100, title))
