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

parser = argparse.ArgumentParser()
parser.add_argument('--limit', type=int, default=25,
                    help='Maximum number of careers per sector.')
parser.add_argument('--sigma', type=int, default=3,
                    help='Minimal significance of relevance scores.')
parser.add_argument('sector', nargs='*', default=SECTORS,
                    help='The LinkedIn sectors to scan.')
args = parser.parse_args()


andb = AnalyticsDB(conf.ANALYTICS_DB)
cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)


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

profilec = andb.query(LIProfile.id) \
               .filter(LIProfile.nrmSector != None) \
               .count()
print('TOTAL PROFILE COUNT: {0:d}'.format(profilec))
nrmSectors = {}
joblists = {}
sectorcounts = {}
countcol = func.count().label('counts')
for sector in args.sector:
    nrmSector, sectorc \
        = andb.query(Entity.nrmName, Entity.profileCount) \
              .filter(Entity.type == 'sector',
                      Entity.source == 'linkedin',
                      Entity.language == 'en',
                      Entity.name == sector) \
              .first()
    nrmSectors[sector] = nrmSector
    sectorcounts[sector] = sectorc
    
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
skillclouds = {}
for sector, jobs in joblists.items():
    print('{0:d} {1:s}'.format(sectorcounts[sector], sector))
    sectorc = sectorcounts[sector]
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
                      'skillCloud' : []
        }
        
        sectortitlec = andb.query(LIExperience.id) \
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
        skillcloud \
            = sortEntities(relevanceScores(experiencec, sectortitlec,
                                           entityq, coincidenceq),
                           minSignificance=args.sigma)
        for _, skill, skillc, sectortitleskillc, score, _ in skillcloud:
            careerdict['skillCloud'].append({
                'skillName' : skill,
                'totalCount' : experiencec,
                'titleCount' : sectortitlec,
                'skillCount' : skillc,
                'count' : sectortitleskillc,
                'relevanceScore' : score,
            })

        cddb.addCareer(careerdict, update=True)
        cddb.commit()
