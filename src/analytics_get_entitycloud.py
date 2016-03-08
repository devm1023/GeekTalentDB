import conf
from analyticsdb import *
from textnormalization import normalizedTitle, normalizedCompany, normalizedSkill
from sqlalchemy import func, or_
import sys
import csv
from logger import Logger
import argparse


def getEntities(andb, source, categorytype, entitytype, categories,
                countThreshold=1, entityThreshold=1):
    if not categories:
        return []
    
    countcol = func.count().label('counts')
    if categorytype == 'title':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                    .join(LIExperience) \
                    .filter(LIExperience.nrmTitle.in_(categories)) \
                    .group_by(LIExperienceSkill.nrmSkill) \
                    .having(countcol >= countThreshold)
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'company':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                    .join(LIExperience) \
                    .filter(LIExperience.nrmCompany.in_(categories)) \
                    .group_by(LIExperienceSkill.nrmSkill) \
                    .having(countcol >= countThreshold)                    
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'skill':
        if entitytype == 'skill':
            LIExperienceSkill2 = aliased(LIExperienceSkill)
            q = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                    .filter(LIExperienceSkill.liexperienceId \
                            == LIExperienceSkill2.liexperienceId,
                            LIExperienceSkill2.nrmSkill.in_(categories)) \
                    .group_by(LIExperienceSkill.nrmSkill) \
                    .having(countcol >= countThreshold)
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    else:
        raise ValueError('Unsupported category type `{0:s}`.' \
                         .format(categorytype))

    coincidencecounts = dict(q)
    if coincidencecounts:
        q = andb.query(Entity.nrmName, Entity.name, Entity.subDocumentCount) \
                .filter(Entity.nrmName.in_(coincidencecounts.keys()),
                        Entity.subDocumentCount >= entityThreshold)
        counts = [(nrmName, name, entitycount, coincidencecounts[nrmName]) \
                  for nrmName, name, entitycount in q]
    else:
        counts = []
        
    return counts
    
        

def getScore(totalcount, categorycount, entitycount, coincidencecount):
    return coincidencecount/categorycount \
        - (entitycount-coincidencecount)/(totalcount-categorycount)


def getSkillCloud(entitytype, categorytype, query,
                  entityThreshold=1, categoryThreshold=1, countThreshold=1,
                  exact=False):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    totalcount = andb.query(LIExperience.id).count()
    categories = andb.findEntities(categorytype, 'linkedin', 'en', query,
                                   minSubDocumentCount=categoryThreshold,
                                   exact=exact)
    categories = [(nrmName, name, sdc) for nrmName, name, pc, sdc in categories]
    categories.sort(key=lambda x: x[-1])
    categorycount = sum(c[-1] for c in categories)
    
    entities = getEntities(andb, 'linkedin', categorytype, entitytype,
                           [c[0] for c in categories],
                           countThreshold=countThreshold,
                           entityThreshold=entityThreshold)
    entities = [(nrmName, name, getScore(totalcount, categorycount,
                                         entitycount, coincidencecount)) \
                for nrmName, name, entitycount, coincidencecount in entities]
    entities.sort(key=lambda x: x[-1])

    return totalcount, categorycount, categories, entities


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('entitytype',
                        choices=['skill'],
                        help='The type of entity cloud to generate.')
    parser.add_argument('categorytype',
                        choices=['title', 'company', 'skill'],
                        help='The type of skill cloud to generate.')
    parser.add_argument('query',
                        help='The category search term')
    parser.add_argument('--category-threshold', type=int, default=1,
                        help='The minimal count for a category to be inlcuded')
    parser.add_argument('--entity-threshold', type=int, default=1,
                        help='The minimal count for an entity to be inlcuded')
    parser.add_argument('--count-threshold', type=int, default=1,
                        help='The minimal coincidence count for an entity '
                        'to be inlcuded')
    parser.add_argument('--exact', action='store_true', 
                        help='Require an exact match for the category')
    args = parser.parse_args()

    totalcount, categorycount, categories, entites \
        = getSkillCloud(args.entitytype, args.categorytype, args.query,
                        categoryThreshold=args.category_threshold,
                        entityThreshold=args.entity_threshold,
                        countThreshold=args.count_threshold,
                        exact=args.exact)

    print('MATCHING CATEGORIES')
    for nrmName, name, count in categories:
        print('{0: <60.60s} {1: >7d}'.format(name, count))
    print('\nENTITIES')
    for nrmName, name, score in entites:
        print('{0: <60.60s} {1: >6.1f}%'.format(name, score*100))
    print('\nCATEGORY COUNT: {0: >7d}'.format(categorycount))
    print('TOTAL COUNT:    {0: >7d}'.format(totalcount))

