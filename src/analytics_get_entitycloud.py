import conf
from analyticsdb import *
from textnormalization import normalizedTitle, normalizedCompany, \
    normalizedSkill
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased
import sys
import csv
from logger import Logger
import argparse


def _score(totalcount, categorycount, entitycount, coincidencecount):
    return coincidencecount/categorycount \
        - (entitycount-coincidencecount)/(totalcount-categorycount)

def relevanceScores(totalcount, categorycount, entitiesq, coincidenceq,
                    mincount=1):
    cols = tuple(coincidenceq.statement.inner_columns)
    if len(cols) != 2:
        raise ValueError('entities query must have two columns')
    entitycol, countcol = cols
    q = coincidenceq.group_by(entitycol).having(countcol >= mincount)
    counts = dict(q)

    for row in entitiesq(counts.keys()):
        entity = row[0]
        entitycount = row[-1]
        count = counts[entity]
        yield row[:-1] \
            + (entitycount, count,
               _score(totalcount, categorycount, entitycount, count))

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
    categorynames = [nrmName for nrmName, name, count in categories]
    
    entitiesq = lambda keys: \
                andb.query(Entity.nrmName,
                           Entity.name,
                           Entity.subDocumentCount) \
                    .filter(Entity.nrmName.in_(keys),
                            Entity.subDocumentCount >= entityThreshold)
    
    countcol = func.count().label('counts')
    if categorytype == 'title':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                    .join(LIExperience) \
                    .filter(LIExperience.nrmTitle.in_(categorynames))
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'company':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                    .join(LIExperience) \
                    .filter(LIExperience.nrmCompany.in_(categorynames))       
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'skill':
        if entitytype == 'skill':
            LIExperienceSkill2 = aliased(LIExperienceSkill)
            q = andb.query(LIExperienceSkill.nrmSkill, countcol) \
                    .filter(LIExperienceSkill.liexperienceId \
                            == LIExperienceSkill2.liexperienceId,
                            LIExperienceSkill2.nrmSkill.in_(categorynames))
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    else:
        raise ValueError('Unsupported category type `{0:s}`.' \
                         .format(categorytype))

    entities = list(relevanceScores(totalcount, categorycount, entitiesq, q,
                                    mincount=countThreshold))
    entities.sort(key=lambda x: x[-1])

    return totalcount, categorycount, categories, entities


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('entitytype',
                        choices=['skill'],
                        help='The type of entity cloud to generate.')
    parser.add_argument('categorytype',
                        choices=['title', 'company', 'skill', 'sector'],
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
    for nrmName, name, entitycount, count, score in entites:
        print('{0: <60.60s} {1: >6.1f}%'.format(name, score*100))
    print('\nCATEGORY COUNT: {0: >7d}'.format(categorycount))
    print('TOTAL COUNT:    {0: >7d}'.format(totalcount))

