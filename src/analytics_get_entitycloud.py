import conf
from analyticsdb import *
from textnormalization import normalized_title, normalized_company, \
    normalized_skill
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased
import sys
import csv
from math import sqrt
from numpy import inf
from logger import Logger
import argparse


def relevance_score(totalcount, categorycount, entitycount, coincidencecount):
    c1 = coincidencecount
    n1 = categorycount
    c2 = entitycount - coincidencecount
    n2 = totalcount - categorycount
    if c2 < 0:
        raise ValueError('entitycount < coincidencecount')
    if n2 < 0:
        raise ValueError('totalcount < categorycount')
    if n1 == 0 or n2 == 0:
        return 0.0, inf
    f1 = c1/n1
    f2 = c2/n2
    score = f1 - f2
    var = f1*(1-f1)/n1 + f2*(1-f2)/n2
    return score, sqrt(var)

def relevance_scores(totalcount, categorycount, entitiesq, coincidenceq,
                     mincount=1, entitymap=None):
    """Extract and score relevant entities for a given category.

    Args:
      totalcount (int): The total number of documents.
      categorycount (int): The number of documents matching the category.
      entitiesq (callable): Function that receives a list (or iterable) of
        entity IDs and returns a query at least two columns. The first column
        must hold the supplied entity IDs. The last column must hold the
        count of all documents containing that entity (across all categories).
      coincidenceq (query object): A query with two columns. The first column
        must hold the IDs of entities that appear in documents matching the
        category of interest. The second column must hold the number of
        documents matching the category and containing the respective entity.
        The entitiy IDs gathered this way will then be supplied to `entitiesq`.
      mincount (int, optional): The minimum coincidence count required for
        entities to appear in the result.

    Yields:
      The rows returned by (the return value of) entitiesq with the following
      extra columns appended:

        coincidencecount
          The number of documents matching the category and containin the entity
          in the first column (i.e. the value in the second column of
          `coincidenceq`).
        score
          The relevance score for this entity (a number between -1 and 1).
        error
          The standard deviation of `score`.

    """
    cols = tuple(coincidenceq.statement.inner_columns)
    if len(cols) != 2:
        raise ValueError('entities query must have two columns')
    entitycol, countcol = cols
    q = coincidenceq.group_by(entitycol).having(countcol >= mincount)
    counts = dict(q)

    if counts:
        if entitymap is None:
            for row in entitiesq(counts.keys()):
                entity = row[0]
                if entity not in counts:
                    continue
                entitycount = row[-1]
                count = counts[entity]
                score, err = relevance_score(totalcount, categorycount,
                                             entitycount, count)
                yield row[:-1] + (entitycount, count, score, err)
        else:
            entitycounts = {}
            mappedcounts = {}
            for row in entitiesq(counts.keys()):
                entity = entitymap(row[0])
                if not entity:
                    continue
                entitycount = row[-1]
                count = counts[row[0]]
                entitycounts[entity] = entitycounts.get(entity, 0) + entitycount
                mappedcounts[entity] = mappedcounts.get(entity, 0) + count
            for entity, entitycount in entitycounts.items():
                count = mappedcounts[entity]
                score, err = relevance_score(totalcount, categorycount,
                                             entitycount, count)
                yield (entity, entitycount, count, score, err)

def entity_cloud(totalcount, categorycount, entitiesq, coincidenceq,
                 mincount=1, limit=None, sigma=3, entitymap=None):
    entities = list(relevance_scores(
        totalcount, categorycount, entitiesq, coincidenceq,
        mincount=mincount, entitymap=entitymap))
    entities.sort(key=lambda x: -x[-2])
    newentities = []
    for row in entities:
        score = row[-2]
        error = row[-1]
        if limit is not None and len(newentities) >= limit:
            break
        if sigma is not None and score < sigma*error:
            continue
        newentities.append(row)

    return newentities
                
def get_skill_cloud(entitytype, categorytype, query,
                    entity_threshold=1, category_threshold=1, count_threshold=1,
                    exact=False):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    totalcount = andb.query(LIExperience.id).count()
    categories = andb.find_entities(categorytype, 'linkedin', 'en', query,
                                    min_sub_document_count=category_threshold,
                                    exact=exact)
    categories = [(nrm_name, name, sdc) for nrm_name, name, pc, sdc \
                  in categories]
    categories.sort(key=lambda x: x[-1])
    categorycount = sum(c[-1] for c in categories)
    categorynames = [nrm_name for nrm_name, name, count in categories]

    entitiesq = lambda keys: \
                andb.query(Entity.nrm_name,
                           Entity.name,
                           Entity.sub_document_count) \
                    .filter(Entity.nrm_name.in_(keys),
                            Entity.sub_document_count >= entity_threshold)

    countcol = func.count().label('counts')
    if categorytype == 'title':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrm_skill, countcol) \
                    .join(LIExperience) \
                    .filter(LIExperience.nrm_title.in_(categorynames))
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'company':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrm_skill, countcol) \
                    .join(LIExperience) \
                    .filter(LIExperience.nrm_company.in_(categorynames))
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'skill':
        if entitytype == 'skill':
            LIExperienceSkill2 = aliased(LIExperienceSkill)
            q = andb.query(LIExperienceSkill.nrm_skill, countcol) \
                    .filter(LIExperienceSkill.liexperience_id \
                            == LIExperienceSkill2.liexperience_id,
                            LIExperienceSkill2.nrm_skill.in_(categorynames))
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    elif categorytype == 'sector':
        if entitytype == 'skill':
            q = andb.query(LIExperienceSkill.nrm_skill, countcol) \
                    .join(LIExperience) \
                    .join(LIProfile) \
                    .filter(LIProfile.nrm_sector.in_(categorynames))
        else:
            raise ValueError('Unsupported entity type `{0:s}`.' \
                             .format(entitytype))
    else:
        raise ValueError('Unsupported category type `{0:s}`.' \
                         .format(categorytype))

    entities = list(relevance_scores(totalcount, categorycount, entitiesq, q,
                                    mincount=count_threshold))
    entities.sort(key=lambda x: x[-2])

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
        = get_skill_cloud(args.entitytype, args.categorytype, args.query,
                          category_threshold=args.category_threshold,
                          entity_threshold=args.entity_threshold,
                          count_threshold=args.count_threshold,
                          exact=args.exact)

    print('MATCHING CATEGORIES')
    for nrm_name, name, count in categories:
        print('{0: <60.60s} {1: >7d}'.format(name, count))
    print('\nEntities')
    for nrm_name, name, entitycount, count, score, err in entites:
        if err >= 5e-4:
            print('{0: <60.60s} {1: >6.1f}% +/- {2: >5.1f}%' \
                  .format(name, score*100, err*100))
        else:
            print('{0: <60.60s} {1: >6.1f}%'.format(name, score*100))
    print('\nCategory count: {0: >7d}'.format(categorycount))
    print('TOTAL COUNT:    {0: >7d}'.format(totalcount))

