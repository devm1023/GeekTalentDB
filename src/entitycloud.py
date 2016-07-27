__all__ = ['relevance_score', 'relevance_scores', 'entity_cloud']

from math import sqrt
from numpy import inf


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
