import conf
from geekmapsdb import *
from textnormalization import normalizedSkill, normalizedTitle, normalizedCompany
from nuts import NutsRegions
import sys
from sqlalchemy import func, distinct


_columns = {
    'enforcedskill' : LIProfileSkill.nrmSkill,
    'skill'         : LIProfileSkill.nrmSkill,
    'title'         : LIProfileSkill.nrmTitle,
    'company'       : LIProfileSkill.nrmCompany,
}

_languages = ['en', 'nl']


def geekmapsQuery(querytype, language, querytext, gmdb, nutsids):
    if language == 'all':
        languages = _languages[:]
    elif language in _languages:
        languages = [language]
    else:
        raise ValueError('Invalid language.')

    if querytype != 'total' and querytype not in _columns:
        raise ValueError('Invalid query type.')
        
    counts = dict((id, 0) for id in nutsids)
    for language in languages:
        q = gmdb.query(LIProfileSkill.nuts3,
                       func.count(distinct(LIProfileSkill.profileId)))
        if querytype == 'enforcedskill':
            q = q.filter(LIProfileSkill.rank > 0.0)
            querytype = 'skill'
        entities = None
        words = None
        if querytype != 'total':
            entities, words = gmdb.findEntities(querytype, language, querytext)
            entityids = [e[0] for e in entities]
            if not entityids:
                continue
            q = q.filter(_columns[querytype].in_(entityids))
        q = q.group_by(LIProfileSkill.nuts3)

        for id, count in q:
            counts[id] = counts.get(id, 0) + count
        
    total = sum(counts.values())
    return counts, entities, words, total
    

if __name__ == '__main__':
    querytypes = {
        '-S' : 'enforcedskill',
        '-s' : 'skill',
        '-t' : 'title',
        '-c' : 'company',
    }

    try:
        language = None
        typeflag = None
        query    = None
        if len(sys.argv) > 1:
            language = sys.argv[1]
            typeflag = sys.argv[2]
            query    = sys.argv[3]
        if typeflag is not None and typeflag not in querytypes:
            raise ValueError('Invalid query type')
    except (ValueError, IndexError):
        print('usage: python3 geekmaps_query.py [(en | nl) '
              '(-S | -s | -t | -c) <query>]')
        exit(1)


    gmdb = GeekMapsDB(conf.GEEKMAPS_DB)
    nuts = NutsRegions(conf.NUTS_DATA)
    nutsids = [id for id, shape in nuts.level(3)]

    if typeflag is None:
        querytype = 'total'
    else:
        querytype = querytypes[typeflag]


    counts, entities, words, total \
        = geekmapsQuery(querytype, language, query, gmdb, nutsids)

    counts = sorted(list(counts.items()))
    print('MATCHED WORDS:')
    for word in words:
        print('   ', word)
    print('MATCHED ENTITIES:')
    for entityId, entityName, count in entities:
        print('    {0:s} ({1:d})'.format(entityName, count))
    print('REGION COUNTS:')
    for id, count in counts:
        print('   ', id, count)
    print('TOTAL:', total)

