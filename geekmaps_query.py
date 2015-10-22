import conf
from geekmapsdb import *
from canonicaldb import normalizedSkill, normalizedTitle, normalizedCompany
from nuts import NutsRegions
import sys
from sqlalchemy import func, distinct


def geekmapsQuery(querytype, querytext, gmdb, nutsids):
    enforced = False
    if querytype in ['skill', 'enforcedskill']:
        value = normalizedSkill(querytext)
        column = LIProfileSkill.nrmSkill
        if querytype == 'enforcedskill':
            enforced = True
    elif querytype == 'jobtitle':
        value = normalizedTitle(querytext)
        column = LIProfileSkill.nrmTitle
    elif querytype == 'company':
        value = normalizedCompany(querytext)
        column = LIProfileSkill.nrmCompany
    elif querytype == 'total':
        value = None
        column = None
    else:
        return {}, 0

    q = gmdb.query(LIProfileSkill.nuts3,
                   func.count(distinct(LIProfileSkill.profileId)))
    if querytype == 'company':
        q = q.filter(column.like('%'+value+'%'))
    elif column is not None:
        q = q.filter(column == value)
    if enforced:
        q = q.filter(LIProfileSkill.rank > 0.0)
    q = q.group_by(LIProfileSkill.nuts3)

    counts = dict((id, 0) for id in nutsids)
    total = 0
    for id, count in q:
        counts[id] = count
        total += count

    return counts, total


if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            typeflag = None
            querytext = None
        elif len(sys.argv) == 2:
            raise ValueError('Invalid argument list')
        else:
            typeflag = sys.argv[1]
            if typeflag not in ['-S', '-s', '-t', '-c']:
                raise ValueError('Invalid query type')
            querytext = sys.argv[2]
    except ValueError:
        print('usage: python3 geekmaps_query.py [(-S | -s | -t | -c) <query>]')
        exit(1)

    typedict = {
        '-S' : 'enforcedskill',
        '-s' : 'skill',
        '-t' : 'jobtitle',
        '-c' : 'company',
    }
        
    gmdb = GeekMapsDB(conf.GEEKMAPS_DB)
    nuts = NutsRegions(conf.NUTS_DATA)
    nutsids = [id for id, shape in nuts.level(3)]

    counts, total = geekmapsQuery(typedict.get(typeflag, 'total'),
                                  querytext,
                                  gmdb,
                                  nutsids)

    if not counts:
        print('Invalid query.')
        exit(1)
    
    counts = sorted(list(counts.items()))
    for id, count in counts:
        print(id, count)
    print('total', total)




    
