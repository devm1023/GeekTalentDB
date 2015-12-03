import conf
from geekmapsdb import *
from textnormalization import normalizedSkill, normalizedTitle, normalizedCompany
from nuts import NutsRegions
import sys
from sqlalchemy import func, distinct


columns = {
    '-S' : LIProfileSkill.nrmSkill,
    '-s' : LIProfileSkill.nrmSkill,
    '-t' : LIProfileSkill.nrmTitle,
    '-c' : LIProfileSkill.nrmCompany,
}

querytypes = {
    '-S' : 'skill',
    '-s' : 'skill',
    '-t' : 'title',
    '-c' : 'company',
}

try:
    language = sys.argv[1]
    typeflag = sys.argv[2]
    query    = sys.argv[3]
    if typeflag not in columns:
        raise ValueError('Invalid query type')
except (ValueError, IndexError):
    print('usage: python3 geekmaps_query.py (en | nl) '
          '[(-S | -s | -t | -c) <query>]')
    exit(1)


gmdb = GeekMapsDB(conf.GEEKMAPS_DB)
nuts = NutsRegions(conf.NUTS_DATA)
nutsids = [id for id, shape in nuts.level(3)]

entities, words = gmdb.findEntities(querytypes[typeflag], language, query)
entityids = [e[0] for e in entities]

q = gmdb.query(LIProfileSkill.nuts3,
               func.count(distinct(LIProfileSkill.profileId))) \
        .filter(columns[typeflag].in_(entityids))
if typeflag == '-S':
    q = q.filter(LIProfileSkill.rank > 0.0)
q = q.group_by(LIProfileSkill.nuts3)

counts = dict((id, 0) for id in nutsids)
total = 0
for id, count in q:
    counts[id] = count
    total += count

counts = sorted(list(counts.items()))
for id, count in counts:
    print(id, count)
print('total', total)

