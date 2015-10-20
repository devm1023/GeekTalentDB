import conf
from geekmapsdb import *
from canonicaldb import normalizedSkill, normalizedTitle, normalizedCompany
from nuts import NutsRegions
import sys
from sqlalchemy import func, distinct


try:
    if len(sys.argv) <= 2:
        raise ValueError('Invalid argument list')
    querytype = sys.argv[1]
    if querytype not in ['-S', '-s', '-j', '-c']:
        raise ValueError('Invalid query type')
    querytext = sys.argv[2]
except ValueError:
    print('usage: python3 geekmaps_query.py (-S | -s | -j | -c) <query>')
    exit(1)


gmdb = GeekMapsDB(conf.GEEKMAPS_DB)
nuts = NutsRegions(conf.NUTS_DATA)

counts = dict((id, 0) for id, shape in nuts.level(3))

reinforced = False
if querytype in ['-s', '-S']:
    value = normalizedSkill(querytext)
    column = LIProfileSkill.nrmSkill
    if querytype == '-S':
        reinforced = True
elif querytype == '-j':
    value = normalizedTitle(querytext)
    column = LIProfileSkill.nrmTitle
elif querytype == '-c':
    value = normalizedCompany(querytext)
    column = LIProfileSkill.nrmCompany

q = gmdb.query(LIProfileSkill.nuts3,
               func.count(distinct(LIProfileSkill.profileId))) \
        .filter(column == value, nuts3 != None)
if reinforced:
    q = q.filter(LIProfileSkill.rank > 0.0)
q = q.group_by(LIProfileSkill.nuts3)

total = 0
for id, count in q:
    counts[id] = count
    total += count
counts['total'] = total

counts = sorted(list(counts.items()))
for id, count in counts:
    print(id, count)




    
