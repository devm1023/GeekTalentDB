import conf
from analyticsdb import *
from sqlalchemy import case, or_, func
from sqlalchemy.sql.expression import literal_column, union_all
import sys

_searchtypes = {'-s' : 'skill', '-t' : 'title', '-c' : 'company'}

try:
    language = sys.argv[1]
    searchargs = sys.argv[2:]
    if not searchargs or len(searchargs) % 2 != 0:
        raise ValueError('Invalid query list.')
    searchterms = []
    for i in range(0, len(searchargs), 2):
        searchterms.append((_searchtypes[searchargs[i]], searchargs[i+1]))
except (ValueError, IndexError, KeyError):
    sys.stdout.write('usage: python3 analytics_search.py '
                     '<language> (-s | -t | -c) <query1> '
                     '[(-s | -t | -c) <query2> ...]\n')
    sys.stdout.flush()
    exit(1)


categories = ['title', 'skill', 'company']

andb = AnalyticsDB(conf.ANALYTICS_DB)

searchentities = []
entitycounts = dict((c, 0) for c in categories)
entitysets = dict((c, set()) for c in categories)
for searchtype, searchterm in searchterms:
    elist, words = andb.findEntities(searchtype, language, searchterm,
                                     exact=True)
    escore = sum(c for e, n, c in elist)
    elist = [e for e, n, c in elist]
    searchentities.append((searchtype, elist, escore))
    entitycounts[searchtype] += 1
    entitysets[searchtype].update(elist)

for (searchtype, entities, score), (_, searchterm) \
    in zip(searchentities, searchterms):
    print('\n'+searchterm)
    for entity in entities:
        print('    '+entity)


if entitysets['title'] and entitysets['company']:
    liprofilefilter = or_(LIProfile.nrmTitle.in_(entitysets['title']),
                          LIProfile.nrmCompany.in_(entitysets['company']))
    experiencefilter = or_(Experience.nrmTitle.in_(entitysets['title']),
                           Experience.nrmCompany.in_(entitysets['company']))
elif entitysets['title']:
    liprofilefilter = LIProfile.nrmTitle.in_(entitysets['title'])
    experiencefilter = Experience.nrmTitle.in_(entitysets['title'])
elif entitysets['company']:
    liprofilefilter = LIProfile.nrmCompany.in_(entitysets['company'])
    experiencefilter = Experience.nrmCompany.in_(entitysets['company'])
else:
    liprofilefilter = None
    experiencefilter = None

if entitysets['skill']:
    skillfilter = LIProfileSkill.nrmName.in_(entitysets['skill'])
else:
    skillfilter = None
                           
liprofilequery = andb.query(LIProfile.id.label('s_id'))
if liprofilefilter is not None:
    liprofilequery.filter(liprofilefilter)
experiencequery = andb.query(Experience.liprofileId.label('s_id'))
if experiencefilter is not None:
    experiencequery = experiencequery.filter(experiencefilter)
skillquery = andb.query(LIProfileSkill.liprofileId.label('s_id'))
if skillfilter is not None:
    skillquery = skillquery.filter(skillfilter)

subq = union_all(liprofilequery, experiencequery, skillquery)
countcol = func.count().label('countcol')
print(subq.c)
q = andb.query(subq.c.s_id, countcol) \
        .group_by(subq.c.s_id) \
        .order_by(countcol.desc())
    

for row in q:
    print(row)
                            
# titles = entities[0][1]
# casecol = case([(Experience.nrmTitle.in_(titles), 1)], else_=0)
# q = andb.query(Experience.id, casecol) \
#         .filter(Experience.nrmTitle.in_(titles)) \
#         .limit(100)

# print(titles)
# print(q.all())
