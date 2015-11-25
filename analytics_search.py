import conf
from analyticsdb import *
import sys

try:
    querytype = sys.argv[1]
    if querytype not in ['title', 'company', 'skill']:
        raise ValueError('Invalid category string')
    query = sys.argv[2]
    filename = None
    if len(sys.argv) > 3:
        filename = sys.argv[3]
except (ValueError, KeyError):
    sys.stdout.write('usage: python3 analytics_get_skillclouds.py '
                     '(title | company | skill) <query>\n')
    sys.stdout.flush()
    exit(1)


andb = AnalyticsDB(conf.ANALYTICS_DB)

entities, words = andb.findEntities(querytype, query)

print('MATCHING WORDS:')
for w in words:
    print('   ', w)
print('\nMATCHING ENTITIES:')
for entity, count in entities:
    print('   ', count, entity)
