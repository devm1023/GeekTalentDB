from datoindb import *
from sqldb import dictFromRow
from pprint import pprint
from datetime import datetime
import sys

timestamp0 = datetime(year=1970, month=1, day=1)
try:
    fromdate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    todate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
    limit = 1
    if len(sys.argv) > 3:
        limit = int(sys.argv[3])
    offset = 0
    if len(sys.argv) > 4:
        offset = int(sys.argv[4])
except ValueError:
    print('usage: python datoin_show_profile.py <from-date> '
          '<to-date> [<limit> [<offset>]]')

fromTs = (fromdate-timestamp0).total_seconds()*1000
toTs   = (todate-timestamp0).total_seconds()*1000

dtdb = DatoinDB()

for liprofile in dtdb.query(LIProfile) \
                     .filter(LIProfile.crawledDate >= fromTs,
                             LIProfile.crawledDate < toTs) \
                     .limit(limit) \
                     .offset(offset):
    experiences = dtdb.query(Experience) \
                      .filter(Experience.parentId == liprofile.id) \
                      .all()
    educations = dtdb.query(Education) \
                      .filter(Education.parentId == liprofile.id) \
                      .all()

    profiledict = dictFromRow(liprofile)
    profiledict['experiences'] = list(map(dictFromRow, experiences))
    profiledict['educations'] = list(map(dictFromRow, educations))

    pprint(profiledict)
    
count = dtdb.query(LIProfile.id) \
            .filter(LIProfile.crawledDate >= fromTs,
                    LIProfile.crawledDate < toTs) \
            .count()

print('\n\ntotal profiles: {0:d}\n'.format(count))
