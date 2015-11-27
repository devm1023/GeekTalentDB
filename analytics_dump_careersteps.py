import conf
from analyticsdb import *
from sqlalchemy.orm import aliased
from sqlalchemy import or_
import csv
import sys

try:
    minjobcount = int(sys.argv[1])
    minpathcount = int(sys.argv[2])
    filename = sys.argv[3]
except ValueError:
    print('usage: python3 analytics_dump_careersteps.py <mincount> <filename>')
    exit(1)

andb = AnalyticsDB(conf.ANALYTICS_DB)

Title1 = aliased(Title, name='title_1')
Title2 = aliased(Title, name='title_2')
Title3 = aliased(Title, name='title_3')
q = andb.query(CareerStep.count,
               CareerStep.titlePrefix1, Title1.name,
               CareerStep.titlePrefix2, Title2.name,
               CareerStep.titlePrefix3, Title3.name) \
        .outerjoin(Title1, Title1.nrmName == CareerStep.title1) \
        .outerjoin(Title2, Title2.nrmName == CareerStep.title2) \
        .outerjoin(Title3, Title3.nrmName == CareerStep.title3) \
        .filter(or_(CareerStep.title1 == None,
                    Title1.experienceCount >= minjobcount),
                or_(CareerStep.title2 == None,
                    Title2.experienceCount >= minjobcount),
                or_(CareerStep.title3 == None,
                    Title3.experienceCount >= minjobcount),
                CareerStep.count >= minpathcount)

def capitalize(text):
    if text is None:
        return None
    words = text.split()
    words = [w[0].upper() + w[1:] for w in words]
    return ' '.join(words)

with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for count, prefix1, title1, prefix2, title2, prefix3, title3 in q:
        if title1 and prefix1:
            title1 = ' '.join([capitalize(prefix1), title1])
        if title2 and prefix2:
            title2 = ' '.join([capitalize(prefix2), title2])
        if title3 and prefix3:
            title3 = ' '.join([capitalize(prefix3), title3])
        csvwriter.writerow([title1, title2, title3, count])

