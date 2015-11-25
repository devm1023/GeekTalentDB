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
q = andb.query(CareerStep.count, Title1.name, Title2.name, Title3.name) \
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

with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for count, title1, title2, title3 in q:
        csvwriter.writerow([title1, title2, title3, count])

