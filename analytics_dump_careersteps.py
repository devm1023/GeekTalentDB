import conf
from analyticsdb import *
from sqlalchemy.orm import aliased
import csv
import sys

try:
    mincount = int(sys.argv[1])
    filename = sys.argv[2]
except ValueError:
    print('usage: python3 analytics_dump_careersteps.py <mincount> <filename>')
    exit(1)

andb = AnalyticsDB(conf.ANALYTICS_DB)

Title1 = aliased(Title, name='title_1')
Title2 = aliased(Title, name='title_2')
q = andb.query(CareerStep.count, Title1.name, Title2.name) \
        .join(Title1, Title1.nrmName == CareerStep.titleFrom) \
        .join(Title2, Title2.nrmName == CareerStep.titleTo) \
        .filter(CareerStep.titleFrom != CareerStep.titleTo,
                Title1.experienceCount >= mincount,
                Title2.experienceCount >= mincount)

with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for count, titlefrom, titleto in q:
        csvwriter.writerow([titlefrom, titleto, count])

