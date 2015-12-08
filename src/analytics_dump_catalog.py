import conf
from analyticsdb import *
import sys
import csv

_columns = {
    'skills'     : (Skill.name, Skill.experienceCount, Skill.language),
    'titles'     : (Title.name, Title.experienceCount, Title.language),
    'companies'  : (Company.name, Company.experienceCount, Company.language),
    'institutes' : (Institute.name, Institute.count, Institute.language),
    'degrees'    : (Degree.name, Degree.count, Degree.language),
    'subjects'   : (Subject.name, Subject.count, Subject.language),
    'sectors'    : (Sector.name, Sector.count, None)
}

andb = AnalyticsDB(conf.ANALYTICS_DB)



try:
    catalog = sys.argv[1]
    if catalog not in _columns:
        raise ValueError('Invalid catalog.')
    mincount = sys.argv[2]
    filename = sys.argv[3]
except (IndexError, ValueError):
    print('usage: python3 analytics_dump_catalog.py <catalog> '
          '<mincount> <filename>')
    exit(1)

namecol, countcol, langcol = _columns[catalog]
if langcol is not None:
    q = andb.query(namecol, countcol, langcol)
else:
    q = andb.query(namecol, countcol)
    
q = q.filter(countcol >= mincount).order_by(countcol.desc())
with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for row in q:
        csvwriter.writerow(row)


