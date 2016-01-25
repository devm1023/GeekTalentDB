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
    sys.argv.pop(0)
    catalog = sys.argv.pop(0)
    if catalog not in _columns:
        raise ValueError('Invalid catalog.')
    mincount = int(sys.argv.pop(0))
    filename = sys.argv.pop(0)

    language = None
    while sys.argv:
        option = sys.argv.pop(0).split('=')
        if len(option) == 2:
            value = option[1]
            option = option[0]
            if option == '--language':
                if value not in ['en', 'nl']:
                    raise ValueError('Invalid language.')
                else:
                   language = value 
        else:
            raise ValueError('Invalid option.')
        
except (IndexError, ValueError):
    print('usage: python3 analytics_dump_catalog.py <catalog> '
          '<mincount> <filename> [--language=(en | nl)]')
    exit(1)

namecol, countcol, langcol = _columns[catalog]
if langcol is not None:
    q = andb.query(namecol, countcol, langcol)
    if language is not None:
        q = q.filter(langcol == language)
else:
    q = andb.query(namecol, countcol)
    
q = q.filter(countcol >= mincount).order_by(countcol.desc())
with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for row in q:
        csvwriter.writerow(row)


