import conf
from textnormalization import _stopwords, clean
from analyticsdb import *
from datetime import date
from sqlalchemy import and_, or_
from geoalchemy2.shape import to_shape
import csv
import sys
from logger import Logger
from windowquery import splitProcess
import glob
import fileinput

def substringFilter(col, strings):
    statements = []
    for s in strings:
        statements.append(col == s)
        statements.append(col.like('% '+s))
        statements.append(col.like(s+' %'))
        statements.append(col.like('% '+s+' %'))
    return or_(*statements)

def normalizedCompany(name):
    stopwords = _stopwords | set(['limited', 'ltd', 'uk'])
    return clean(name,
                 nospace='\'’',
                 lowercase=True,
                 removebrackets=True,
                 removestopwords=stopwords)
    
def writeProfiles(jobid, fromid, toid, nrmCompanies, tmpdir, adminMode):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    
    userfile = open(tmpdir+'users{0:03d}.csv'.format(jobid), 'w')
    userwriter = csv.writer(userfile)

    print(fromid, toid)
    q = andb.query(LIProfile) \
            .filter(LIProfile.nrmCompany.in_(nrmCompanies.keys())) \
            .filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)
        
    for liprofile in q:
        lonstr = ''
        latstr = ''
        if liprofile.location is not None and liprofile.location.geo is not None:
            point = to_shape(liprofile.location.geo)
            lonstr = str(point.x)
            latstr = str(point.y)

        title = ''
        if liprofile.title is not None:
            title = liprofile.title.name
        company = ''
        companyId = ''
        if liprofile.company is not None:
            company = liprofile.company.name
            companyId = nrmCompanies[liprofile.nrmCompany]

        startdate = ''
        if liprofile.experiences is not None:
            startdates = [e.start for e in liprofile.experiences \
                          if e.end is None and e.start is not None \
                          and e.nrmCompany == liprofile.nrmCompany]
            if startdates:
                startdate = min(startdates).strftime('%Y-%m-%d')
            
        row = [liprofile.id, latstr, lonstr,
               title, liprofile.rawTitle,
               company, liprofile.rawCompany, companyId,
               startdate]
        if adminMode:
            row += [liprofile.datoinId]
        userwriter.writerow(row)

    userfile.close()



andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    inputfile = sys.argv[3]
    outputfile = sys.argv[4]
    adminMode = False
    if len(sys.argv) > 5:
        adminMode = sys.argv[5] == '--admin'
except ValueError:
    logger.log('usage: python3 nesta_dump.py <njobs> <batchsize> <input>.csv <output>.csv [--admin]\n')


nrmCompanies = {}
with open(inputfile, 'r') as csvinput:
    csvreader = csv.reader(csvinput)

    next(csvreader, None)
    for row in csvreader:
        if len(row) <= 2:
            continue
        if len(row) > 3 and int(row[3]) <= 0:
            continue
        
        companyId = row[0]
        nrmCompany = row[2]
        nrmCompanies[nrmCompany] = companyId
            

userfile = open(outputfile, 'w')
userwriter = csv.writer(userfile)
titlerow = ['user id', 'latitude', 'longitude',
            'processed job title', 'original job title',
            'processed company name', 'original company name', 'company id',
            'start date']
if adminMode:
    titlerow += ['datoin id']
userwriter.writerow(titlerow)

q = andb.query(LIProfile.id) \
        .filter(LIProfile.nrmCompany.in_(nrmCompanies.keys()))
tmpdir = 'jobs/' if njobs <= 1 else ''
splitProcess(q, writeProfiles, batchsize,
             args=[nrmCompanies, tmpdir, adminMode], njobs=njobs, logger=logger,
             workdir='jobs', prefix='nesta_dump')

with fileinput.input(glob.glob('jobs/users*.csv')) as fin:
    for line in fin:
        userfile.write(line)
userfile.close()
