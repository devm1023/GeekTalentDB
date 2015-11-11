import conf
from textnormalization import normalizedCompany
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

mindate = date(year=2013, month=11, day=1)
companies = ['IBM']

    
def writeProfiles(jobid, fromid, toid, tmpdir, adminMode):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    
    userfile = open(tmpdir+'users{0:03d}.csv'.format(jobid), 'w')
    userwriter = csv.writer(userfile)

    print(fromid, toid)
    q = andb.query(LIProfile) \
            .filter(LIProfile.nrmCompany.in_(nrmCompanies)) \
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
        if liprofile.company is not None:
            company = liprofile.company.name

        startdate = ''
        if liprofile.experiences is not None:
            startdates = [e.start for e in liprofile.experiences \
                          if e.end is None and e.start is not None \
                          and e.nrmCompany == liprofile.nrmCompany]
            if startdates:
                startdate = min(startdates).strftime('%Y-%m-%d')
            
        row = [liprofile.id, latstr, lonstr,
               title, liprofile.rawTitle,
               company, liprofile.rawCompany, startdate]
        if adminMode:
            row += [liprofile.datoinId]
        userwriter.writerow(row)

    userfile.close()



andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    adminMode = False
    if len(sys.argv) > 3:
        adminMode = sys.argv[3] == '--admin'
except ValueError:
    logger.log('usage: python3 nesta_dump.py <njobs> <batchsize> [--admin]\n')

logger.log('Retrieving companies...')
nrmCompanies = []
for company in companies:
    nrmCompany = normalizedCompany(company)
    q = andb.query(Company.nrmName) \
            .filter(or_(Company.nrmName == nrmCompany,
                        Company.nrmName.like('% '+nrmCompany),
                        Company.nrmName.like(nrmCompany+' %'),
                        Company.nrmName.like('% '+nrmCompany+' %')))
    nrmCompanies.extend([c[0] for c in q.all()])
logger.log('done.\n')
for company in nrmCompanies:
    logger.log(company+'\n')

userfile = open('users.csv', 'w')
userwriter = csv.writer(userfile)
titlerow = ['user id', 'latitude', 'longitude',
            'processed job title', 'original job title',
            'processed company name', 'original company name',
            'start date']
if adminMode:
    titlerow += ['datoin id']
userwriter.writerow(titlerow)

q = andb.query(LIProfile.id).filter(LIProfile.nrmCompany.in_(nrmCompanies))
tmpdir = 'jobs/' if njobs <= 1 else ''
splitProcess(q, writeProfiles, batchsize,
             args=[tmpdir, adminMode], njobs=njobs, logger=logger,
             workdir='jobs', prefix='nesta_dump')

with fileinput.input(glob.glob('jobs/users*.csv')) as fin:
    for line in fin:
        userfile.write(line)
userfile.close()
