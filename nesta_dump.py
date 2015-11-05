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

    
def writeProfiles(jobid, fromid, toid):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    
    experiencefile = open('experiences{0:03d}.csv'.format(jobid), 'w')
    experiencewriter = csv.writer(experiencefile)

    userfile = open('users{0:03d}.csv'.format(jobid), 'w')
    userwriter = csv.writer(userfile)

    q = andb.query(LIProfile) \
            .join(Experience) \
            .filter(or_(LIProfile.nrmCompany.in_(nrmCompanies),
                        and_(Experience.nrmCompany.in_(nrmCompanies),
                             Experience.start != None,
                             or_(Experience.end == None,
                                 Experience.end >= mindate)))) \
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

        skills = ''
        if liprofile.skills is not None:
            skills = '|'.join([s.skill.name for s in liprofile.skills])

        title = ''
        if liprofile.title is not None:
            title = liprofile.title.name
        company = ''
        if liprofile.company is not None:
            company = liprofile.company.name

        userwriter.writerow(
            [liprofile.id, skills, latstr, lonstr, title, company])

        if liprofile.experiences is not None:
            for experience in liprofile.experiences:
                if experience.start is None:
                    continue
                if experience.end is not None and experience.end < mindate:
                    continue
                startdate = experience.start.strftime('%Y-%m-%d')
                enddate = ''
                if experience.end is not None:
                    enddate = experience.end.strftime('%Y-%m-%d')
                title = ''
                if experience.title is not None:
                    title = experience.title.name
                company = ''
                if experience.company is not None:
                    company = experience.company.name

                experiencewriter.writerow(
                    [liprofile.id, startdate, enddate, title, company])

    experiencefile.close()
    userfile.close()



andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
except ValueError:
    logger.log('usage: python3 nesta_dump.py <njobs> <batchsize>\n')

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

experiencefile = open('experiences.csv', 'w')
experiencewriter = csv.writer(experiencefile)
experiencewriter.writerow(
    ['user id', 'start date', 'end date', 'job title', 'company'])

userfile = open('users.csv', 'w')
userwriter = csv.writer(userfile)
userwriter.writerow(
    ['user id', 'skills', 'latitude', 'longitude', 'job title', 'company'])

q = andb.query(LIProfile.id)
splitProcess(q, writeProfiles, batchsize,
             njobs=njobs, logger=logger,
             workdir='jobs', prefix='nesta_dump')

with fileinput.input(glob.glob('jobs/experiences*.csv')) as fin:
    for line in fin:
        experiencefile.write(line)
experiencefile.close()

with fileinput.input(glob.glob('jobs/users*.csv')) as fin:
    for line in fin:
        userfile.write(line)
userfile.close()
