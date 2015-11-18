import conf
from analyticsdb import *
from logger import Logger
import sys
from windowquery import splitProcess, processDb


def addSkillWords(jobid, fromskill, toskill):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Skill.nrmName) \
            .filter(Skill.nrmName >= fromskill)
    if toskill is not None:
        q = q.filter(Skill.nrmName < toskill)
    q = q.order_by(Skill.nrmName)

    def addSkill(rec):
        skill, = rec
        for word in skill.split():
            andb.addFromDict({
                'word' : word,
                'nrmSkill' : skill,
                }, SkillWord)
            
    processDb(q, addSkill, andb, logger=logger)


def addTitleWords(jobid, fromtitle, totitle):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Title.nrmName) \
            .filter(Title.nrmName >= fromtitle)
    if totitle is not None:
        q = q.filter(Title.nrmName < totitle)
    q = q.order_by(Title.nrmName)

    def addTitle(rec):
        title, = rec
        for word in title.split():
            andb.addFromDict({
                'word' : word,
                'nrmTitle' : title,
                }, TitleWord)
            
    processDb(q, addTitle, andb, logger=logger)


def addCompanyWords(jobid, fromcompany, tocompany):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Company.nrmName) \
            .filter(Company.nrmName >= fromcompany)
    if tocompany is not None:
        q = q.filter(Company.nrmName < tocompany)
    q = q.order_by(Company.nrmName)

    def addCompany(rec):
        company, = rec
        for word in company.split():
            andb.addFromDict({
                'word' : word,
                'nrmCompany' : company,
                }, CompanyWord)
            
    processDb(q, addCompany, andb, logger=logger)



andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    catalog = None
    startval = None
    if len(sys.argv) > 3:
        catalog = sys.argv[3]
        if catalog not in ['skills', 'titles', 'companies']:
            raise ValueError('Invalid catalog string')
    if len(sys.argv) > 4:
        startval = sys.argv[4]
except ValueError:
    logger.log('usage: python3 build_catalogs.py <njobs> <batchsize> '
               '[(skills | titles | sectors | companies | locations | '
               'institutes | degrees | subjects) [<start-value>]]\n')

if catalog is None or catalog == 'skills':
    logger.log('\nBuilding skills catalog.\n')
    q = andb.query(Skill.nrmName)
    if startval:
        q = q.filter(Skill.nrmName >= startval)
    splitProcess(q, addSkillWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_skillwords')

if catalog is None or catalog == 'titles':
    logger.log('\nBuilding titles catalog.\n')
    q = andb.query(Title.nrmName)
    if startval:
        q = q.filter(Title.nrmName >= startval)
    splitProcess(q, addTitleWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_titlewords')

if catalog is None or catalog == 'companys':
    logger.log('\nBuilding companies catalog.\n')
    q = andb.query(Company.nrmName)
    if startval:
        q = q.filter(Company.nrmName >= startval)
    splitProcess(q, addCompanyWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_companywords')

    
