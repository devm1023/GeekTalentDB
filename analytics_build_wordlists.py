import conf
from analyticsdb import *
from sqlalchemy import func
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
                'nrmName' : skill,
                }, SkillWord)
            
    processDb(q, addSkill, andb, logger=logger)

def countSkillWords(jobid, fromword, toword):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(SkillWord.word,
                   func.sum(Skill.liprofileCount),
                   func.sum(Skill.experienceCount)) \
            .join(Skill) \
            .filter(SkillWord.word >= fromword)
    if toword is not None:
        q = q.filter(SkillWord.word < toword)
    q = q.group_by(SkillWord.word)

    def addCounts(rec):
        word, liprofileCount, experienceCount = rec
        
        andb.addFromDict({
            'word' : word,
            'liprofileSkillCount' : liprofileCount,
            'experienceSkillCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, logger=logger)


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
                'nrmName' : title,
                }, TitleWord)
            
    processDb(q, addTitle, andb, logger=logger)

def countTitleWords(jobid, fromword, toword):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(TitleWord.word,
                   func.sum(Title.liprofileCount),
                   func.sum(Title.experienceCount)) \
            .join(Title) \
            .filter(TitleWord.word >= fromword)
    if toword is not None:
        q = q.filter(TitleWord.word < toword)
    q = q.group_by(TitleWord.word)

    def addCounts(rec):
        word, liprofileCount, experienceCount = rec
        
        andb.addFromDict({
            'word' : word,
            'liprofileTitleCount' : liprofileCount,
            'experienceTitleCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, logger=logger)
    

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
                'nrmName' : company,
                }, CompanyWord)
            
    processDb(q, addCompany, andb, logger=logger)

def countCompanyWords(jobid, fromword, toword):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(CompanyWord.word,
                   func.sum(Company.liprofileCount),
                   func.sum(Company.experienceCount)) \
            .join(Company) \
            .filter(CompanyWord.word >= fromword)
    if toword is not None:
        q = q.filter(CompanyWord.word < toword)
    q = q.group_by(CompanyWord.word)

    def addCounts(rec):
        word, liprofileCount, experienceCount = rec
        
        andb.addFromDict({
            'word' : word,
            'liprofileCompanyCount' : liprofileCount,
            'experienceCompanyCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, logger=logger)


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
    logger.log('\nBuilding skills wordlist.\n')
    q = andb.query(Skill.nrmName)
    if startval:
        q = q.filter(Skill.nrmName >= startval)
    splitProcess(q, addSkillWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_skillwords')

if catalog is None or catalog == 'titles':
    logger.log('\nBuilding titles wordlist.\n')
    q = andb.query(Title.nrmName)
    if startval:
        q = q.filter(Title.nrmName >= startval)
    splitProcess(q, addTitleWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_titlewords')

if catalog is None or catalog == 'companies':
    logger.log('\nBuilding companies wordlist.\n')
    q = andb.query(Company.nrmName)
    if startval:
        q = q.filter(Company.nrmName >= startval)
    splitProcess(q, addCompanyWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_companywords')

if catalog is None or catalog == 'words':
    logger.log('\nBuilding skill word counts.\n')
    q = andb.query(SkillWord.word)
    splitProcess(q, countSkillWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='count_skillwords')

    logger.log('\nBuilding title word counts.\n')
    q = andb.query(TitleWord.word)
    splitProcess(q, countTitleWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='count_skillwords')

    logger.log('\nBuilding company word counts.\n')
    q = andb.query(CompanyWord.word)
    splitProcess(q, countCompanyWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='count_skillwords')
    
