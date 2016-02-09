import conf
from analyticsdb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import splitProcess, processDb
from textnormalization import splitNrmName
import argparse


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
        language, skillwords = splitNrmName(skill)
        for word in set(skillwords.split()):
            andb.addFromDict({
                'language' : language,
                'word'     : word,
                'nrmName'  : skill,
                }, SkillWord)
            
    processDb(q, addSkill, andb, logger=logger)

def countSkillWords(jobid, fromword, toword):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(SkillWord.language, SkillWord.word,
                   func.sum(Skill.profileCount),
                   func.sum(Skill.experienceCount)) \
            .join(Skill) \
            .filter(SkillWord.word >= fromword)
    if toword is not None:
        q = q.filter(SkillWord.word < toword)
    q = q.group_by(SkillWord.language, SkillWord.word)

    def addCounts(rec):
        language, word, profileCount, experienceCount = rec
        
        andb.addFromDict({
            'language' : language,
            'word' : word,
            'profileSkillCount'    : profileCount,
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
        language, titlewords = splitNrmName(title)
        for word in set(titlewords.split()):
            andb.addFromDict({
                'language' : language,
                'word' : word,
                'nrmName' : title,
                }, TitleWord)
            
    processDb(q, addTitle, andb, logger=logger)

def countTitleWords(jobid, fromword, toword):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(TitleWord.language, TitleWord.word,
                   func.sum(Title.profileCount),
                   func.sum(Title.experienceCount)) \
            .join(Title) \
            .filter(TitleWord.word >= fromword)
    if toword is not None:
        q = q.filter(TitleWord.word < toword)
    q = q.group_by(TitleWord.language, TitleWord.word)

    def addCounts(rec):
        language, word, profileCount, experienceCount = rec
        
        andb.addFromDict({
            'language' : language,
            'word' : word,
            'profileTitleCount' : profileCount,
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
        language, companywords = splitNrmName(company)
        for word in set(companywords.split()):
            andb.addFromDict({
                'language' : language,
                'word' : word,
                'nrmName' : company,
                }, CompanyWord)
            
    processDb(q, addCompany, andb, logger=logger)

def countCompanyWords(jobid, fromword, toword):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(CompanyWord.language, CompanyWord.word,
                   func.sum(Company.profileCount),
                   func.sum(Company.experienceCount)) \
            .join(Company) \
            .filter(CompanyWord.word >= fromword)
    if toword is not None:
        q = q.filter(CompanyWord.word < toword)
    q = q.group_by(CompanyWord.language, CompanyWord.word)

    def addCounts(rec):
        language, word, profileCount, experienceCount = rec
        
        andb.addFromDict({
            'language' : language,
            'word' : word,
            'profileCompanyCount' : profileCount,
            'experienceCompanyCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, logger=logger)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('njobs',
                        help='The number of parallel jobs.',
                        type=int)
    parser.add_argument('batchsize',
                        help='The number of rows in each parallel batch.',
                        type=int)
    parser.add_argument('catalog', help=
                        'The type of word list to build. If omitted all are built.',
                        choices=['skills', 'titles', 'companies', 'words'],
                        default=None,
                        nargs='?')
    parser.add_argument('--from-item', help=
                        'The skill/title/company to start from. Useful for crash '
                        'recovery. Irrelevant for `words` catalog.',
                        default=None)
    args = parser.parse_args()
    
    njobs = args.njobs
    batchsize = args.batchsize
    catalog = args.catalog
    startval = args.from_item
    
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

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
                     workdir='jobs', prefix='count_titlewords')

        logger.log('\nBuilding company word counts.\n')
        q = andb.query(CompanyWord.word)
        splitProcess(q, countCompanyWords, batchsize,
                     njobs=njobs, logger=logger,
                     workdir='jobs', prefix='count_companywords')

