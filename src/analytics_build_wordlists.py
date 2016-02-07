import conf
from analyticsdb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import splitProcess, processDb
from textnormalization import splitNrmName
import argparse


def addSkillWords(batchsize):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Skill.nrmName)
    
    def addSkill(rec):
        skill, = rec
        language, skillwords = splitNrmName(skill)
        for word in skillwords.split():
            andb.addFromDict({
                'language' : language,
                'word'     : word,
                'nrmName'  : skill,
                }, SkillWord)
            
    processDb(q, addSkill, andb, batchsize=batchsize, logger=logger)

def countSkillWords(batchsize):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(SkillWord.language, SkillWord.word,
                   func.sum(Skill.profileCount),
                   func.sum(Skill.experienceCount)) \
            .join(Skill)
    q = q.group_by(SkillWord.language, SkillWord.word)

    def addCounts(rec):
        language, word, profileCount, experienceCount = rec
        
        andb.addFromDict({
            'language' : language,
            'word' : word,
            'profileSkillCount'    : profileCount,
            'experienceSkillCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, batchsize=batchsize, logger=logger)


def addTitleWords(batchsize):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Title.nrmName) \
            .order_by(Title.nrmName)

    def addTitle(rec):
        title, = rec
        language, titlewords = splitNrmName(title)
        for word in titlewords.split():
            andb.addFromDict({
                'language' : language,
                'word' : word,
                'nrmName' : title,
                }, TitleWord)
            
    processDb(q, addTitle, andb, batchsize=batchsize, logger=logger)

def countTitleWords(batchsize):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(TitleWord.language, TitleWord.word,
                   func.sum(Title.profileCount),
                   func.sum(Title.experienceCount)) \
            .join(Title) \
            .group_by(TitleWord.language, TitleWord.word)

    def addCounts(rec):
        language, word, profileCount, experienceCount = rec
        
        andb.addFromDict({
            'language' : language,
            'word' : word,
            'profileTitleCount' : profileCount,
            'experienceTitleCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, batchsize=batchsize, logger=logger)
    

def addCompanyWords(batchsize):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Company.nrmName) \
            .order_by(Company.nrmName)

    def addCompany(rec):
        company, = rec
        language, companywords = splitNrmName(company)
        for word in companywords.split():
            andb.addFromDict({
                'language' : language,
                'word' : word,
                'nrmName' : company,
                }, CompanyWord)
            
    processDb(q, addCompany, andb, batchsize=batchsize, logger=logger)

def countCompanyWords(batchsize):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(CompanyWord.language, CompanyWord.word,
                   func.sum(Company.profileCount),
                   func.sum(Company.experienceCount)) \
            .join(Company) \
            .group_by(CompanyWord.language, CompanyWord.word)

    def addCounts(rec):
        language, word, profileCount, experienceCount = rec
        
        andb.addFromDict({
            'language' : language,
            'word' : word,
            'profileCompanyCount' : profileCount,
            'experienceCompanyCount' : experienceCount,
        }, Word)
            
    processDb(q, addCounts, andb, batchsize=batchsize, logger=logger)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('catalog', help=
                        'The typw of word list to build. If omitted all are built.',
                        choices=['skills', 'titles', 'companies', 'counts'],
                        default=None, nargs='?')
    parser.add_argument('--batchsize', type=int, default=10000, help=
                        'Number of rows to commit in one batch.')
    args = parser.parse_args()
    catalog = args.catalog

    logger = Logger(sys.stdout)

    if catalog is None or catalog == 'skills':
        logger.log('\nBuilding skills wordlist.\n')
        addSkillWords(args.batchsize)

    if catalog is None or catalog == 'titles':
        logger.log('\nBuilding titles wordlist.\n')
        addTitleWords(args.batchsize)

    if catalog is None or catalog == 'companies':
        logger.log('\nBuilding companies wordlist.\n')
        addCompanyWords(args.batchsize)

    if catalog is None or catalog == 'counts':
        logger.log('\nBuilding skill word counts.\n')
        countSkillWords(args.batchsize)

        logger.log('\nBuilding title word counts.\n')
        countTitleWords(args.batchsize)

        logger.log('\nBuilding company word counts.\n')
        countCompanyWords(args.batchsize)

