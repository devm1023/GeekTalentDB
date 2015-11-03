import conf
from analyticsdb import *
from sqlalchemy import func
from sqlalchemy.orm import aliased
import sys
from logger import Logger
from windowquery import splitProcess, processDb


def addTitleSkills(fromtitle, totitle,
                   titlethreshold, skillthreshold, countthreshold):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    count = func.count(ExperienceSkill.experienceId).label('counts')
    q = andb.query(Experience.nrmTitle,
                   ExperienceSkill.nrmSkill,
                   count) \
            .join(ExperienceSkill) \
            .join(Skill) \
            .join(Title) \
            .filter(Title.experienceCount >= titlethreshold) \
            .filter(Skill.experienceCount >= skillthreshold) \
            .filter(Title.nrmName >= fromtitle)
    if totitle is not None:
        q = q.filter(Title.nrmName < totitle)
    q = q.having(count >= countthreshold) \
         .group_by(Experience.nrmTitle, Title.experienceCount,
                   ExperienceSkill.nrmSkill, Skill.experienceCount)

    def addTitleSkill(rec):
        nrmTitle, nrmSkill, count = rec
        andb.addFromDict({
            'nrmTitle'        : nrmTitle,
            'nrmSkill'        : nrmSkill,
            'experienceCount' : count,
            },
            TitleSkill)

    processDb(q, addTitleSkill, andb, logger=logger)


def addCompanySkills(fromcompany, tocompany,
                     companythreshold, skillthreshold, countthreshold):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    count = func.count(ExperienceSkill.experienceId).label('counts')
    q = andb.query(Experience.nrmCompany,
                   ExperienceSkill.nrmSkill,
                   count) \
            .join(ExperienceSkill) \
            .join(Skill) \
            .join(Company) \
            .filter(Company.experienceCount >= companythreshold) \
            .filter(Skill.experienceCount >= skillthreshold) \
            .filter(Company.nrmName >= fromcompany)
    if tocompany is not None:
        q = q.filter(Company.nrmName < tocompany)
    q = q.having(count >= countthreshold) \
         .group_by(Experience.nrmCompany, Company.experienceCount,
                   ExperienceSkill.nrmSkill, Skill.experienceCount)

    def addCompanySkill(rec):
        nrmCompany, nrmSkill, count = rec
        andb.addFromDict({
            'nrmCompany'      : nrmCompany,
            'nrmSkill'        : nrmSkill,
            'experienceCount' : count,
            },
            CompanySkill)

    processDb(q, addCompanySkill, andb, logger=logger)


def addSkillSkills(fromskill, toskill,
                   skill1threshold, skill2threshold, countthreshold):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    ExperienceSkill1 = aliased(ExperienceSkill, name='experience_skill_1')
    ExperienceSkill2 = aliased(ExperienceSkill, name='experience_skill_2')
    Skill1 = aliased(Skill, name='skill_1')
    Skill2 = aliased(Skill, name='skill_2')
    count = func.count(ExperienceSkill1.experienceId).label('counts')
    q = andb.query(ExperienceSkill1.nrmSkill,
                   ExperienceSkill2.nrmSkill,
                   count) \
            .join(ExperienceSkill2, ExperienceSkill1.experienceId \
                  == ExperienceSkill2.experienceId) \
            .join(Skill1, ExperienceSkill1.nrmSkill == Skill1.nrmName) \
            .join(Skill2, ExperienceSkill2.nrmSkill == Skill2.nrmName) \
            .filter(ExperienceSkill1.nrmSkill != ExperienceSkill2.nrmSkill) \
            .filter(Skill1.experienceCount >= skill1threshold) \
            .filter(Skill2.experienceCount >= skill2threshold) \
            .filter(ExperienceSkill1.nrmSkill >= fromskill)
    if toskill is not None:
        q = q.filter(ExperienceSkill1.nrmSkill < toskill)
    q = q.having(count >= countthreshold) \
         .group_by(ExperienceSkill1.nrmSkill, Skill1.experienceCount,
                   ExperienceSkill2.nrmSkill, Skill2.experienceCount)

    def addSkillSkill(rec):
        nrmSkill1, nrmSkill2, count = rec
        andb.addFromDict({
            'nrmSkill1'       : nrmSkill1,
            'nrmSkill2'       : nrmSkill2,
            'experienceCount' : count,
            },
            SkillSkill)

    processDb(q, addSkillSkill, andb, logger=logger)

    

andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    categorythreshold = int(sys.argv[3])
    skillthreshold = int(sys.argv[4])
    countthreshold = int(sys.argv[5])
    category = None
    startval = None
    if len(sys.argv) > 6:
        category = sys.argv[6]
        if category not in ['titles', 'companies', 'skills']:
            raise ValueError('Invalid category string')
    if len(sys.argv) > 7:
        startval = sys.argv[7]
except ValueError:
    logger.log('usage: python3 build_skillclouds.py <njobs> <batchsize> '
               '<category-threshold> <skill-threshold> <count-threshold> '
               '[(titles | companies | skills) [<start-value>]]\n')
    
if category is None or category == 'titles':
    logger.log('\nBuilding titles skillclouds.\n')
    q = andb.query(Title.nrmName)
    if startval:
        q = q.filter(Title.nrmName >= startval)
    else:
        andb.query(TitleSkill).delete()
    splitProcess(q, addTitleSkills, batchsize,
                 njobs=njobs, logger=logger,
                 args=[categorythreshold, skillthreshold, countthreshold],
                 workdir='jobs', prefix='build_title_skillclouds')
if category is None or category == 'companies':
    logger.log('\nBuilding companies skillclouds.\n')
    q = andb.query(Company.nrmName)
    if startval:
        q = q.filter(Company.nrmName >= startval)
    else:
        andb.query(CompanySkill).delete()
    splitProcess(q, addCompanySkills, batchsize,
                 njobs=njobs, logger=logger,
                 args=[categorythreshold, skillthreshold, countthreshold],
                 workdir='jobs', prefix='build_company_skillclouds')
if category is None or category == 'skills':
    logger.log('\nBuilding skills skillclouds.\n')
    q = andb.query(Skill.nrmName)
    if startval:
        q = q.filter(Skill.nrmName >= startval)
    else:
        andb.query(SkillSkill).delete()
    splitProcess(q, addSkillSkills, batchsize,
                 njobs=njobs, logger=logger,
                 args=[categorythreshold, skillthreshold, countthreshold],
                 workdir='jobs', prefix='build_skill_skillclouds')
    
