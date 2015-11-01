import conf
from analyticsdb import *
from sqlalchemy import func
import sys
from logger import Logger
from windowquery import splitProcess, processDb

# nrmTitle = sys.argv[1]
skillcountthreshold = 10
titlecountthreshold = 10
countthreshold = 3


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
            .filter(Title.experienceCount >= titlecountthreshold) \
            .filter(Skill.experienceCount >= skillcountthreshold) \
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


andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    category = None
    startval = None
    if len(sys.argv) > 3:
        category = sys.argv[3]
        if category not in ['titles']:
            raise ValueError('Invalid category string')
    if len(sys.argv) > 4:
        startval = sys.argv[4]
except ValueError:
    logger.log('usage: python3 build_skillclouds.py <njobs> <batchsize> '
               '[titles [<start-value>]]\n')
    
if category is None or category == 'titles':
    titlethreshold = 10
    skillthreshold = 10
    countthreshold = 3
    logger.log('\nBuilding titles skillclouds.\n')
    q = andb.query(Title.nrmName)
    if startval:
        q = q.filter(Title.nrmName >= startval)
    splitProcess(q, addTitleSkills, batchsize,
                 njobs=njobs, logger=logger,
                 args=[titlethreshold, skillthreshold, countthreshold],
                 workdir='jobs', prefix='build_title_skillclouds')
    
