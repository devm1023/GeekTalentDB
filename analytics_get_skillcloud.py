import conf
from analyticsdb import *
from canonicaldb import normalizedTitle
from sqlalchemy import func
import sys
from logger import Logger


def getScores(q, categoryNameCol, categoryCountCol, skillCountCol, nrecords):
    coincidenceCounts = {}
    categories = set()
    for category, skill, coincidenceCount in q:
        coincidenceCounts[skill] \
            = coincidenceCount + coincidenceCounts.get(skill, 0)
        categories.add(category)
    skillCounts = {}
    for skill in coincidenceCounts:
        skillCounts[skill] = andb.query(Skill.name, skillCountCol) \
                                 .filter(Skill.nrmName == skill) \
                                 .first()
    categoryCount = 0
    for category in categories:
        categoryCount += andb.query(categoryCountCol) \
                             .filter(categoryNameCol == category) \
                             .first()[0]
    scores = []
    for skill, coincidenceCount in coincidenceCounts.items():
        skillName, skillCount = skillCounts[skill]
        scores.append((skill, skillName, skillScore(coincidenceCount,
                                                    categoryCount,
                                                    skillCount,
                                                    nrecords)))
    scores.sort(key=lambda s: s[2])
    categories = list(categories)
    categories.sort()

    return scores, categories


try:
    querytype = sys.argv[1]
    if querytype not in ['title']:
        raise ValueError('Invalid category string')
    query = sys.argv[2]
except (ValueError, KeyError):
    sys.stdout.write('usage: python3 analytics_get_skillclouds.py title <query>\n')
    sys.stdout.flush()

andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

if querytype == 'title':
    nrecords = andb.query(Experience.id).count()
    nrmQuery = normalizedTitle(query)
    q = andb.query(TitleSkill.nrmTitle,
                   TitleSkill.nrmSkill,
                   TitleSkill.experienceCount) \
            .filter(TitleSkill.nrmTitle.ilike('%'+nrmQuery+'%'))
    scores, categories = getScores(q, Title.nrmName, Title.experienceCount,
                                   Skill.experienceCount, nrecords)
        
    for skill, skillName, score in scores:
        logger.log('{0:> 6.1f}% {1:s}\n'.format(score*100, skillName))
    logger.log('\n')
    logger.log('Matched titles\n')
    for category in categories:
        logger.log('    '+category+'\n')
    

