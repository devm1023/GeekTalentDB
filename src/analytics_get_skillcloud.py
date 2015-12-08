import conf
from analyticsdb import *
from textnormalization import normalizedTitle, normalizedCompany, normalizedSkill
from sqlalchemy import func, or_
import sys
import csv
from logger import Logger


def getScores(q, categoryNameCol, categoryCountCol, skillCountCol, nrecords):
    coincidenceCounts = {}
    categories = {}
    for category, skill, coincidenceCount in q:
        coincidenceCounts[skill] \
            = coincidenceCount + coincidenceCounts.get(skill, 0)
        categories[category] = 0
    skillCounts = {}
    for skill in coincidenceCounts:
        skillCounts[skill] = andb.query(Skill.name, skillCountCol) \
                                 .filter(Skill.nrmName == skill) \
                                 .first()
    categoryCount = 0
    for category in categories:
        count = andb.query(categoryCountCol) \
                    .filter(categoryNameCol == category) \
                    .first()[0]
        categories[category] += count
        categoryCount += count 
    scores = []
    for skill, coincidenceCount in coincidenceCounts.items():
        skillName, skillCount = skillCounts[skill]
        scores.append((skill, skillName, skillScore(coincidenceCount,
                                                    categoryCount,
                                                    skillCount,
                                                    nrecords)))
    scores.sort(key=lambda s: s[2])
    categories = list(categories.items())
    categories.sort(key=lambda c: c[1])

    return scores, categories, categoryCount


try:
    querytype = sys.argv[1]
    if querytype not in ['title', 'company', 'skill']:
        raise ValueError('Invalid category string')
    query = sys.argv[2]
    filename = None
    if len(sys.argv) > 3:
        filename = sys.argv[3]
except (ValueError, KeyError):
    sys.stdout.write('usage: python3 analytics_get_skillclouds.py '
                     '(title | company | skill) <query>\n')
    sys.stdout.flush()
    exit(1)

andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

csvwriter = None
csvfile = None
if filename is not None:
    csvfile = open(filename, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['skill', 'relevance'])

if querytype == 'title':
    nrecords = andb.query(Experience.id).count()
    titles, words = andb.findEntities('title', query)
    titles = [title[0] for title in titles]
    q = andb.query(TitleSkill.nrmTitle,
                   TitleSkill.nrmSkill,
                   TitleSkill.experienceCount) \
            .filter(TitleSkill.nrmTitle.in_(titles))
    scores, categories, nmatches \
        = getScores(q, Title.nrmName, Title.experienceCount,
                    Skill.experienceCount, nrecords)
        
    for skill, skillName, score in scores:
        logger.log('{0:> 6.1f}% {1:s}\n'.format(score*100, skillName))
        if csvwriter is not None:
            csvwriter.writerow([skillName, score])
    logger.log('\nMatching records: {0:d}\n'.format(nmatches))
    logger.log('\nMatched titles\n')
    for category, count in categories:
        logger.log('    {0:7d} {1:s}\n'.format(count, category))
if querytype == 'company':
    nrecords = andb.query(Experience.id).count()
    companies, words = andb.findEntities('company', query)
    companies = [company[0] for company in companies]
    q = andb.query(CompanySkill.nrmCompany,
                   CompanySkill.nrmSkill,
                   CompanySkill.experienceCount) \
            .filter(CompanySkill.nrmCompany.in_(companies))
    scores, categories, nmatches \
        = getScores(q, Company.nrmName, Company.experienceCount,
                    Skill.experienceCount, nrecords)
        
    for skill, skillName, score in scores:
        logger.log('{0:> 6.1f}% {1:s}\n'.format(score*100, skillName))
        if csvwriter is not None:
            csvwriter.writerow([skillName, score])
    logger.log('\nMatching records: {0:d}\n'.format(nmatches))
    logger.log('\nMatched companies\n')
    for category, count in categories:
        logger.log('    {0:7d} {1:s}\n'.format(count, category))
if querytype == 'skill':
    nrecords = andb.query(Experience.id).count()
    skills, words = andb.findEntities('skill', query)
    skills = [skill[0] for skill in skills]
    print(skills)
    q = andb.query(SkillSkill.nrmSkill1,
                   SkillSkill.nrmSkill2,
                   SkillSkill.experienceCount) \
            .filter(SkillSkill.nrmSkill1.in_(skills))
    scores, categories, nmatches \
        = getScores(q, Skill.nrmName, Skill.experienceCount,
                    Skill.experienceCount, nrecords)
        
    for skill, skillName, score in scores:
        logger.log('{0:> 6.1f}% {1:s}\n'.format(score*100, skillName))
        if csvwriter is not None:
            csvwriter.writerow([skillName, score])
    logger.log('\nMatching records: {0:d}\n'.format(nmatches))
    logger.log('\nMatched skills\n')
    for category, count in categories:
        logger.log('    {0:7d} {1:s}\n'.format(count, category))
    
if csvfile is not None:
    csvfile.close()
