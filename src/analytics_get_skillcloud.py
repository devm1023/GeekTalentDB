import conf
from analyticsdb import *
from textnormalization import normalizedTitle, normalizedCompany, normalizedSkill
from sqlalchemy import func, or_
import sys
import csv
from logger import Logger
import argparse


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


def getSkillCloud(querytype, query):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    nrecords = andb.query(LIExperience.id).count()
    if querytype == 'title':
        matches, words = andb.findEntities('title', query)
        q = andb.query(TitleSkill.nrmTitle,
                       TitleSkill.nrmSkill,
                       TitleSkill.experienceCount) \
                .filter(TitleSkill.nrmTitle.in_(matches))
        scores, categories, nmatches \
            = getScores(q, Title.nrmName, Title.experienceCount,
                        Skill.experienceCount, nrecords)
    elif querytype == 'company':
        matches, words = andb.findEntities('company', query)
        matches = [match[0] for match in matches]
        q = andb.query(CompanySkill.nrmCompany,
                       CompanySkill.nrmSkill,
                       CompanySkill.experienceCount) \
                .filter(CompanySkill.nrmCompany.in_(matches))
        scores, categories, nmatches \
            = getScores(q, Company.nrmName, Company.experienceCount,
                        Skill.experienceCount, nrecords)
    elif querytype == 'skill':
        matches, words = andb.findEntities('skill', query)
        matches = [match[0] for match in matches]
        q = andb.query(SkillSkill.nrmSkill1,
                       SkillSkill.nrmSkill2,
                       SkillSkill.experienceCount) \
                .filter(SkillSkill.nrmSkill1.in_(matches))
        scores, categories, nmatches \
            = getScores(q, Skill.nrmName, Skill.experienceCount,
                        Skill.experienceCount, nrecords)
    else:
        raise ValueError('Invalid query type.')
    
    result = {}
    result['nrecords'] = nrecords
    result['nmatches'] = nmatches
    result['skills'] = []
    for skill, skillName, score in scores:
        result['skills'].append({'name'  : skillName,
                                 'score' : score})
    result['matches'] = []
    for match, count in categories:
        result['matches'].append({'name'  : category,
                                  'count' : count})

    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('querytype',
                        choices=['title', 'company', 'skill'],
                        help='The type of skill cloud to generate.')
    parser.add_argument('query',
                        help='The search term')
    parser.add_argument('-o',
                        help='The name of the output file (CSV).')
    args = parser.parse_args()

    logger = Logger(sys.stdout)
    result = getSkillCloud(args.querytype, args.query)

    try:
        csvwriter = None
        csvfile = None
        if filename is not None:
            csvfile = open(filename, 'w')
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['skill', 'relevance'])

        for skill in result['skills']:
            logger.log('{0:> 6.1f}% {1:s}\n' \
                       .format(skill['score']*100, skill['name']))
            if csvwriter is not None:
                csvwriter.writerow([skill['name'], skill['score']])
        logger.log('\nMatching records: {0:d}\n'.format(result['nmatches']))
        logger.log('\nMatched entities\n')
        for match in result['matches']:
            logger.log('    {0:7d} {1:s}\n' \
                       .format(match['count'], match['name']))
    finally:
        if csvfile is not None:
            csvfile.close()

