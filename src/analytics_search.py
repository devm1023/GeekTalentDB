import conf
from analyticsdb import *
from sqlalchemy import case, or_, func
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import literal_column, union_all
import sys
from pprint import pprint
from datetime import datetime
from math import exp
import pandas as pd

_categories = ['title', 'skill', 'company']

def _valueFilter(cols, sets):
    expressions = [c.in_(s) for c, s in zip(cols, sets) if s]
    if not expressions:
        return None
    elif len(expressions) == 1:
        return expressions[0]
    else:
        return or_(*expressions)

def _recencyScore(start, duration, now):
    if start is None:
        dt = 1.0
        t = 1.0
    else:
        t = max((now - start).total_seconds()/(60*60*24*365), 0.0)
        if duration is None:
            dt = t
        else:
            dt = max(min(duration/365, t), 0.0)

    T = 10.0  # lifetime in years
    return T*exp(-t/T)*(exp(dt/T)-1)
    
def findEntities(andb, language, searchterms):
    entities = dict((c, []) for c in _categories)
    for searchtype, searchterm in searchterms:
        elist, words = andb.findEntities(searchtype, language, searchterm,
                                         exact=True)
        escore = sum(c for e, n, c in elist)
        eset = set(e for e, n, c in elist)
        entities[searchtype].append({'term'    : searchterm,
                                     'matches' : eset,
                                     'score'   : escore})
    return entities
        
def findCandidates(andb, entities):
    entitysets = {}
    nentities = 0
    for c in _categories:
        entitysets[c] = set()
        for term in entities[c]:
            entitysets[c].update(term['matches'])
            nentities += 1

    liprofilefilter \
        = _valueFilter([LIProfile.nrmTitle,  LIProfile.nrmCompany],
                       [entitysets['title'], entitysets['company']])
    experiencefilter \
        = _valueFilter([Experience.nrmTitle, Experience.nrmCompany],
                       [entitysets['title'], entitysets['company']])
    skillfilter \
        = _valueFilter([LIProfileSkill.nrmName], [entitysets['skill']])
    
    queries = []
    if liprofilefilter is not None:
        liprofilequery = andb.query(LIProfile.id.label('s_id')) \
                             .filter(liprofilefilter)
        queries.append(liprofilequery)
    if experiencefilter is not None:
        experiencequery = andb.query(Experience.liprofileId.label('s_id')) \
                              .filter(experiencefilter)
        queries.append(experiencequery)
    if skillfilter is not None:
        skillquery = andb.query(LIProfileSkill.liprofileId.label('s_id')) \
                         .filter(skillfilter)
        queries.append(skillquery)

    if not queries:
        return []

    subq = aliased(union_all(*queries))
    countcol = func.count().label('countcol')
    q = andb.query(subq.c.s_id, countcol) \
            .group_by(subq.c.s_id) \
            .order_by(countcol.desc()) \
            .having(countcol >= nentities)

    candidates = [id for id, count in q]
    return candidates


def rankCandidates(andb, now, ids, entities):
    entitysets = {}
    for c in _categories:
        entitysets[c] = set()
        for term in entities[c]:
            entitysets[c].update(term['matches'])

    hasTitleCols = ['hasTitle:'+repr(i) \
                    for i in range(len(entities['title']))]
    titleScoreCols = ['titleScore:'+repr(i) \
                     for i in range(len(entities['title']))]
    hasSkillCols = ['hasSkill:'+repr(i) \
                    for i in range(len(entities['skill']))]
    skillScoreCols = ['skillScore:'+repr(i) \
                     for i in range(len(entities['skill']))]
    hasCompanyCols = ['hasCompany:'+repr(i) \
                      for i in range(len(entities['company']))]
    companyScoreCols = ['companyScore:'+repr(i) \
                       for i in range(len(entities['company']))]

    coldict = {'isMatch' : True, 'totalScore' : 0.0}
    for hasTitleCol, titleScoreCol in zip(hasTitleCols, titleScoreCols):
        coldict[hasTitleCol] = False
        coldict[titleScoreCol] = 0.0
    for hasSkillCol, skillScoreCol in zip(hasSkillCols, skillScoreCols):
        coldict[hasSkillCol] = False
        coldict[skillScoreCol] = 0.0
    for hasCompanyCol, companyScoreCol in zip(hasCompanyCols, companyScoreCols):
        coldict[hasCompanyCol] = False
        coldict[companyScoreCol] = 0.0
        
    df = pd.DataFrame(coldict, index=ids)

    # score profile titles and companies
    filter = _valueFilter([LIProfile.nrmTitle,  LIProfile.nrmCompany],
                          [entitysets['title'], entitysets['company']])
    if filter is not None:
        q = andb.query(LIProfile.id, LIProfile.nrmTitle, LIProfile.nrmCompany) \
                .filter(LIProfile.id.in_(ids), filter)
        for id, title, company in q:
            for hasTitleCol, titleScoreCol, e \
                in zip(hasTitleCols, titleScoreCols, entities['title']):
                if title in e['matches']:
                    df.loc[id, hasTitleCol] = True
                    df.loc[id, titleScoreCol] \
                        += _recencyScore(None, None, now)
            for hasCompanyCol, companyScoreCol, e \
                in zip(hasCompanyCols, companyScoreCols, entities['company']):
                if company in e['matches']:
                    df.loc[id, hasCompanyCol] = True
                    df.loc[id, companyScoreCol] \
                        += _recencyScore(None, None, now)

    # score skills
    filter = _valueFilter([LIProfileSkill.nrmName], [entitysets['skill']])
    if filter is not None:
        q = andb.query(LIProfileSkill.liprofileId,
                       LIProfileSkill.nrmName,
                       LIProfileSkill.rank) \
                .filter(LIProfileSkill.liprofileId.in_(ids),
                        LIProfileSkill.nrmName.in_(entitysets['skill']))
        for id, skill, score in q:
            for hasSkillCol, skillScoreCol, e \
                in zip(hasSkillCols, skillScoreCols, entities['skill']):
                if skill in e['matches']:
                    df.loc[id, hasSkillCol] = True
                    df.loc[id, skillScoreCol] += score

    # score experiences
    filter = _valueFilter([Experience.nrmTitle, Experience.nrmCompany],
                          [entitysets['title'], entitysets['company']])
    if filter is not None:
        q = andb.query(Experience.liprofileId,
                       Experience.start,
                       Experience.duration,
                       Experience.nrmTitle,
                       Experience.nrmCompany) \
                .filter(Experience.liprofileId.in_(ids), filter)
        for id, start, duration, title, company in q:
            start = datetime.combine(start, datetime.min.time())
            for hasTitleCol, titleScoreCol, e \
                in zip(hasTitleCols, titleScoreCols, entities['title']):
                if title in e['matches']:
                    df.loc[id, hasTitleCol] = True
                    df.loc[id, titleScoreCol] \
                        += _recencyScore(start, duration, now)
            for hasCompanyCol, companyScoreCol, e \
                in zip(hasCompanyCols, companyScoreCols, entities['company']):
                if company in e['matches']:
                    df.loc[id, hasCompanyCol] = True
                    df.loc[id, companyScoreCol] \
                        += _recencyScore(start, duration, now)

    # compute 'isMatch' and 'totalScore' columns
    for hasTitleCol in hasTitleCols:
        df['isMatch'] &= df[hasTitleCol]
    for hasCompanyCol in hasCompanyCols:
        df['isMatch'] &= df[hasCompanyCol]
    for titleScoreCol in titleScoreCols:
        df['totalScore'] += df[titleScoreCol]
    for companyScoreCol in companyScoreCols:
        df['totalScore'] += df[companyScoreCol]
    for hasSkillCol in hasSkillCols:
        df['isMatch'] &= df[hasSkillCol]
    for skillScoreCol in skillScoreCols:
        df['totalScore'] += df[skillScoreCol]

    # sort and return result
    df = df[df['isMatch']].sort_values('totalScore', ascending=False)
    return list(zip(df.index, df['totalScore']))


if __name__ == '__main__':
    _searchtypes = {'-s' : 'skill', '-t' : 'title', '-c' : 'company'}

    try:
        language = sys.argv[1]
        searchargs = sys.argv[2:]
        if not searchargs or len(searchargs) % 2 != 0:
            raise ValueError('Invalid query list.')
        searchterms = []
        for i in range(0, len(searchargs), 2):
            searchterms.append((_searchtypes[searchargs[i]], searchargs[i+1]))
    except (ValueError, IndexError, KeyError):
        sys.stdout.write('usage: python3 analytics_search.py '
                         '<language> (-s | -t | -c) <query1> '
                         '[(-s | -t | -c) <query2> ...]\n')
        sys.stdout.flush()
        exit(1)


    andb = AnalyticsDB(conf.ANALYTICS_DB)
    now = datetime.now()
    
    entities = findEntities(andb, language, searchterms)
    pprint(entities)
    
    candidates = findCandidates(andb, entities)
    print('\nResults: {0:d}'.format(len(candidates)))

    candidates = rankCandidates(andb, now, candidates, entities)
    for id, score in candidates:
        print(id, score)
