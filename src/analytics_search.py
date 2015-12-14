import conf
from analyticsdb import *
from sqlalchemy import case, or_, func
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import literal_column, union_all
import sys
from pprint import pprint

_categories = ['title', 'skill', 'company']


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
    
    if entitysets['title'] and entitysets['company']:
        liprofilefilter = or_(LIProfile.nrmTitle.in_(entitysets['title']),
                              LIProfile.nrmCompany.in_(entitysets['company']))
        experiencefilter = or_(Experience.nrmTitle.in_(entitysets['title']),
                               Experience.nrmCompany.in_(entitysets['company']))
    elif entitysets['title']:
        liprofilefilter = LIProfile.nrmTitle.in_(entitysets['title'])
        experiencefilter = Experience.nrmTitle.in_(entitysets['title'])
    elif entitysets['company']:
        liprofilefilter = LIProfile.nrmCompany.in_(entitysets['company'])
        experiencefilter = Experience.nrmCompany.in_(entitysets['company'])
    else:
        liprofilefilter = None
        experiencefilter = None

    if entitysets['skill']:
        skillfilter = LIProfileSkill.nrmName.in_(entitysets['skill'])
    else:
        skillfilter = None

    liprofilequery = andb.query(LIProfile.id.label('s_id'))
    if liprofilefilter is not None:
        liprofilequery.filter(liprofilefilter)
    experiencequery = andb.query(Experience.liprofileId.label('s_id'))
    if experiencefilter is not None:
        experiencequery = experiencequery.filter(experiencefilter)
    skillquery = andb.query(LIProfileSkill.liprofileId.label('s_id'))
    if skillfilter is not None:
        skillquery = skillquery.filter(skillfilter)

    subq = aliased(union_all(liprofilequery, experiencequery, skillquery))
    countcol = func.count().label('countcol')
    q = andb.query(subq.c.s_id) \
            .group_by(subq.c.s_id) \
            .having(countcol >= nentities)

    candidates = [c for c, in q]
    return candidates
    

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
    
    entities = findEntities(andb, language, searchterms)
    pprint(entities)
    
    candidates = findCandidates(andb, entities)
    print('\nResults: {0:d}'.format(len(candidates)))
