import sys
sys.path.append('../src') 

import conf
from analyticsdb import *
from sqlalchemy import func
import numpy as np
import csv


def employmentLists(q):
    idcol, titlecol, companycol, startcol, endcol \
        = tuple(q.statement.inner_columns)
    newq = q.filter(startcol != None,
                    endcol != None,
                    companycol != None,
                    titlecol != None) \
            .order_by(idcol, startcol)
    currentid = None
    currenttitle = None
    currentcompany = None
    currentstart = None
    currentend = None
    employments = []

    def newcompany(title, company, start, end):
        nonlocal currenttitle, currentcompany, currentstart, currentend, \
            employments
        if currentid and currentcompany:
            employments.append({'title'   : currenttitle,
                                'company' : currentcompany, \
                                'start'   : currentstart,
                                'end'     : currentend})
        currenttitle = title
        currentcompany = company
        currentstart = start
        currentend = end

    def newid(id):
        nonlocal currentid, employments
        currentid = id
        employments = []

    for id, title, company, start, end in newq:
        if id == currentid:
            if company != currentcompany:
                newcompany(title, company, start, end)
            else:
                currentend = max(end, currentend)
        else:
            newcompany(title, company, start, end)
            if currentid:
                yield currentid, employments
            newid(id)

    if currentid:
        newcompany(None, None, None, None)
        yield currentid, employments

def differenceInYears(t1, t2):
    return (t2-t1).days/365
    

andb = AnalyticsDB(conf.ANALYTICS_DB)
q = andb.query(LIProfile.id).order_by(func.random()).limit(5000)
ids = [id for id, in q]
q = andb.query(LIExperience.liprofileId,
               LIExperience.nrmTitle,
               LIExperience.nrmCompany,
               LIExperience.start,
               LIExperience.end) \
        .filter(LIExperience.liprofileId.in_(ids))

with open('availabilitysample.csv', 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for id, employments in employmentLists(q):
            if len(employments) <= 1:
                continue
            firststart = employments[0]['start']
            laststart = employments[-1]['start']
            lasttitle = employments[-1]['title']

            lastduration = int(differenceInYears(employments[-1]['start'],
                                                 employments[-1]['end']))
            age = int(differenceInYears(firststart, laststart))
            nexperience = len(employments)
            maxduration = int(max(differenceInYears(e['start'], e['end']) \
                                  for e in employments[:-1]))
            prevduration = int(differenceInYears(employments[-2]['start'],
                                                 employments[-2]['end']))

            csvwriter.writerow([lastduration,
                                age,
                                nexperience,
                                maxduration,
                                prevduration])

