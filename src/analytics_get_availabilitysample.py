import conf
from analyticsdb import *
from sqlalchemy import func
import numpy as np
import csv
import argparse

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
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', choices=['linkedin', 'indeed'],
                        help='The data source to use.',
                        default='linkedin')
    parser.add_argument('--threshold', type=int, default=10000,
                        help='The minimal popularity for job titles.')
    parser.add_argument('nsamples', type=int,
                        help='The number of samples to extract.')
    parser.add_argument('outputfile',
                        help='The name of the output file')
    args = parser.parse_args()

    andb = AnalyticsDB(conf.ANALYTICS_DB)
    idquery = andb.query(LIProfile.id) \
                  .order_by(func.random()) \
                  .limit(args.nsamples)
    q = andb.query(LIExperience.liprofileId,
                   LIExperience.nrmTitle,
                   LIExperience.nrmCompany,
                   LIExperience.start,
                   LIExperience.end) \
            .filter(LIExperience.liprofileId.in_(idquery))

    with open(args.outputfile, 'w') as csvfile:
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

