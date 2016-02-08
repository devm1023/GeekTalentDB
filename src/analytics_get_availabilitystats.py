import conf
from analyticsdb import *
from histograms import Histogram1D, Histogram2D, densityHistogram, \
    cumulatedHistogram, GVar

from math import log, sqrt, exp
import numpy as np
import pickle
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

def retentionHistogram(hist):
    n = int(hist.sum())
    hist = hist/n
    hist = cumulatedHistogram(hist, upper=True)
    data = []
    for t1, t2, f1, f2 in zip(hist.xbins[:-2], hist.xbins[2:], \
                              hist.data[:-1], hist.data[1:]):
        if f1 <= 0 or f2 <= 0:
            data.append(None)
        else:
            df2sq = f2*(1-f2)/n
            df1sq = f1*(1-f1)/n
            dt = (t2-t1)/2
            r = (f2/f1)**(1/dt)
            dr = r/dt*sqrt(df2sq/f2**2 + df1sq/f1**2)
            data.append(GVar(r, dr))
    return Histogram1D(xbins=hist.xvals, data=data)

def differenceInYears(start, end):
    return (end-start).days/365
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('titlethreshold',
                        help='The minimal popularity for job titles.',
                        type=int)
    parser.add_argument('outputfile',
                        help='The name of the output file')
    args = parser.parse_args()

    andb = AnalyticsDB(conf.ANALYTICS_DB)

    # get titles
    q = andb.query(Title.nrmName, Title.name) \
            .filter(Title.experienceCount >= args.titlethreshold)
    titlenames = dict(q)

    q = andb.query(LIExperience.liprofileId,
                   LIExperience.nrmTitle,
                   LIExperience.nrmCompany,
                   LIExperience.start,
                   LIExperience.end)

    durationbins = np.arange(0.0, 10.1, 1.0)

    hist = {}
    hist['duration'] = Histogram1D(xbins=durationbins, init=0)
    hist['nexp']     = Histogram2D(xbins=np.arange(0.5, 7.0, 2.0),
                                   ybins=durationbins,
                                   init=0)
    hist['age']      = Histogram2D(xbins=np.arange(0.0, 10.1, 2.0),
                                   ybins=durationbins,
                                   init=0)
    hist['maxdur']   = Histogram2D(xbins=np.arange(0.0, 10.1, 2.0),
                                   ybins=durationbins,
                                   init=0)
    hist['prevdur']  = Histogram2D(xbins=np.arange(0.0, 10.1, 2.0),
                                   ybins=durationbins,
                                   init=0)
    hist['title'] = dict((title, Histogram1D(xbins=durationbins, init=0)) \
                         for title in titlenames)

    for id, employments in employmentLists(q):
        if not employments:
            continue
        firststart = employments[0]['start']
        laststart = employments[-1]['start']
        lastduration = differenceInYears(employments[-1]['start'],
                                         employments[-1]['end'])
        lasttitle = employments[-1]['title']
        age = differenceInYears(firststart, laststart)
        nexperience = len(employments)

        hist['duration'].inc(lastduration)
        hist['nexp'].inc(nexperience, lastduration)
        hist['age'].inc(age, lastduration)

        if len(employments) > 1:
            maxduration = max(differenceInYears(e['start'], e['end']) \
                              for e in employments[:-1])
            prevduration = differenceInYears(employments[-2]['start'],
                                             employments[-2]['end'])

            hist['maxdur'].inc(maxduration, lastduration)
            hist['prevdur'].inc(prevduration, lastduration)

        if lasttitle in hist['title']:
            hist['title'][lasttitle].inc(lastduration)

    with open(args.outputfile, 'wb') as pclfile:
        pickle.dump(hist, pclfile)
