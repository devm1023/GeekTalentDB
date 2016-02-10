import conf
from analyticsdb import *
from histograms import Histogram1D, Histogram2D, densityHistogram, \
    cumulatedHistogram, GVar, HistoMatrix

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
    if n <= 0:
        return None
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

def retentionHistogramList(hist, labelfmt='{0:d} to {1:d}'):
    slices = hist.xslices()
    result = []
    for nfrom, nto, h in zip(hist.xbins[:-1],
                             hist.xbins[1:],
                             slices):
        h = retentionHistogram(h)
        label = labelfmt.format(int(nfrom), int(nto))
        result.append((label, h))

    return result

def differenceInYears(start, end):
    return (end-start).days/365
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', choices=['linkedin', 'indeed'],
                        help='The data source to use.',
                        default='linkedin')
    parser.add_argument('--threshold', type=int, default=10000,
                        help='The minimal popularity for job titles.')
    parser.add_argument('outputfile',
                        help='The name of the output file')
    args = parser.parse_args()

    andb = AnalyticsDB(conf.ANALYTICS_DB)

    # get titles
    q = andb.query(Title.nrmName, Title.name) \
            .filter(Title.experienceCount >= args.threshold)
    titlenames = dict(q)

    if args.source == 'linkedin':
        q = andb.query(LIExperience.liprofileId,
                       LIExperience.nrmTitle,
                       LIExperience.nrmCompany,
                       LIExperience.start,
                       LIExperience.end)
    elif args.source == 'indeed':
        q = andb.query(INExperience.inprofileId,
                       INExperience.nrmTitle,
                       INExperience.nrmCompany,
                       INExperience.start,
                       INExperience.end)
    else:
        raise ValueError('Invalid source type')
    

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
    hist['matrix'] = HistoMatrix('duration', durationbins,
                                 'nexp',     np.arange(0.5, 7.0, 2.0),
                                 'age',      np.arange(0.0, 10.1, 2.0),
                                 'maxdur',   np.arange(0.0, 10.1, 2.0),
                                 'prevdur',  np.arange(0.0, 10.1, 2.0))

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
            hist['matrix'].inc({'duration' : lastduration,
                                'nexp'     : nexperience,
                                'age'      : age,
                                'maxdur'   : maxduration,
                                'prevdur'  : prevduration})

        if lasttitle in hist['title']:
            hist['title'][lasttitle].inc(lastduration)

    hist['duration'] = retentionHistogram(hist['duration'])
    hist['nexp']     = retentionHistogramList(hist['nexp'])
    hist['age']      = retentionHistogramList(hist['age'],
                                              labelfmt='{0:d} to {1:d} years')
    hist['maxdur']   = retentionHistogramList(hist['maxdur'],
                                              labelfmt='{0:d} to {1:d} years')
    hist['prevdur']  = retentionHistogramList(hist['prevdur'],
                                              labelfmt='{0:d} to {1:d} years')

    titlehists = []
    for title, h in hist['title'].items():
        if h.sum() < args.threshold:
            continue
        h = retentionHistogram(h)
        if h is not None:
            titlehists.append((titlenames[title], h))
    hist['title'] = titlehists
            
    with open(args.outputfile, 'wb') as pclfile:
        pickle.dump(hist, pclfile)
