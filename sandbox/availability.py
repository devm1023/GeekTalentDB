import sys
sys.path.append('../src') 

import conf
from analyticsdb import *
from histograms import Histogram1D, Histogram2D, densityHistogram, \
    cumulatedHistogram, GVar

import numpy as np
import matplotlib.pyplot as plt
import seaborn
from math import log, sqrt, exp


def employmentLists(q):
    idcol, companycol, startcol, endcol = tuple(q.statement.inner_columns)
    newq = q.filter(startcol != None,
                    endcol != None,
                    companycol != None) \
            .order_by(idcol, startcol)
    currentid = None
    currentcompany = None
    currentstart = None
    currentend = None
    employments = []

    def newcompany(company, start, end):
        nonlocal currentcompany, currentstart, currentend, employments
        if currentid and currentcompany:
            employments.append((currentcompany, currentstart, currentend))
        currentcompany = company
        currentstart = start
        currentend = end

    def newid(id):
        nonlocal currentid, employments
        currentid = id
        employments = []

    for id, company, start, end in newq:
        if id == currentid:
            if company != currentcompany:
                newcompany(company, start, end)
            else:
                currentend = max(end, currentend)
        else:
            newcompany(company, start, end)
            if currentid:
                yield currentid, employments
            newid(id)

    if currentid:
        newcompany(None, None, None)
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

def differenceInYears(t1, t2):
    return (t2-t1).days/365
    

andb = AnalyticsDB(conf.ANALYTICS_DB)
q = andb.query(LIExperience.liprofileId,
               LIExperience.nrmCompany,
               LIExperience.start,
               LIExperience.end)


durationHist = Histogram1D(xbins=np.arange(0.0, 10.1, 1.0), init=0)
nexperienceDurationHist = Histogram2D(xbins=np.arange(0.5, 7.0, 2.0),
                                      ybins=np.arange(0.0, 10.1, 1.0),
                                      init=0)
ageDurationHist = Histogram2D(xbins=np.arange(0.0, 10.1, 2.0),
                              ybins=np.arange(0.0, 10.1, 1.0),
                              init=0)
maxDurationHist = Histogram2D(xbins=np.arange(0.0, 10.1, 2.0),
                              ybins=np.arange(0.0, 10.1, 1.0),
                              init=0)

for id, employments in employmentLists(q):
    if not employments:
        continue
    firststart = employments[0][1]
    laststart = employments[-1][1]
    lastduration = (employments[-1][2] - employments[-1][1]).days/365
    age = (laststart - firststart).days/365
    nexperience = len(employments)

    durationHist.inc(lastduration)
    nexperienceDurationHist.inc(nexperience, lastduration)
    ageDurationHist.inc(age, lastduration)

    if len(employments) > 1:
        maxduration = max(differenceInYears(start, end) \
                          for company, start, end in employments[:-1])
        maxDurationHist.inc(maxduration, lastduration)


durationHist = retentionHistogram(durationHist)
durationHist.errorbar(drawstyle='steps')
plt.xlabel('employment duration [years]')
plt.ylabel('annual retention rate')
plt.ylim(0.0, 1.0)
plt.show()

slices = nexperienceDurationHist.xslices()
for nfrom, nto, (i, hist) in zip(nexperienceDurationHist.xbins[:-1],
                                 nexperienceDurationHist.xbins[1:],
                                 enumerate(slices)):
    hist = retentionHistogram(hist)
    label = '{0:d} to {1:d} experiences'.format(int(nfrom), int(nto))
    n = len(slices)
    offset = (i-n/2)*0.2
    hist.errorbar(drawstyle='steps', label=label, offset=offset)
plt.xlabel('employment duration [years]')
plt.ylabel('annual retention rate')
plt.ylim(0.0, 1.0)
plt.legend(loc='lower left')
plt.show()

slices = ageDurationHist.xslices()
for agefrom, ageto, (i, hist) in zip(ageDurationHist.xbins[:-1],
                                     ageDurationHist.xbins[1:],
                                     enumerate(slices)):
    hist = retentionHistogram(hist)
    label = 'age {0:d} to {1:d} years'.format(int(agefrom), int(ageto))
    n = len(slices)
    offset = (i-n/2)*0.2
    hist.errorbar(drawstyle='steps', label=label, offset=offset)
plt.xlabel('employment duration [years]')
plt.ylabel('annual retention rate')
plt.ylim(0.0, 1.0)
plt.legend(loc='lower left')
plt.show()

slices = maxDurationHist.xslices()
for agefrom, ageto, (i, hist) in zip(maxDurationHist.xbins[:-1],
                                     maxDurationHist.xbins[1:],
                                     enumerate(slices)):
    hist = retentionHistogram(hist)
    label = 'max {0:d} to {1:d} years'.format(int(agefrom), int(ageto))
    n = len(slices)
    offset = (i-n/2)*0.2
    hist.errorbar(drawstyle='steps', label=label, offset=offset)
plt.xlabel('employment duration [years]')
plt.ylabel('annual retention rate')
plt.ylim(0.0, 1.0)
plt.legend(loc='lower left')
plt.show()
