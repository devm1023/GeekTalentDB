import sys
sys.path.append('../src') 

import conf
from analyticsdb import *
from histograms import Histogram1D, Histogram2D, densityHistogram, \
    cumulatedHistogram, GVar

import numpy as np
import matplotlib.pyplot as plt
import seaborn
from math import log, sqrt


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

def decayRateHistogram(hist):
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
            lam = 2*log(f2/f1)/(t1-t2)
            dlam = abs(2/(t1-t2))*sqrt(df2sq/f2**2 + df1sq/f1**2)
            data.append(GVar(lam, dlam))
    return Histogram1D(xbins=hist.xvals, data=data)
    
    

andb = AnalyticsDB(conf.ANALYTICS_DB)
q = andb.query(LIExperience.liprofileId,
               LIExperience.nrmCompany,
               LIExperience.start,
               LIExperience.end)


durationHist = Histogram1D(xbins=np.arange(0.0, 10.1, 0.5), init=0)
nexperienceDurationHist = Histogram2D(xbins=np.arange(0.5, 11.0, 2.0),
                                      ybins=np.arange(0.0, 10.1, 1.0),
                                      init=0)

for id, employments in employmentLists(q):
    if not employments:
        continue
    firststart = employments[0][1]
    laststart = employments[-1][1]
    lastduration = (employments[-1][2] - employments[-1][1]).days/365
    nexperience = len(employments)

    durationHist.inc(lastduration)
    nexperienceDurationHist.inc(nexperience, lastduration)


durationHist = decayRateHistogram(durationHist)
durationHist.errorbar(drawstyle='steps')
plt.show()

slices = nexperienceDurationHist.xslices()
for nexperience, hist in zip(nexperienceDurationHist.xvals, slices):
    hist = decayRateHistogram(hist)
    hist.errorbar(drawstyle='steps', label=str(nexperience))
plt.legend()
plt.show()
