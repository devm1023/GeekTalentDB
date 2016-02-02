import sys
sys.path.append('../src') 

import conf
from analyticsdb import *
from histograms import Histogram1D, Histogram2D

import numpy as np
import matplotlib.pyplot as plt
import seaborn


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
    
    

andb = AnalyticsDB(conf.ANALYTICS_DB)
q = andb.query(LIExperience.liprofileId,
               LIExperience.nrmCompany,
               LIExperience.start,
               LIExperience.end)


nexperienceHist = Histogram1D(xbins=np.arange(0.5, 11.0, 1.0), init=0)
durationHist = Histogram1D(xbins=np.arange(0.0, 10.1, 0.5), init=0)
ageHist = Histogram1D(xbins=np.arange(0.0, 10.1, 0.5), init=0)
ageDurationHist = Histogram2D(xbins=np.arange(0.0, 10.1, 0.2),
                              ybins=np.arange(0.0, 10.1, 0.2),
                              init=0)

for id, employments in employmentLists(q):
    # if len(employments) < 2:
    #     continue
    firststart = employments[0][1]
    # laststart = employments[-1][1]
    
    for company, start, end in employments:
        duration = (end - start).days/365
        age = (start - firststart).days/365
        nexperienceHist.inc(len(employments))
        durationHist.inc(duration)
        ageHist.inc(age)
        ageDurationHist.inc(age, duration)

nexperienceHist.plot(drawstyle='steps')
plt.show()

durationHist.plot(drawstyle='steps')
plt.show()

ageHist.plot(drawstyle='steps')
plt.show()

p = ageDurationHist.pcolormesh(cmap='YlOrRd')
plt.colorbar(p)
plt.show()

