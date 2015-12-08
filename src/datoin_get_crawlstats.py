import conf
from datoindb import *
from datetime import datetime, timedelta
from histograms import Histogram1D, cumulatedHistogram
import sys
import pickle

def days(dt):
    return dt.total_seconds()/(24*60*60)

t0 = datetime(year=1970, month=1, day=1)
day = timedelta(days=1)

fromDate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
toDate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
filename = sys.argv[3]

dtdb = DatoinDB(conf.DATOIN_DB)

ndays = int(days(toDate-fromDate))+1
datebins = [timedelta(days=n) for n in range(ndays+1)]
indexHist = Histogram1D(xbins=datebins)
indexHist.set(0)
crawlHist = Histogram1D(xbins=datebins)
crawlHist.set(0)
delayHist = Histogram1D(xbins=list(range(31)))
delayHist.set(0)

fromTs = (fromDate - t0).total_seconds()*1000
toTs   = (toDate - t0).total_seconds()*1000

indexed0 = dtdb.query(LIProfile.id) \
               .filter(LIProfile.indexedOn < fromTs) \
               .count()
crawled0 = dtdb.query(LIProfile.id) \
               .filter(LIProfile.crawledDate < fromTs) \
               .count()

q = dtdb.query(LIProfile.indexedOn, LIProfile.crawledDate) \
        .filter(LIProfile.crawledDate >= fromTs,
                LIProfile.crawledDate < toTs)
for indexedOn, crawledOn in q:
    indexedOn = t0 + timedelta(milliseconds=indexedOn) - fromDate
    crawledOn = t0 + timedelta(milliseconds=crawledOn) - fromDate
    delay     = days(indexedOn-crawledOn)

    crawlHist[crawledOn] += 1
    delayHist[delay]     += 1

q = dtdb.query(LIProfile.indexedOn) \
        .filter(LIProfile.indexedOn >= fromTs,
                LIProfile.indexedOn < toTs)
for indexedOn, in q:
    indexedOn = t0 + timedelta(milliseconds=indexedOn) - fromDate
    indexHist[indexedOn] += 1

indexHist = cumulatedHistogram(indexHist, const=indexed0)
crawlHist = cumulatedHistogram(crawlHist, const=crawled0)

    
obj = {'fromDate'  : fromDate,
       'toDate'    : toDate,
       'indexHist' : indexHist,
       'crawlHist' : crawlHist,
       'delayHist' : delayHist}

with open(filename, 'wb') as pclfile:
    pickle.dump(obj, pclfile)

