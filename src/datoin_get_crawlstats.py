import conf
from datoindb import *
from datetime import datetime, timedelta
from histograms import Histogram1D, cumulated_histogram
import sys
import pickle
import argparse


def days(dt):
    return dt.total_seconds()/(24*60*60)

parser = argparse.ArgumentParser()
parser.add_argument('from_date',
                    help='Start of time window to analyze.')
parser.add_argument('to_date',
                    help='End of time window to analyze.')
parser.add_argument('output_file',
                    help='Name of the pickle file the results are written to.')
args = parser.parse_args()

t0 = datetime(year=1970, month=1, day=1)
day = timedelta(days=1)

from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
to_date = datetime.strptime(args.to_date, '%Y-%m-%d')
filename = args.output_file

dtdb = DatoinDB(conf.DATOIN_DB)

ndays = int(days(to_date-from_date))+1
datebins = [timedelta(days=n) for n in range(ndays+1)]
index_hist = Histogram1D(xbins=datebins)
index_hist.set(0)
crawl_hist = Histogram1D(xbins=datebins)
crawl_hist.set(0)
delay_hist = Histogram1D(xbins=list(range(31)))
delay_hist.set(0)

from_ts = (from_date - t0).total_seconds()*1000
to_ts   = (to_date - t0).total_seconds()*1000

indexed0 = dtdb.query(LIProfile.id) \
               .filter(LIProfile.crawled_date != None,
                       LIProfile.indexed_on < from_ts) \
               .count()
crawled0 = dtdb.query(LIProfile.id) \
               .filter(LIProfile.crawled_date != None,
                       LIProfile.crawled_date < from_ts) \
               .count()

q = dtdb.query(LIProfile.indexed_on, LIProfile.crawled_date) \
        .filter(LIProfile.crawled_date != None,
                LIProfile.crawled_date >= from_ts,
                LIProfile.crawled_date < to_ts)
for indexed_on, crawled_on in q:
    indexed_on = t0 + timedelta(milliseconds=indexed_on) - from_date
    crawled_on = t0 + timedelta(milliseconds=crawled_on) - from_date
    delay     = days(indexed_on-crawled_on)

    crawl_hist[crawled_on] += 1
    delay_hist[delay]     += 1

q = dtdb.query(LIProfile.indexed_on) \
        .filter(LIProfile.crawled_date != None,
                LIProfile.indexed_on >= from_ts,
                LIProfile.indexed_on < to_ts)
for indexed_on, in q:
    indexed_on = t0 + timedelta(milliseconds=indexed_on) - from_date
    index_hist[indexed_on] += 1

index_hist = cumulated_histogram(index_hist, const=indexed0)
crawl_hist = cumulated_histogram(crawl_hist, const=crawled0)


obj = {'from_date'  : from_date,
       'to_date'    : to_date,
       'index_hist' : index_hist,
       'crawl_hist' : crawl_hist,
       'delay_hist' : delay_hist}

with open(filename, 'wb') as pclfile:
    pickle.dump(obj, pclfile)

