import conf
from datoindb import *
from datetime import datetime, timedelta
from histograms import Histogram1D, cumulated_histogram
import sys
import pickle
import argparse
from logger import Logger


logger = Logger()
timestamp0 = datetime(year=1970, month=1, day=1)
dtdb = DatoinDB(conf.DATOIN_DB)


def days(dt):
    return dt.total_seconds()/(24*60*60)


def to_timedelta(t, t0):
    return timestamp0 + timedelta(milliseconds=t) - t0


def make_linkedin_histograms(datebins, results):
    from_date = results['from_date']
    to_date = results['to_date']
    from_ts = (from_date - timestamp0).total_seconds()*1000
    to_ts   = (to_date - timestamp0).total_seconds()*1000
    
    index_hist = Histogram1D(xbins=datebins, init=0)
    newcrawl_hist = Histogram1D(xbins=datebins, init=0)
    recrawl_hist = Histogram1D(xbins=datebins, init=0)
    failedcrawl_hist = Histogram1D(xbins=datebins, init=0)
    delay_hist = Histogram1D(xbins=list(range(31)), init=0)
    
    nexp_hist = Histogram1D(xbins=datebins, init=0)
    nexp2_hist = Histogram1D(xbins=datebins, init=0)
    nedu_hist = Histogram1D(xbins=datebins, init=0)
    nedu2_hist = Histogram1D(xbins=datebins, init=0)
    ncat_hist = Histogram1D(xbins=datebins, init=0)
    ncat2_hist = Histogram1D(xbins=datebins, init=0)

    url_hist = Histogram1D(xbins=datebins, init=0)
    picture_url_hist = Histogram1D(xbins=datebins, init=0)
    name_hist = Histogram1D(xbins=datebins, init=0)
    first_name_hist = Histogram1D(xbins=datebins, init=0)
    last_name_hist = Histogram1D(xbins=datebins, init=0)
    city_hist = Histogram1D(xbins=datebins, init=0)
    country_hist = Histogram1D(xbins=datebins, init=0)
    title_hist = Histogram1D(xbins=datebins, init=0)
    description_hist = Histogram1D(xbins=datebins, init=0)
    

    indexed_before = dtdb.query(LIProfile.id) \
                         .filter(LIProfile.crawled_date != None,
                                 LIProfile.indexed_on < from_ts) \
                         .count()
    crawled_before = dtdb.query(LIProfile.id) \
                         .filter(LIProfile.crawled_date != None,
                                 LIProfile.crawled_date < from_ts) \
                         .count()

    q = dtdb.query(LIProfile.indexed_on) \
            .filter(LIProfile.crawled_date != None,
                    LIProfile.indexed_on >= from_ts,
                    LIProfile.indexed_on < to_ts)
    for indexed_on, in q:
        indexed_on = to_timedelta(indexed_on, from_date)
        index_hist[indexed_on] += 1

    q = dtdb.query(LIProfile) \
            .filter(LIProfile.crawled_date != None,
                    LIProfile.crawled_date >= from_ts,
                    LIProfile.crawled_date < to_ts)
    totalcount = q.count()
    logger.log('Scanning {0:d} profiles.\n'.format(totalcount))
    profilecount = 0
    for liprofile in q:
        indexed_on = to_timedelta(liprofile.indexed_on, from_date)
        crawled_on = to_timedelta(liprofile.crawled_date, from_date)
        delay      = days(indexed_on-crawled_on)
        nexp       = len(liprofile.experiences)
        nedu       = len(liprofile.educations)
        ncat       = len(liprofile.categories) if liprofile.categories else 0

        if liprofile.crawl_number == 0:
            newcrawl_hist[crawled_on] += 1
        elif liprofile.crawl_fail_count == 0:
            recrawl_hist[crawled_on] += 1
        else:
            failedcrawl_hist[crawled_on] += 1
        delay_hist[delay] += 1

        nexp_hist[crawled_on] += nexp
        nexp2_hist[crawled_on] += nexp**2
        nedu_hist[crawled_on] += nedu
        nedu2_hist[crawled_on] += nedu**2
        ncat_hist[crawled_on] += ncat
        ncat2_hist[crawled_on] += ncat**2

        if liprofile.profile_url is not None:
            url_hist[crawled_on] += 1
        if liprofile.profile_picture_url is not None:
            picture_url_hist[crawled_on] += 1
        if liprofile.name is not None:
            name_hist[crawled_on] += 1
        if liprofile.first_name is not None:
            first_name_hist[crawled_on] += 1
        if liprofile.last_name is not None:
            last_name_hist[crawled_on] += 1
        if liprofile.city is not None:
            city_hist[crawled_on] += 1
        if liprofile.country is not None:
            country_hist[crawled_on] += 1
        if liprofile.title is not None:
            title_hist[crawled_on] += 1
        if liprofile.description is not None:
            description_hist[crawled_on] += 1

        profilecount += 1
        if profilecount % 1000 == 0:
            logger.log('{0:d} of {1:d} profiles processed ({2:3.0f}%)\n' \
                       .format(profilecount, totalcount,
                               profilecount/totalcount*100))

    results['linkedin'] = {
        'crawled_before' : crawled_before,
        'indexed_before' : indexed_before,
        'indexed_on'     : index_hist,
        'newcrawl'       : newcrawl_hist,
        'recrawl'        : recrawl_hist,
        'failedcrawl'    : failedcrawl_hist,
        'delay'          : delay_hist,
        'nexp'           : nexp_hist,
        'nexp2'          : nexp2_hist,
        'nedu'           : nedu_hist,
        'nedu2'          : nedu2_hist,
        'ncat'           : ncat_hist,
        'ncat2'          : ncat2_hist,
        'url'            : url_hist,
        'picture_url'    : picture_url_hist,
        'name'           : name_hist,
        'first_name'     : first_name_hist,
        'last_name'      : last_name_hist,
        'city'           : city_hist,
        'country'        : country_hist,
        'title'          : title_hist,
        'description'    : description_hist,
        }


def main(args):
    day = timedelta(days=1)

    from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
    to_date = datetime.strptime(args.to_date, '%Y-%m-%d')
    filename = args.output_file
    
    results = {'from_date'  : from_date,
               'to_date'    : to_date}
    
    ndays = int(days(to_date-from_date))+1
    datebins = [timedelta(days=n) for n in range(ndays+1)]

    make_linkedin_histograms(datebins, results)

    with open(filename, 'wb') as pclfile:
        pickle.dump(results, pclfile)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('from_date',
                        help='Start of time window to analyze.')
    parser.add_argument('to_date',
                        help='End of time window to analyze.')
    parser.add_argument('output_file',
                        help='Name of the pickle file the results are written to.')
    args = parser.parse_args()
    main(args)


