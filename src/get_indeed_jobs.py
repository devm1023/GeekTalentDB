import requests
import argparse
import csv
import sys
import traceback
import conf
import urllib.parse as url
from datoindb import *


"""
   Script extracts job postings from Indeed using their API and populates Datoin DB on the fly.
"""
# http://api.indeed.com/ads/apisearch?publisher=4481506106001115&sort=date&radius&st&jt&start
# &limit=25&fromage&filter=1&latlong=1&co=UK&chnl&userip=1.2.3.4&useragent=Mozilla//4.0(Firefox)
# &v=2&format=json&latlong=1&q=developer


class _Api:
    """
    Returns a formatted api request string.
    """

    LIMIT = 1025  # Indeed limits pagination to 1024 objects.

    def __init__(self, country, title, l, max_age):
        self.api = '{0}?publisher={1:d}&sort=date&radius=15&st&jt&start={2:d}&limit={3:d}' \
                   '&filter=1&latlong=1&co={4}&chnl&userip=1.2.3.4&useragent=Mozilla//4.0%28Firefox%29' \
                   '&v=2&format=json&latlong=1&q={5}&l={6}&fromage={7}'

        self.country = country
        self.title = title
        self.location = l
        self.max_age = max_age
        if max_age == 0:
            self.max_age = ''
        self.start = 1
        self.total = 1
        self.step = 25

    def __iter__(self):
        return self

    def getpage(self, start):
        return self.api.format(
            conf.INDEED_SEARCH_API,
            conf.INDEED_PUB_ID,
            start,
            self.step,
            self.country,
            self.title,
            self.location,
            self.max_age)

    def __next__(self):
        if self.start + self.step < min(self.total, _Api.LIMIT):
            self.start += self.step
            return self.getpage(self.start)
        else:
            raise StopIteration()


def main(args):

    dtdb = DatoinDB()

    def extract_jobs(jobs, crawl_url):
        for job in jobs:
            dtdb.add_indeed_job(job, args.category, crawl_url)

    dtdb.flush()
    dtdb.commit()

    titles = []
    with open(args.titles_from, 'r') as infile:
        for title in infile:
            titles.append(url.quote_plus(title.rstrip()))
            if len(titles) % 100 == 0 and not args.quiet:
                print('Reading titles... {0}'.format(len(titles)))

    if not args.quiet:
        print('Titles read: {0}'.format(len(titles)))

    locations = []
    with open(args.locations_from, 'r') as infile:
        csvreader = csv.reader(infile)
        for row in csvreader:
            if len(row) < 2 or row[0] != args.country:
                continue

            locations.append(url.quote_plus(row[1]))
            if len(locations) % 10 == 0 and not args.quiet:
                print('Reading locations... {0}'.format(len(locations)))

    if not args.quiet:
        print('Locations read: {0}'.format(len(titles)))

    session = requests.Session()

    for title in titles:

        if args.from_title is not None and title != url.quote_plus(args.from_title):
            continue

        #Reached first title, reset
        args.from_title = None

        for location in locations:

            api = _Api(args.country, title, location, args.max_age)

            init_api = api.getpage(1)

            if not args.quiet:
                print('Querying Indeed with: {0}'.format(init_api))

            try:
                r = session.get(init_api)
                json = r.json()
                total = json['totalResults']
                api.total = total
                jobs = json['results']

                # Those combinations would have to be made more granular.
                if total > _Api.LIMIT:
                    print('Location/Title combination exceeds API limits: {}, {}, {}/{}'
                          .format(title, location, total, _Api.LIMIT), file=sys.stderr)

                extract_jobs(jobs, init_api)

                if not args.quiet:
                    print('Total jobs to get: {0:d}'.format(total))

                for page in api:
                    try:
                        if not args.quiet:
                            print('Requesting: {0:s}'.format(page))
                        r = session.get(page)
                        json = r.json()
                        jobs = json['results']
                        extract_jobs(jobs, page)
                    except Exception as e:
                        print('URL failed: {0}\n'.format(page), file=sys.stderr)
                        print(traceback.format_exc(), file=sys.stderr)

                if not args.quiet:
                    print('Jobs found: {0:d}'.format(total))

            except Exception as e:
                print('Initial URL failed: {0} {1}'.format(init_api, e), file=sys.stderr)

    if not args.quiet:
        print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--titles-from', type=str,
                        help='CSV files with titles to be searched for.', default=None, required=True)
    parser.add_argument('--locations-from', type=str,
                        help='CSV files with locations to be searched in.', default=None)
    parser.add_argument('--category', type=str,
                        help='Category for jobs. e.g. it-jobs', required=True)
    parser.add_argument('--country', type=str, default='gb',
                        help='ISO 3166-1 country code')
    parser.add_argument('--from-title', type=str,
                        help='First title to search for.', default=None)
    parser.add_argument('--quiet', action='store_true',
                        help='Only print errors')
    parser.add_argument('--max-age', type=str,
                        help='Maximum number of days ago to crawl.', default=0)
    args = parser.parse_args()

    main(args)
