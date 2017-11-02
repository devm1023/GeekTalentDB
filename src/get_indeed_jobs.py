import requests
import argparse
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

    def __init__(self, title, l):
        self.api = '{0}?publisher={1:d}&sort=date&radius=50&st&jt&start={2:d}&limit={3:d}&fromage' \
                   '&filter=1&latlong=1&co=UK&chnl&userip=1.2.3.4&useragent=Mozilla//4.0%28Firefox%29' \
                   '&v=2&format=json&latlong=1&q={4}&l={5}'
        self.title = title
        self.location = l
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
            self.title,
            self.location)

    def __next__(self):
        if self.start + self.step < self.total:
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
            if len(titles) % 100 == 0:
                print('Reading titles... {0}'.format(len(titles)))

    print('Titles read: {0}'.format(len(titles)))

    locations = []
    with open(args.locations_from, 'r') as infile:
        for location in infile:
            locations.append(url.quote_plus(location.rstrip()))
            if len(locations) % 10 == 0:
                print('Reading locations... {0}'.format(len(locations)))

    print('Locations read: {0}'.format(len(titles)))

    for title in titles:

        for location in locations:

            api = _Api(title, location)

            init_api = api.getpage(1)

            print('Querying Indeed with: {0}'.format(init_api))

            try:
                r = requests.get(init_api)
                json = r.json()
                total = json['totalResults']
                api.total = total
                jobs = json['results']

                # Those combinations would have to be made more granular.
                if total > _Api.LIMIT:
                    print('Location/Title combination exceeds API limits: {0}'.format(init_api))

                extract_jobs(jobs, init_api)

                print('Total jobs to get: {0:d}'.format(total))

                for page in api:
                    print('Requesting: {0:s}'.format(page))
                    r = requests.get(page)
                    json = r.json()
                    jobs = json['results']
                    extract_jobs(jobs, page)

                print('Jobs found: {0:d}'.format(total))

            except Exception as e:
                print('URL failed: {0} {1}'.format(init_api, e))
                raise

    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--titles-from', type=str,
                        help='CSV files with titles to be searched for.', default=None, required=True)
    parser.add_argument('--locations-from', type=str,
                        help='CSV files with locations to be searched in.', default=None)
    parser.add_argument('--category', type=str,
                        help='Category for jobs. e.g. it-jobs', required=True)
    args = parser.parse_args()

    main(args)
