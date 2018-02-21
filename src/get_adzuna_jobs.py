import requests
import argparse
from datetime import datetime
import sys
import traceback

import conf
from datoindb import *


"""
   Script extracts job postings from Adzuna using their API and populates Datoin DB on the fly.
"""
# http://api.adzuna.com:80/v1/api/jobs/gb/search/1?app_id=5f619739&app_key=da114b2c3ded37d8cc280d8841e5f7f6
# &results_per_page=50&location0=UK&location1=North%20East%20England&category=it-jobs

class _Api():
    """
    Returns a formatted api request string.
    """
    def __init__(self, country, loc1, loc2, cat):
        if loc2 is not None:
            self.api = '{0}{1:d}?app_id={2}&app_key={3}&results_per_page=50&category={4}' \
                       '&location0=UK&location1={5}&location2={6}&sort_by=date&sort_direction=down'
            self.location2 = loc2.replace(' ', '+')
            self.location1 = loc1.replace(' ', '+')
        elif loc1 is not None:
            self.api = '{0}{1:d}?app_id={2}&app_key={3}&results_per_page=50&category={4}' \
                       '&location0=UK&location1={5}&sort_by=date&sort_direction=down'
            self.location2 = None
            self.location1 = loc1.replace(' ', '+')
        else:
            self.api = '{0}{1:d}?app_id={2}&app_key={3}&results_per_page=50&category={4}' \
                       '&sort_by=date&sort_direction=down'
            self.location1 = None
            self.location2 = None

        self.country = country
        self.category = cat
        self.page  = 1
        self.total = 1
        self.step  = 50

    def __iter__(self):
        return self

    def getpage(self, p):
        return self.api.format(
            conf.ADZUNA_SEARCH_API.format(self.country),
            p,
            conf.ADZUNA_APP_ID,
            conf.ADZUNA_APP_KEY,
            self.category,
            self.location1,
            self.location2)

    def __next__(self):
        if self.page * self.step < self.total:
            self.page += 1
            return self.getpage(self.page)
        else:
            raise StopIteration()

def main(args):
    start = datetime.now()

    dtdb = DatoinDB(autoflush=False)

    def extract_jobs(jobs):
        for job in jobs:
            dtdb.add_adzuna_job(job)

        dtdb.flush()
        dtdb.commit()

    api = _Api(args.country, args.location1, args.location2, args.category)
    init_api = api.getpage(1)

    session = requests.Session()

    if not args.quiet:
        print('Querying Adzuna with: {0}\n'.format(init_api))

    try:
        r = session.get(init_api)
        json = r.json()
        total = json['count']
        api.total = total
        jobs = json['results']
        extract_jobs(jobs)

        if not args.quiet:
            print('Total jobs to get: {0:d}\n'.format(total))

        for page in api:
            try:
                if not args.quiet:
                    print('Requesting: {0}'.format(page))
                r = session.get(page)
                json = r.json()
                jobs = json['results']
                extract_jobs(jobs)
            except Exception as e:
                print('URL failed: {0}\n'.format(page), file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)

        if not args.quiet:
            print('Jobs found: {0:d}\n'.format(total))

    except Exception as e:
        print('Initial URL failed: {0}\n'.format(init_api), file=sys.stderr)

    # with open('digital_tech_solr_skills.txt', 'w') as outputfile:
    #     for i, skill in enumerate(skills):
    #         if i % 2 == 1:
    #             continue
    #         outputfile.write('{0}\n'.format(skill))

    end = datetime.now()
    if not args.quiet:
        print('\nDone (in {}s)!'.format((end - start).seconds))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--location1', type=str,
                        help='Location 1 to get the jobs from.', default=None)
    parser.add_argument('--location2', type=str,
                        help='Location 2 to get the jobs from.', default=None)
    parser.add_argument('--category', type=str, required=True,
                        help='Adzuna category for jobs.')
    parser.add_argument('--country', type=str, default='gb',
                        help='ISO 3166-1 country code')
    parser.add_argument('--quiet', action='store_true',
                        help='Only print errors')
    args = parser.parse_args()

    if args.location2 and not args.location1:
        raise Exception("Location1 must be provided is location2 is supplied!")

    main(args)
