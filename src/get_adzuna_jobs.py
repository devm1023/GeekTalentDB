import requests
import argparse
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
    def __init__(self, loc, cat):
        self.api = '{0}{1:d}?app_id={2}&app_key={3}&results_per_page=50&location0=UK&location1={4}&category={5}'
        self.location = loc.replace(' ', '+')
        self.category = cat
        self.page  = 1
        self.total = 1
        self.step  = 50

    def __iter__(self):
        return self

    def getpage(self, p):
        return self.api.format(
            conf.ADZUNA_SEARCH_API,
            p,
            conf.ADZUNA_APP_ID,
            conf.ADZUNA_APP_KEY,
            self.location,
            self.category)

    def __next__(self):
        if self.page * self.step < self.total:
            self.page += 1
            return self.getpage(self.page)
        else:
            raise StopIteration()

def main(args):

    dtdb = DatoinDB()

    def extract_jobs(jobs):
        for job in jobs:
            dtdb.add_adzuna_job(job)

    api = _Api(args.location1, args.category)
    init_api = api.getpage(1)

    print('Querying Adzuna with: {0}\n'.format(init_api))

    try:
        r = requests.get(init_api)
        json = r.json()
        total = json['count']
        api.total = total
        jobs = json['results']
        for job in jobs:
            print('Job: {0}'.format(job))

        extract_jobs(jobs)

        for page in api:
            print(page)

        print('Total jobs: {0:d}\n'.format(total))

    except Exception as e:
        print('URL failed: {0}\n'.format(init_api))
        raise

    # with open('solr_skills.txt', 'w') as outputfile:
    #     for i, skill in enumerate(skills):
    #         if i % 2 == 1:
    #             continue
    #         outputfile.write('{0}\n'.format(skill))

    print('\nDone!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--location1', type=str, default='North East England',
                        help='Location to get the jobs from.')
    parser.add_argument('--category', type=str, default='it-jobs',
                        help='Adzuna category for jobs.')
    args = parser.parse_args()
    main(args)
