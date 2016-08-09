import conf
from careerdefinitiondb import *
from logger import Logger
from datetime import datetime
import requests
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--update', action='store_true',
                        help='Update existing salary data')
    args = parser.parse_args()    
    
    cddb = CareerDefinitionDB()
    logger = Logger()
    
    q = cddb.query(Career, Sector.name) \
            .join(Sector) \
            .order_by(Sector.name, Career.title)
    for career, sector_name in q:
        logger.log('{0:s} | {1:s}\n'.format(sector_name, career.title))
        # update salary histogram
        salary_q = cddb.query(SalaryBin) \
                       .filter(SalaryBin.career_id == career.id)
        if args.update:
            salary_q.delete()
        if args.update or salary_q.first() is None:
            r = requests.get(conf.ADZUNA_HISTOGRAM_API,
                             params={'app_id' : conf.ADZUNA_APP_ID,
                                     'app_key' : conf.ADZUNA_APP_KEY,
                                     'location0' : 'UK',
                                     'what' : career.title,
                                     'content-type' : 'application/json'}) \
                        .json()
            if 'histogram' in r:
                hist = [(float(key), val) \
                        for key, val in r['histogram'].items()]
                if hist:
                    hist.sort(key=lambda x: x[0])
                    lbs = [b[0] for b in hist]
                    ubs = lbs[1:] + [None]
                    counts = [val for k, val in hist]
                    for lb, ub, count in zip(lbs, ubs, counts):
                        salary_bin = SalaryBin(career_id=career.id,
                                               lower_bound=lb,
                                               upper_bound=ub,
                                               count=count)
                        cddb.add(salary_bin)
            cddb.commit()

        # update salary history
        salary_q = cddb.query(SalaryHistoryPoint) \
                       .filter(SalaryHistoryPoint.career_id == career.id)
        if args.update:
            salary_q.delete()
        if args.update or salary_q.first() is None:
            r = requests.get(conf.ADZUNA_HISTORY_API,
                             params={'app_id' : conf.ADZUNA_APP_ID,
                                     'app_key' : conf.ADZUNA_APP_KEY,
                                     'location0' : 'UK',
                                     'what' : career.title,
                                     'months' : '12',
                                     'content-type' : 'application/json'}) \
                        .json()
            if 'month' in r:
                chart = [(datetime.strptime(key, '%Y-%m'), val) \
                         for key, val in r['month'].items()]
                for date, salary in chart:
                    salary_history_point \
                        = SalaryHistoryPoint(career_id=career.id,
                                             date=date,
                                             salary=salary)
                    cddb.add(salary_history_point)
                cddb.commit()

