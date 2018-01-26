import math

from canonicaldb import *
import sys
from logger import Logger
import argparse
from sqlalchemy import func


def main(args):
    logger = Logger(sys.stdout)
    cndb = CanonicalDB()

    doc_count = cndb.query(ADZJob).filter(ADZJob.nuts0 == 'UK' if args.uk_only else None).count() \
        + cndb.query(INJob).filter(INJob.nuts0 == 'UK' if args.uk_only else None).count()

    logger.log('Total jobs found {0:d}\n'.format(doc_count))
    countcol = func.count().label('counts')

    q = cndb.query(ADZJobSkill.name, countcol).group_by(ADZJobSkill.name)

    if args.uk_only:
        q = q.join(ADZJob, ADZJob.id == ADZJobSkill.adzjob_id) \
            .filter(ADZJob.nuts0 == 'UK')

    adz_doc_freq = dict(q)

    q = cndb.query(INJobSkill.name, countcol).group_by(INJobSkill.name)

    if args.uk_only:
        q = q.join(INJob, INJob.id == INJobSkill.adzjob_id) \
            .filter(INJob.nuts0 == 'UK')

    ind_doc_freq = dict(q)

    doc_freq = {k: adz_doc_freq.get(k, 0) + ind_doc_freq.get(k, 0) for k in set(adz_doc_freq) | set(ind_doc_freq)}

    logger.log('adz_doc_freq size {0:d}\n'.format(len(adz_doc_freq)))
    logger.log('ind_doc_freq size {0:d}\n'.format(len(ind_doc_freq)))
    logger.log('doc_freq size {0:d}\n'.format(len(doc_freq)))

    logger.log('Calculating idf values...\n')
    for skill_name, df in doc_freq.items():
        idf = {'name': skill_name, 'idf': math.log10(doc_count / df)}
        logger.log('{0:s}, {1:f}\n'.format(idf['name'], idf['idf']))
        cndb.add_skill_idf(idf)

    cndb.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--uk-only', action='store_true',
                        help='Compute IDF values using UK jobs only.')
    args = parser.parse_args()
    main(args)
