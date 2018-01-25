import math

from canonicaldb import *
import sys
from logger import Logger
import argparse
from sqlalchemy import func


def main(args):
    logger = Logger(sys.stdout)
    cndb = CanonicalDB()

    doc_count = cndb.query(ADZJob).count() + cndb.query(INJob).count()
    logger.log('Total jobs found {0:d}\n'.format(doc_count))

    countcol = func.count().label('counts')
    adz_doc_freq = dict(cndb.query(ADZJobSkill.name, countcol).group_by(ADZJobSkill.name))
    ind_doc_freq = dict(cndb.query(INJobSkill.name, countcol).group_by(INJobSkill.name))
    doc_freq = {k: adz_doc_freq.get(k, 0) + ind_doc_freq.get(k, 0) for k in set(adz_doc_freq) | set(ind_doc_freq)}

    logger.log('adz_doc_freq size {0:d}\n'.format(len(adz_doc_freq)))
    logger.log('ind_doc_freq size {0:d}\n'.format(len(ind_doc_freq)))
    logger.log('doc_freq size {0:d}\n'.format(len(doc_freq)))

    logger.log('Calculating idf values...\n')
    for skill_name, df in doc_freq.items():
        idf = {'name': skill_name, 'idf': math.log(doc_count / df)}
        logger.log('{0:s}, {1:f}\n'.format(idf['name'], idf['idf']))
        cndb.add_skill_idf(idf)

    cndb.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # To be implemented.
    # parser.add_argument('--all-sectors', action='store_true',
    #                    help=Compute IDF values where document frequency is calculated within all sectors.')
    args = parser.parse_args()
    main(args)


