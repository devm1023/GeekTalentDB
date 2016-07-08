import conf
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import split_process, process_db
from textnormalization import split_nrm_name
import argparse


def add_words(jobid, fromentity, toentity):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(Entity.nrm_name) \
            .filter(Entity.nrm_name >= fromentity)
    if toentity is not None:
        q = q.filter(Entity.nrm_name < toentity)
    q = q.order_by(Entity.nrm_name)

    def add_entity(rec):
        nrm_name, = rec
        tpe, source, language, words = split_nrm_name(nrm_name)
        for word in set(words.split()):
            cndb.add_from_dict({
                'type'     : tpe,
                'source'   : source,
                'language' : language,
                'word'     : word,
                'nrm_name'  : nrm_name,
                }, Word)

    process_db(q, add_entity, cndb, logger=logger)


def main(args):
    njobs = args.jobs
    batchsize = args.batch_size
    startval = args.from_entity

    cndb = CanonicalDB(conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(Entity.nrm_name)
    if startval:
        q = q.filter(Entity.nrm_name >= startval)
    split_process(q, add_words, batchsize,
                  njobs=njobs, logger=logger,
                  workdir='jobs', prefix='build_wordlists')
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='The number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='The number of rows in each parallel batch.')
    parser.add_argument('--from-entity', default=None, help=
                        'The entity to start from. Useful for crash recovery.')
    args = parser.parse_args()
    main(args)

