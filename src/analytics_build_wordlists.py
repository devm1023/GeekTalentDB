import conf
from analyticsdb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import splitProcess, processDb
from textnormalization import splitNrmName
import argparse


def addWords(jobid, fromentity, toentity):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(Entity.nrmName) \
            .filter(Entity.nrmName >= fromentity)
    if toentity is not None:
        q = q.filter(Entity.nrmName < toentity)
    q = q.order_by(Entity.nrmName)

    def addEntity(rec):
        nrmName, = rec
        tpe, source, language, words = splitNrmName(nrmName)
        for word in set(words.split()):
            andb.addFromDict({
                'type'     : tpe,
                'source'   : source,
                'language' : language,
                'word'     : word,
                'nrmName'  : nrmName,
                }, Word)
            
    processDb(q, addEntity, andb, logger=logger)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('njobs',
                        help='The number of parallel jobs.',
                        type=int)
    parser.add_argument('batchsize',
                        help='The number of rows in each parallel batch.',
                        type=int)
    parser.add_argument('--from-entity', help=
                        'The entity to start from. Useful for crash recovery.',
                        default=None)
    args = parser.parse_args()
    
    njobs = args.njobs
    batchsize = args.batchsize
    startval = args.from_entity
    
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = andb.query(Entity.nrmName)
    if startval:
        q = q.filter(Entity.nrmName >= startval)
    splitProcess(q, addWords, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_wordlists')
