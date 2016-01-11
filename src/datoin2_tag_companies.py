import conf
from datoin2db import *
from windowquery import splitProcess, processDb
from logger import Logger
from textnormalization import clean
import sys


def tagCompanies(jobid, fromid, toid):
    logger = Logger(sys.stdout)
    dtdb = Datoin2DB(url=conf.DATOIN2_DB)
    
    q = dtdb.query(LIProfile) \
            .filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    def tagCompany(liprofile):
        if liprofile.name:
            name = liprofile.name
        else:
            components = [liprofile.firstName, liprofile.lastName]
            name = ' '.join([c for c in components if c])
        tokens = clean(name, lowercase=True, tokenize=True)

        isCompany = False
        if 'limited' in tokens or 'ltd' in tokens:
            isCompany = True

        liprofile.isCompany = isCompany

    processDb(q, tagCompany, dtdb, logger=logger)



try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
except (ValueError, IndexError):
    print('usage: python3 canonical_parse_linkedin.py <njobs> <batchsize>')
    exit(1)

dtdb = Datoin2DB(url=conf.DATOIN2_DB)
logger = Logger(sys.stdout)

q = dtdb.query(LIProfile.id)
splitProcess(q, tagCompanies, batchsize,
             njobs=njobs, logger=logger,
             workdir='jobs', prefix='datoin2_tag_companies')


