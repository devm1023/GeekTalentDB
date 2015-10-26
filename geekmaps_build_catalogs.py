import conf
import geekmapsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import splitProcess, windows


def entities(q):
    currententity = None
    maxcount = 0
    profilecount = 0
    bestname = None
    for nrmName, name, count in q:
        if nrmName != currententity:
            if bestname:
                yield currententity, bestname, profilecount
            maxcount = 0
            profilecount = 0
            bestname = None
            currententity = nrmName
        if count > maxcount:
            bestname = name
            maxcount = count
        profilecount += count
    
    if bestname:
        yield currententity, bestname, profilecount


def addSkills(fromskill, toskill):
    batchsize = 1000

    cndb = CanonicalDB(conf.CANONICAL_DB)
    gmdb = geekmapsdb.GeekMapsDB(conf.GEEKMAPS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(Skill.nrmName, Skill.name, func.count(Skill.profileId)) \
            .filter(Skill.nrmName >= fromskill)
    if toskill is not None:
        q = q.filter(Skill.nrmName < toskill)
    q = q.group_by(Skill.nrmName, Skill.name).order_by(Skill.nrmName)

    skillcount = 0
    for nrmName, bestname, profilecount in entities(q):
        gmdb.addSkill(nrmName, bestname, profilecount)
        skillcount += 1
        if skillcount % batchsize == 0:
            gmdb.commit()
            logger.log('Batch: {0:d} skills processed.\n' \
                       .format(skillcount))
    if skillcount % batchsize != 0:
        gmdb.commit()
        logger.log('Batch: {0:d} skills processed.\n' \
                   .format(skillcount))


def addTitles(fromtitle, totitle):
    batchsize = 1000

    cndb = CanonicalDB(conf.CANONICAL_DB)
    gmdb = geekmapsdb.GeekMapsDB(conf.GEEKMAPS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile.nrmTitle, LIProfile.parsedTitle,
                   func.count(LIProfile.id)) \
            .filter(LIProfile.nrmTitle >= fromtitle)
    if totitle is not None:
        q = q.filter(LIProfile.nrmTitle < totitle)
    q = q.group_by(LIProfile.nrmTitle, LIProfile.parsedTitle) \
         .order_by(LIProfile.nrmTitle)

    titlecount = 0
    for nrmName, bestname, profilecount in entities(q):
        gmdb.addTitle(nrmName, bestname, profilecount)
        titlecount += 1
        if titlecount % batchsize == 0:
            gmdb.commit()
            logger.log('Batch: {0:d} titles processed.\n' \
                       .format(titlecount))
    if titlecount % batchsize != 0:
        gmdb.commit()
        logger.log('Batch: {0:d} titles processed.\n' \
                   .format(titlecount))


def addCompanies(fromcompany, tocompany):
    batchsize = 1000

    cndb = CanonicalDB(conf.CANONICAL_DB)
    gmdb = geekmapsdb.GeekMapsDB(conf.GEEKMAPS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile.nrmCompany, LIProfile.company,
                   func.count(LIProfile.id)) \
            .filter(LIProfile.nrmCompany >= fromcompany)
    if tocompany is not None:
        q = q.filter(LIProfile.nrmCompany < tocompany)
    q = q.group_by(LIProfile.nrmCompany, LIProfile.company) \
         .order_by(LIProfile.nrmCompany)

    companycount = 0
    for nrmName, bestname, profilecount in entities(q):
        gmdb.addCompany(nrmName, bestname, profilecount)
        companycount += 1
        if companycount % batchsize == 0:
            gmdb.commit()
            logger.log('Batch: {0:d} companies processed.\n' \
                       .format(companycount))
    if companycount % batchsize != 0:
        gmdb.commit()
        logger.log('Batch: {0:d} companies processed.\n' \
                   .format(companycount))



cndb = CanonicalDB(conf.CANONICAL_DB)
logger = Logger(sys.stdout)

try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    catalog = None
    startval = None
    if len(sys.argv) > 3:
        catalog = sys.argv[3]
        if catalog not in ['skills', 'titles', 'companies']:
            raise ValueError('Invalid catalog string')
    if len(sys.argv) > 4:
        startval = sys.argv[4]
except ValueError:
    logger.log('usage: python3 build_catalogs.py <njobs> <batchsize> '
               '[(skills | titles) [<start-value>]]\n')

if catalog is None or catalog == 'skills':
    logger.log('\nBuilding skills catalog.\n')
    q = cndb.query(Skill.nrmName).filter(Skill.nrmName != None)
    if startval:
        q = q.filter(Skill.nrmName >= startval)
    splitProcess(q, addSkills, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_skills')

if catalog is None or catalog == 'titles':
    logger.log('\nBuilding titles catalog.\n')
    q = cndb.query(LIProfile.nrmTitle).filter(LIProfile.nrmTitle != None)
    if startval:
        q = q.filter(LIProfile.nrmTitle >= startval)
    splitProcess(q, addTitles, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_titles')

if catalog is None or catalog == 'companies':
    logger.log('\nBuilding companies catalog.\n')
    q = cndb.query(LIProfile.nrmCompany).filter(LIProfile.nrmCompany != None)
    if startval:
        q = q.filter(LIProfile.nrmCompany >= startval)
    splitProcess(q, addCompanies, batchsize,
                 njobs=njobs, logger=logger,
                 workdir='jobs', prefix='build_companies')
