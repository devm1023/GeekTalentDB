import conf
import geekmapsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from windowquery import splitProcess, windows


def addSkills(fromskill, toskill):
    batchsize = 1000

    cndb = NormalFormDB(conf.CANONICAL_DB)
    gmdb = geekmapsdb.GeekMapsDB(conf.GEEKMAPS_DB)
    logger = Logger(sys.stdout)

    
    q = cndb.query(Skill.nrmName, Skill.name, func.count(Skill.profileId)) \
            .filter(Skill.nrmName >= fromskill)
    if toskill is not None:
        q = q.filter(Skill.nrmName < toskill)
    q = q.group_by(Skill.nrmName, Skill.name).order_by(Skill.nrmName)

    currentskill = None
    maxcount = 0
    profilecount = 0
    bestname = None
    skillcount = 0
    for nrmName, name, count in q:
        if nrmName != currentskill:
            if bestname:
                gmdb.addSkill(currentskill, bestname, profilecount)
                skillcount += 1
                if skillcount % batchsize == 0:
                    gmdb.commit()
                    logger.log('Batch: {0:d} skills processed.\n' \
                               .format(skillcount))
            maxcount = 0
            profilecount = 0
            bestname = None
            currentskill = nrmName
        if count > maxcount:
            bestname = name
            maxcount = count
        profilecount += count

    if bestname:
        gmdb.addSkill(currentskill, bestname, profilecount)
    gmdb.commit()
    logger.log('Batch: {0:d} skills processed.\n'.format(skillcount))

    if currentskill is None:
        logger.log('Batch: range {0:s} to {1:s}\n' \
                   .format(repr(fromskill), repr(toskill)))
        raise ValueError('')
    return skillcount, currentskill




cndb = NormalFormDB(conf.CANONICAL_DB)
logger = Logger(sys.stdout)

njobs = int(sys.argv[1])
batchsize = int(sys.argv[2])

q = cndb.query(Skill.nrmName).filter(Skill.nrmName != None)

splitProcess(q, addSkills, batchsize,
             njobs=njobs, logger=logger,
             workdir='jobs', prefix='build_catalogues')
