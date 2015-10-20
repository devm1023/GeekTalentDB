import conf
import geekmapsdb
from canonicaldb import *
from logger import Logger
import sys
from windowquery import splitProcess
from nuts import NutsRegions
from geoalchemy2.shape import to_shape
from sqlalchemy import and_
from datetime import datetime



def addProfileSkills(fromid, toid, fromdate, todate, nuts):
    batchsize = 1000

    cndb = CanonicalDB(conf.CANONICAL_DB)
    gmdb = geekmapsdb.GeekMapsDB(conf.GEEKMAPS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile, Skill, Location) \
            .filter(LIProfile.indexedOn >= fromdate,
                    LIProfile.indexedOn < todate,
                    LIProfile.id == Skill.profileId,
                    LIProfile.nrmLocation == Location.nrmName,
                    LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)
    q = q.order_by(LIProfile.id)

    currentid = None
    nutsid = None
    profilecount = -1
    for liprofile, liprofileskill, location in q:
        if currentid != liprofile.id:
            nutsid = None
            if location.geo is not None:
                point = to_shape(location.geo)
                nutsid = nuts.find(point)
            profilecount += 1
            currentid = liprofile.id
            if profilecount != 0 and profilecount % batchsize == 0:
                gmdb.commit()
                logger.log('Batch: {0:d} profiles processed.\n' \
                           .format(profilecount))
        gmdb.addLIProfileSkill(liprofile.id,
                               nutsid,
                               liprofile.nrmTitle,
                               liprofile.nrmCompany,
                               liprofileskill.nrmName,
                               liprofileskill.rank,
                               liprofile.indexedOn)
    profilecount += 1
    gmdb.commit()
    logger.log('Batch: {0:d} profiles processed.\n'.format(profilecount))


njobs = int(sys.argv[1])
batchsize = int(sys.argv[2])
fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
fromid = None
if len(sys.argv) > 5:
    fromid = int(sys.argv[5])

filter = and_(LIProfile.indexedOn >= fromdate,
              LIProfile.indexedOn < todate)
if fromid is not None:
    filter = and_(filter, LIProfile.id >= fromid)

cndb = CanonicalDB(conf.CANONICAL_DB)
nuts = NutsRegions(conf.NUTS_DATA)
logger = Logger(sys.stdout)

query = cndb.query(LIProfile.id).filter(filter)
splitProcess(query, addProfileSkills, batchsize,
             njobs=njobs, args=[fromdate, todate, nuts], logger=logger,
             workdir='jobs', prefix='compute_nuts')
