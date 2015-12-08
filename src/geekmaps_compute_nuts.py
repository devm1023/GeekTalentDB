import conf
import geekmapsdb
from analyticsdb import *
from logger import Logger
import sys
from windowquery import splitProcess
from nuts import NutsRegions
from geoalchemy2.shape import to_shape
from sqlalchemy import and_
from datetime import datetime



def addProfileSkills(jobid, fromid, toid, nuts):
    batchsize = 1000

    andb = AnalyticsDB(conf.ANALYTICS_DB)
    gmdb = geekmapsdb.GeekMapsDB(conf.GEEKMAPS_DB)
    logger = Logger(sys.stdout)
    
    q = andb.query(LIProfile, Location, LIProfileSkill) \
            .join(Location) \
            .outerjoin(LIProfileSkill) \
            .filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)
    q = q.order_by(LIProfile.id)

    currentid = None
    nutsid = None
    profilecount = -1
    for liprofile, location, liprofileskill in q:
        if currentid != liprofile.id:
            nutsid = None
            if location is not None:
                point = to_shape(location.geo)
                nutsid = nuts.find(point)
            profilecount += 1
            currentid = liprofile.id
            if profilecount != 0 and profilecount % batchsize == 0:
                gmdb.commit()
                logger.log('Batch: {0:d} profiles processed.\n' \
                           .format(profilecount))

        nrmSkill = None
        rank = None
        if liprofileskill is not None:
            nrmSkill = liprofileskill.nrmName
            rank = liprofileskill.rank
        gmdb.addLIProfileSkill(liprofile.id,
                               liprofile.language,
                               location.name,
                               nutsid,
                               liprofile.nrmTitle,
                               liprofile.nrmCompany,
                               nrmSkill,
                               rank)
    profilecount += 1
    gmdb.commit()
    logger.log('Batch: {0:d} profiles processed.\n'.format(profilecount))


try:
    njobs = int(sys.argv[1])
    batchsize = int(sys.argv[2])
    fromid = None
    if len(sys.argv) > 3:
        fromid = int(sys.argv[3])
except (ValueError, IndexError):
    print('python3 geekmaps_compute_nuts.py <njobs> <batchsize> [<from-id>]')
    exit(1)


andb = AnalyticsDB(conf.ANALYTICS_DB)
nuts = NutsRegions(conf.NUTS_DATA)
logger = Logger(sys.stdout)

query = andb.query(LIProfile.id)
if fromid is not None:
    query = query.filter(LIProfile.id >= fromid)
    
splitProcess(query, addProfileSkills, batchsize,
             njobs=njobs, args=[nuts], logger=logger,
             workdir='jobs', prefix='compute_nuts')
