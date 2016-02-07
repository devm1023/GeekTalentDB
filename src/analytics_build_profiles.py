import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from sqldb import dictFromRow
from windowquery import splitProcess, processDb
import argparse


def addLIProfiles(jobid, fromid, toid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(LIProfile, Location) \
            .outerjoin(Location, LIProfile.nrmLocation == Location.nrmName) \
            .filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    def addLIProfile(rec):
        liprofile, location = rec
        liprofiledict = dictFromRow(liprofile)
        
        if 'title' in liprofiledict:
            liprofiledict['rawTitle'] = liprofiledict.pop('title')
        if 'company' in liprofiledict:
            liprofiledict['rawCompany'] = liprofiledict.pop('company')
        if 'sector' in liprofiledict:
            liprofiledict['rawSector'] = liprofiledict.pop('sector')
            
        if liprofiledict.get('experiences', None) is not None:
            for liexperience in liprofiledict['experiences']:
                placeId = None
                if liexperience.get('nrmLocation', None) is not None:
                    placeId = cndb.query(Location.placeId) \
                                  .filter(Location.nrmName ==
                                          liexperience['nrmLocation']) \
                                  .first()
                    if placeId is not None:
                        placeId = placeId[0]
                liexperience['placeId'] = placeId
                if 'title' in liexperience:
                    liexperience['rawTitle'] = liexperience.pop('title')
                if 'company' in liexperience:
                    liexperience['rawCompany'] = liexperience.pop('company')
                if liexperience.get('skills', None) is not None:
                    liexperience['skills'] \
                        = [s['skill']['nrmName'] \
                           for s in liexperience['skills']]

        if liprofiledict.get('educations', None) is not None:
            for lieducation in liprofiledict['educations']:                
                if 'institute' in lieducation:
                    lieducation['rawInstitute'] = lieducation.pop('institute')
                if 'degree' in lieducation:
                    lieducation['rawDegree'] = lieducation.pop('degree')
                if 'subject' in lieducation:
                    lieducation['rawSubject'] = lieducation.pop('subject')
                    
        if location is not None:
            liprofiledict['placeId'] = location.placeId
            
        andb.addLIProfile(liprofiledict)
        
    processDb(q, addLIProfile, andb, logger=logger)


def addINProfiles(jobid, fromid, toid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)
    
    q = cndb.query(INProfile, Location) \
            .outerjoin(Location, INProfile.nrmLocation == Location.nrmName) \
            .filter(INProfile.id >= fromid)
    if toid is not None:
        q = q.filter(INProfile.id < toid)

    def addINProfile(rec):
        inprofile, location = rec
        inprofiledict = dictFromRow(inprofile)
        
        if 'title' in inprofiledict:
            inprofiledict['rawTitle'] = inprofiledict.pop('title')
        if 'company' in inprofiledict:
            inprofiledict['rawCompany'] = inprofiledict.pop('company')
            
        if inprofiledict.get('experiences', None) is not None:
            for inexperience in inprofiledict['experiences']:
                placeId = None
                if inexperience.get('nrmLocation', None) is not None:
                    placeId = cndb.query(Location.placeId) \
                                  .filter(Location.nrmName ==
                                          inexperience['nrmLocation']) \
                                  .first()
                    if placeId is not None:
                        placeId = placeId[0]
                inexperience['placeId'] = placeId
                if 'title' in inexperience:
                    inexperience['rawTitle'] = inexperience.pop('title')
                if 'company' in inexperience:
                    inexperience['rawCompany'] = inexperience.pop('company')
                if inexperience.get('skills', None) is not None:
                    inexperience['skills'] \
                        = [s['skill']['nrmName'] \
                           for s in inexperience['skills']]

        if inprofiledict.get('educations', None) is not None:
            for ineducation in inprofiledict['educations']:                
                if 'institute' in ineducation:
                    ineducation['rawInstitute'] = ineducation.pop('institute')
                if 'degree' in ineducation:
                    ineducation['rawDegree'] = ineducation.pop('degree')
                if 'subject' in ineducation:
                    ineducation['rawSubject'] = ineducation.pop('subject')
                    
        if location is not None:
            inprofiledict['placeId'] = location.placeId
            
        andb.addINProfile(inprofiledict)
        
    processDb(q, addINProfile, andb, logger=logger)


def addProfiles(args):
    logger = Logger(sys.stdout)
    if args.source is None:
        logger.log('Building LinkedIn profiles.\n')
        args.source = 'linkedin'
        addProfiles(args)
        logger.log('Building Indeed profiles.\n')
        args.source = 'indeed'
        addProfiles(args)
        return
    elif args.source == 'linkedin':
        table = LIProfile
        addfunc = addLIProfiles
    elif args.source == 'indeed':
        table = INProfile
        addfunc = addINProfiles
    else:
        raise ValueError('Invalid source.')

    cndb = CanonicalDB(conf.CANONICAL_DB)

    q = cndb.query(table.id)
    if args.fromid is not None:
        q = q.filter(table.id >= args.fromid)
    splitProcess(q, addfunc, args.batchsize,
                 njobs=args.njobs, logger=logger,
                 workdir='jobs', prefix='analytics_build_liprofiles')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('njobs',
                        help='The number of parallel jobs.',
                        type=int)
    parser.add_argument('batchsize',
                        help='The number of rows in each parallel batch.',
                        type=int)
    parser.add_argument('--fromid',
                        help='The profile ID to start from.',
                        type=int)
    parser.add_argument('--source',
                        help='The data source to process.',
                        choices=['linkedin', 'indeed'])
    args = parser.parse_args()
    addProfiles(args)


