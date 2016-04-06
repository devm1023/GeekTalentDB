import conf
import analyticsdb
from canonicaldb import *
from sqlalchemy import func
from logger import Logger
import sys
from sqldb import dict_from_row
from windowquery import split_process, process_db
import argparse


def add_liprofiles(jobid, fromid, toid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(LIProfile, Location) \
            .outerjoin(Location, LIProfile.nrm_location == Location.nrm_name) \
            .filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    def add_liprofile(rec):
        liprofile, location = rec
        liprofiledict = dict_from_row(liprofile)

        if 'title' in liprofiledict:
            liprofiledict['raw_title'] = liprofiledict.pop('title')
        if 'company' in liprofiledict:
            liprofiledict['raw_company'] = liprofiledict.pop('company')
        if 'sector' in liprofiledict:
            liprofiledict['raw_sector'] = liprofiledict.pop('sector')

        if liprofiledict.get('experiences', None) is not None:
            for liexperience in liprofiledict['experiences']:
                place_id = None
                if liexperience.get('nrm_location', None) is not None:
                    place_id = cndb.query(Location.place_id) \
                                  .filter(Location.nrm_name ==
                                          liexperience['nrm_location']) \
                                  .first()
                    if place_id is not None:
                        place_id = place_id[0]
                liexperience['place_id'] = place_id
                if 'title' in liexperience:
                    liexperience['raw_title'] = liexperience.pop('title')
                if 'company' in liexperience:
                    liexperience['raw_company'] = liexperience.pop('company')
                if liexperience.get('skills', None) is not None:
                    liexperience['skills'] \
                        = [s['skill']['nrm_name'] \
                           for s in liexperience['skills']]

        if liprofiledict.get('educations', None) is not None:
            for lieducation in liprofiledict['educations']:
                if 'institute' in lieducation:
                    lieducation['raw_institute'] = lieducation.pop('institute')
                if 'degree' in lieducation:
                    lieducation['raw_degree'] = lieducation.pop('degree')
                if 'subject' in lieducation:
                    lieducation['raw_subject'] = lieducation.pop('subject')

        if location is not None:
            liprofiledict['place_id'] = location.place_id

        andb.add_liprofile(liprofiledict)

    process_db(q, add_liprofile, andb, logger=logger)


def add_inprofiles(jobid, fromid, toid):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    andb = analyticsdb.AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger(sys.stdout)

    q = cndb.query(INProfile, Location) \
            .outerjoin(Location, INProfile.nrm_location == Location.nrm_name) \
            .filter(INProfile.id >= fromid)
    if toid is not None:
        q = q.filter(INProfile.id < toid)

    def add_inprofile(rec):
        inprofile, location = rec
        inprofiledict = dict_from_row(inprofile)

        if 'title' in inprofiledict:
            inprofiledict['raw_title'] = inprofiledict.pop('title')
        if 'company' in inprofiledict:
            inprofiledict['raw_company'] = inprofiledict.pop('company')

        if inprofiledict.get('experiences', None) is not None:
            for inexperience in inprofiledict['experiences']:
                place_id = None
                if inexperience.get('nrm_location', None) is not None:
                    place_id = cndb.query(Location.place_id) \
                                  .filter(Location.nrm_name ==
                                          inexperience['nrm_location']) \
                                  .first()
                    if place_id is not None:
                        place_id = place_id[0]
                inexperience['place_id'] = place_id
                if 'title' in inexperience:
                    inexperience['raw_title'] = inexperience.pop('title')
                if 'company' in inexperience:
                    inexperience['raw_company'] = inexperience.pop('company')
                if inexperience.get('skills', None) is not None:
                    inexperience['skills'] \
                        = [s['skill']['nrm_name'] \
                           for s in inexperience['skills']]

        if inprofiledict.get('educations', None) is not None:
            for ineducation in inprofiledict['educations']:
                if 'institute' in ineducation:
                    ineducation['raw_institute'] = ineducation.pop('institute')
                if 'degree' in ineducation:
                    ineducation['raw_degree'] = ineducation.pop('degree')
                if 'subject' in ineducation:
                    ineducation['raw_subject'] = ineducation.pop('subject')

        if location is not None:
            inprofiledict['place_id'] = location.place_id

        andb.add_inprofile(inprofiledict)

    process_db(q, add_inprofile, andb, logger=logger)


def main(args):
    logger = Logger(sys.stdout)
    if args.source is None:
        logger.log('Building LinkedIn profiles.\n')
        args.source = 'linkedin'
        main(args)
        logger.log('Building Indeed profiles.\n')
        args.source = 'indeed'
        main(args)
        return
    elif args.source == 'linkedin':
        table = LIProfile
        addfunc = add_liprofiles
    elif args.source == 'indeed':
        table = INProfile
        addfunc = add_inprofiles
    else:
        raise ValueError('Invalid source.')

    cndb = CanonicalDB(conf.CANONICAL_DB)

    q = cndb.query(table.id)
    if args.from_id is not None:
        q = q.filter(table.id >= args.from_id)
    split_process(q, addfunc, args.batch_size,
                  njobs=args.jobs, logger=logger,
                  workdir='jobs', prefix='analytics_build_liprofiles')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='The number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='The number of rows in each parallel batch.')
    parser.add_argument('--from-id', type=int,
                        help='The profile ID to start from.')
    parser.add_argument('--source', choices=['linkedin', 'indeed'],
                        help='The data source to process.')
    args = parser.parse_args()
    main(args)


