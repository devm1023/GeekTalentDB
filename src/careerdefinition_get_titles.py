from canonicaldb import *
from entitycloud import entity_cloud
from textnormalization import normalized_sector
from entity_mapper import EntityMapper
from sqlalchemy import func
from pgvalues import in_values
import sys
import csv
import argparse


def get_sectors(sectors, filename, mapper, norm):
    sectors = [mapper(norm(s)) for s in sectors]
    if filename:
        with open(filename, 'r') as sectorfile:
            for line in sectorfile:
                row = line.split('|')
                if not row:
                    continue
                sector = mapper(norm(row[0]))
                if not sector:
                    continue
                if sector not in sectors:
                    sectors.append(sector)
    return sectors

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for a job title to be included. '
                        'Default: 1')
    parser.add_argument('--max-titles', type=int, default=20,
                        help='Maximum number of titles per sector. '
                        'Default: 20')
    parser.add_argument('--sigma', type=int, default=3,
                        help='Minimal significance of relevance scores. '
                        'Default: 3')
    parser.add_argument('--sector-filter-fraction', type=float, default=0.5,
                        help='Apply sector filter for job titles where the '
                        'fraction of people coming from other sectors is '
                        'larger than SECTOR_FILTER_FRACTION.')
    parser.add_argument('--sectors-from',
                        help='Name of file holding sector names.')
    parser.add_argument('--mappings',
                        help='Name of a csv file holding entity mappings. '
                        'Columns: type | lang | sector | name | mapped name')
    parser.add_argument('--source', choices=['linkedin', 'indeed', 'adzuna'], default='linkedin',
                        help='The data source to process.')
    parser.add_argument('sector', nargs='*', default=[],
                        help='The merged sectors to scan.')
    args = parser.parse_args()

    cndb = CanonicalDB()
    mapper = EntityMapper(cndb, args.mappings)

    # Adzuna categories are not normalised
    if args.source == 'adzuna':
        norm_sector = lambda x: x
    else:
        norm_sector = normalized_sector
    sectors = get_sectors(args.sector, args.sectors_from, mapper, norm_sector)

    if not sectors:
        sys.stderr.write('You must specify at least one sector.\n')
        sys.stderr.flush()
        sys.exit(1)

    if args.source == 'linkedin':
        profile_table = LIProfile
    elif args.source == 'indeed':
        profile_table = INProfile
    elif args.source == 'adzuna':
        profile_table = ADZJob

    totalc = cndb.query(profile_table.id)

    # Adzuna jobs have categories instead of sectors, should always be in the uk and always
    # have a category
    if args.source != 'adzuna':
        totalc = totalc.join(Location,
                             Location.nrm_name == profile_table.nrm_location) \
                       .filter(profile_table.nrm_sector != None,
                               profile_table.language == 'en',
                               Location.nuts0 == 'UK')
    totalc = totalc.count()

    joblists = {}
    sectorcounts = {}
    countcol = func.count().label('counts')
    csvwriter = csv.writer(sys.stdout)
    for nrm_sector in sectors:
        if args.source == 'adzuna':
            sector = nrm_sector

            sectorc = cndb.query(profile_table.id) \
                        .filter(profile_table.category == nrm_sector)  \
                        .count()

            # build title cloud
            entityq = lambda entities: \
                    cndb.query(profile_table.nrm_title, countcol) \
                        .filter(in_values(profile_table.nrm_title, entities)) \
                        .group_by(profile_table.nrm_title)

            coincidenceq = cndb.query(profile_table.nrm_title, countcol) \
                            .filter(profile_table.category == nrm_sector)

            entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
            jobs = entity_cloud(totalc, sectorc, entityq, coincidenceq,
                                entitymap=entitymap, limit=args.max_titles,
                                mincount=args.min_count, sigma=args.sigma)
        else:
            sector = mapper.name(nrm_sector)
            lisectors = mapper.inv(nrm_sector)
            sectorc = cndb.query(profile_table.id) \
                        .join(Location,
                                Location.nrm_name == profile_table.nrm_location) \
                        .filter(profile_table.nrm_sector.in_(lisectors),
                                profile_table.language == 'en',
                                Location.nuts0 == 'UK') \
                        .count()

            # build title cloud
            entityq = lambda entities: \
                    cndb.query(profile_table.nrm_curr_title, countcol) \
                        .join(Location,
                                Location.nrm_name == profile_table.nrm_location) \
                        .filter(in_values(profile_table.nrm_curr_title, entities),
                                profile_table.nrm_sector != None,
                                profile_table.language == 'en',
                                Location.nuts0 == 'UK') \
                        .group_by(profile_table.nrm_curr_title)
            coincidenceq = cndb.query(profile_table.nrm_curr_title, countcol) \
                            .join(Location,
                                    Location.nrm_name == profile_table.nrm_location) \
                            .filter(profile_table.nrm_sector.in_(lisectors),
                                    profile_table.language == 'en',
                                    Location.nuts0 == 'UK')
            entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
            jobs = entity_cloud(totalc, sectorc, entityq, coincidenceq,
                                entitymap=entitymap, limit=args.max_titles,
                                mincount=args.min_count, sigma=args.sigma)

        for nrm_title, titlec, sectortitlec, score, error in jobs:
            title = mapper.name(nrm_title)
            frac1 = sectortitlec/sectorc
            frac2 = (titlec-sectortitlec)/(totalc-sectorc)
            sector_fraction = (titlec - sectortitlec)/titlec
            sector_filter = 0
            if sector_fraction > args.sector_filter_fraction:
                sector_filter = 1
            csvwriter.writerow([sector, title, sector_filter, score,
                                sectortitlec, titlec, sectorc, totalc,
                                frac1, frac2, error])
