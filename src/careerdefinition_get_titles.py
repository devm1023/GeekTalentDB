import conf
from analyticsdb import *
from analytics_get_entitycloud import entity_cloud
from careerdefinitiondb import CareerDefinitionDB
from textnormalization import normalized_entity, normalized_sector
from entity_mapper import EntityMapper
from sqlalchemy import func
import sys
import csv
import argparse


def get_sectors(sectors, filename, mapper):
    sectors = [mapper(normalized_sector(s)) for s in sectors]
    if filename:
        with open(filename, 'r') as sectorfile:
            for line in sectorfile:
                row = line.split('|')
                if not row:
                    continue
                sector = mapper(normalized_sector(row[0]))
                if not sector:
                    continue
                if sector not in sectors:
                    sectors.append(sector)
    return sectors

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for an object to be included '
                        'in a list.')
    parser.add_argument('--max-careers', type=int, default=20,
                        help='Maximum number of careers per sector.')
    parser.add_argument('--sigma', type=int, default=3,
                        help='Minimal significance of relevance scores.')
    parser.add_argument('--sectors-from',
                        help='Name of file holding sector names.')
    parser.add_argument('--mappings',
                        help='Name of a csv file holding entity mappings. '
                        'Columns: type | lang | sector | name | mapped name')
    parser.add_argument('sector', nargs='*', default=[],
                        help='The LinkedIn sectors to scan.')
    args = parser.parse_args()

    andb = AnalyticsDB(conf.ANALYTICS_DB)
    mapper = EntityMapper(andb, args.mappings)
    sectors = get_sectors(args.sector, args.sectors_from, mapper)

    totalc = andb.query(LIProfile.id) \
                 .join(Location) \
                 .filter(LIProfile.nrm_sector != None,
                         LIProfile.language == 'en',
                         Location.nuts0 == 'UK') \
                 .count()

    joblists = {}
    sectorcounts = {}
    countcol = func.count().label('counts')
    csvwriter = csv.writer(sys.stdout)
    for nrm_sector in sectors:
        sector = mapper.name(nrm_sector)
        lisectors = mapper.inv(nrm_sector)
        sectorc = andb.query(LIProfile.id) \
                      .join(Location) \
                      .filter(LIProfile.nrm_sector.in_(lisectors),
                              LIProfile.language == 'en',
                              Location.nuts0 == 'UK') \
                      .count()

        # build title cloud
        entityq = lambda entities: \
                  andb.query(LIProfile.nrm_curr_title, countcol) \
                      .join(Location) \
                      .filter(LIProfile.nrm_curr_title.in_(entities),
                              LIProfile.nrm_sector != None,
                              LIProfile.language == 'en',
                              Location.nuts0 == 'UK') \
                      .group_by(LIProfile.nrm_curr_title)
        coincidenceq = andb.query(LIProfile.nrm_curr_title, countcol) \
                           .join(Location) \
                           .filter(LIProfile.nrm_sector.in_(lisectors),
                                   LIProfile.language == 'en',
                                   Location.nuts0 == 'UK')
        entitymap = lambda s: mapper(s, nrm_sector=nrm_sector)
        jobs = entity_cloud(totalc, sectorc, entityq, coincidenceq,
                            entitymap=entitymap, limit=args.max_careers,
                            mincount=args.min_count, sigma=args.sigma)
        for nrm_title, titlec, sectortitlec, score, error in jobs:
            title = mapper.name(nrm_title)
            frac1 = sectortitlec/sectorc
            frac2 = (titlec-sectortitlec)/(totalc-sectorc)
            csvwriter.writerow([sector, title, score, sectortitlec, titlec,
                                sectorc, totalc, frac1, frac2, error])
