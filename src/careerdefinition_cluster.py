import conf
from analyticsdb import *
from logger import Logger
from textnormalization import normalized_entity, split_nrm_name
from analytics_get_entitycloud import relevance_scores
from entity_mapper import EntityMapper
from sqlalchemy import func
import csv
from math import log, sqrt, acos
import argparse

from clustering import find_clusters
import numpy as np
from sklearn import manifold


def get_skillvectors(filename, titles_from):
    title_set = set()
    has_titles_from = False
    if titles_from:
        has_titles_from = True
        with open(titles_from, 'r') as inputfile:
            csvreader = csv.reader(line for line in inputfile \
                                   if not line.strip().startswith('#'))
            for row in csvreader:
                title_set.add((row[0], row[1], bool(int(row[2]))))
    
    titles = []
    titlecounts = []
    skillvectors = []
    skip = True
    with open(filename, 'r') as inputfile:
        csvreader = csv.reader(inputfile)
        for row in csvreader:
            if not row:
                continue
            if row[0] == 't':
                skip = False
                title = (row[1], row[2], bool(int(row[3])))
                if has_titles_from and title not in title_set:
                    skip = True
                    continue
                title_set.discard(title)
                titles.append(title)
                titlecount = int(row[4])
                titlecounts.append(titlecount)
                skillvectors.append({})
            elif row[0] == 's':
                if skip:
                    continue
                skillvectors[-1][row[1]] = float(row[2])*titlecount
            else:
                raise IOError('Invalid row {0:s}'.format(row))

    if has_titles_from and title_set:
        missing_titles = set(t[1] for t in title_set)
        raise IOError('Missing titles: {0:s}'.format(repr(missing_titles)))

    return titles, titlecounts, skillvectors


def distance(vec1, vec2, power=0):
    v1sq = sum(v**2 for v in vec1.values())
    v2sq = sum(v**2 for v in vec2.values())
    factor = min(v1sq**power, v2sq**power)
    v1v2 = sum(v1*vec2.get(k, 0.0) for k, v1 in vec1.items())
    return factor*acos(max(0.0, min(1.0, v1v2/sqrt(v1sq*v2sq))))


def merge(vec1, vec2):
    result = vec1.copy()
    for k, v2 in vec2.items():
        result[k] = result.get(k, 0) + v2
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file',
                        help='CSV file with skill vectors.')
    parser.add_argument('--titles-from',
                        help='CSV file holding the titles to cluster.')
    parser.add_argument('--clusters', type=int, default=20,
                        help='Number of clusters. Default: 20')
    parser.add_argument('--power', type=float, default=1.0,
                        help='p-parameter for the clustering algorithm. '
                        'Default: 1, which corresponds to k_t.')    
    parser.add_argument('--output', 
                        help='Name of CSV file to write clusters to.')
    parser.add_argument('--mapping-output',
                        help='Name of mapping file to generate.')
    args = parser.parse_args()

    logger = Logger()
    
    titles, titlecounts, skillvectors \
        = get_skillvectors(args.input_file, args.titles_from)
    titlecounts = dict(zip(titles, titlecounts))

    logger.log('Generating clusters.\n')
    clusters, _ = find_clusters(
        args.clusters, skillvectors, labels=titles,
        distance=lambda v1, v2: distance(v1, v2, power=args.power),
        merge=merge)
    clusters = [(sorted(list((t, titlecounts[t]) for t in c),
                        key=lambda x: -x[-1]),
                 max(c, key=lambda x: titlecounts[x]),
                 sum(titlecounts[t] for t in c)) for c in clusters]
    clusters.sort(key=lambda c: -c[-1])
    

    # print clusters
    clusters.sort(key=lambda c: -c[-1])
    for cluster, title, count in clusters:
        logger.log('{0:s} ({1:d})\n'.format(title[1], count))
        for title, count in cluster:
            logger.log('    {0:s} ({1:d})\n'.format(title[1], count))
    logger.log('\n')

    # write clusters to csv
    if args.output:
        with open(args.output, 'w') as outputfile:
            csvwriter = csv.writer(outputfile)
            for cluster, title, count in clusters:
                for (sector, subtitle, sector_filter), subcount in cluster:
                    csvwriter.writerow(
                        [sector, subtitle, int(sector_filter), title[1],
                         subcount])

    # generate mappings file
    if args.mapping_output:
        with open(args.mapping_output, 'w') as outputfile:
            csvwriter = csv.writer(outputfile)
            for cluster, title, _ in clusters:
                for (sector, subtitle, sector_filter), _ in cluster:
                    sector = sector if sector_filter else ''
                    csvwriter.writerow(
                        ['title', 'en', sector, subtitle, title[1]])
