import conf
from analyticsdb import *
from logger import Logger
from textnormalization import normalized_entity
from analytics_get_entitycloud import relevance_scores
from entity_mapper import EntityMapper
from sqlalchemy import func
import csv
from math import log, sqrt, acos
import argparse

from clustering import find_clusters
import numpy as np
from sklearn import manifold

from bokeh.plotting import figure, output_file, show, save, ColumnDataSource
from bokeh.models import HoverTool
from bokeh_aspect import *


def _iter_items(keys, d):
    for key in keys:
        if key in d:
            yield key, d[key]


def skillvectors(titles, mappings, mincount=1):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger()
    mapper = EntityMapper(andb, mappings)

    logger.log('Counting profiles.\n')
    totalc_nosf = andb.query(LIProfile.id) \
                      .join(Location) \
                      .filter(LIProfile.language == 'en',
                              Location.nuts0 == 'UK') \
                      .count()
    totalc_sf = andb.query(LIProfile.id) \
                    .join(Location) \
                    .filter(LIProfile.nrm_sector != None,
                            LIProfile.language == 'en',
                            Location.nuts0 == 'UK') \
                    .count()

    logger.log('Counting skills.\n')
    countcol = func.count().label('counts')
    q = andb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en') \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= mincount)
    skillcounts_nosf = dict(q)
    q = andb.query(LIProfileSkill.nrm_name, countcol) \
            .join(LIProfile) \
            .join(Location) \
            .filter(Location.nuts0 == 'UK',
                    LIProfile.language == 'en',
                    LIProfile.nrm_sector != None) \
            .group_by(LIProfileSkill.nrm_name) \
            .having(countcol >= mincount)
    skillcounts_sf = dict(q)

    skillvectors = []
    newtitles = []
    titlecounts = []
    for sector, title, sector_filter in titles:
        logger.log('Processing: {0:s}\n'.format(title))
        nrm_title = normalized_entity('title', 'linkedin', 'en', title)
        nrm_sector = normalized_entity('sector', 'linkedin', 'en', sector)
        
        if sector_filter:
            totalc = totalc_sf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_sf)
            sector_equal_filter = (
                LIProfile.nrm_sector.in_(mapper.inv(nrm_sector)),)
            sector_notnull_filter = (LIProfile.nrm_sector != None,)
        else:
            totalc = totalc_nosf
            entitiesq = lambda entities: _iter_items(entities, skillcounts_nosf)
            sector_equal_filter = tuple()
            sector_notnull_filter = tuple()
            
        titlec = andb.query(LIProfile.id) \
                     .join(Location) \
                     .filter(LIProfile.language == 'en',
                             Location.nuts0 == 'UK',
                             LIProfile.nrm_curr_title.in_(
                                 mapper.inv(nrm_title)),
                             *sector_notnull_filter) \
                     .count()
        coincidenceq = andb.query(LIProfileSkill.nrm_name, func.count()) \
                           .join(LIProfile) \
                           .join(Location) \
                           .filter(LIProfile.language == 'en',
                                   Location.nuts0 == 'UK',
                                   LIProfile.nrm_curr_title.in_(
                                       mapper.inv(nrm_title)),
                                   *sector_equal_filter)
        
        skillvector = {}
        for nrm_skill, skillc, titleskillc, _, _ in \
            relevance_scores(totalc, titlec, entitiesq, coincidenceq,
                             entitymap=mapper):
            if skillc < mincount:
                continue
            skillvector[nrm_skill] = titleskillc/totalc*log(totalc/skillc)
        if skillvector:
            norm = sqrt(sum(v**2 for v in skillvector.values()))
            skillvector = dict((s, v/norm) for s,v in skillvector.items())
            newtitles.append((sector, title, sector_filter))
            titlecounts.append(titlec)
            skillvectors.append(skillvector)

    return newtitles, titlecounts, skillvectors


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
                        help='CSV file with sectors and job titles.')
    parser.add_argument('output_file',
                        help='Name of the CSV file to generate.')    
    parser.add_argument('--clusters', type=int, default=20,
                        help='Number of clusters.')
    parser.add_argument('--mappings', 
                        help='CSV file holding entity mappings.')
    parser.add_argument('--power', type=float, default=0.0,
                        help='p-parameter for the clustering algorithm.')    
    parser.add_argument('--reprocess', action='store_true',
                        help='Generate output file suitable for re-processing.')
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum count for skills.')
    parser.add_argument('--random-seed', type=int, default=1234,
                        help='Random seed for MDS fit.')
    args = parser.parse_args()

    logger = Logger()
    
    titles = []
    with open(args.input_file, 'r') as input_file:
        csvreader = csv.reader(input_file)
        for row in csvreader:
            if len(row) < 3:
                continue
            sector_filter = bool(int(row[2]))
            titles.append((row[0], row[1], sector_filter))

    titles, titlecounts, skillvectors \
        = skillvectors(titles, args.mappings, args.min_count)
    titlecounts = dict(zip(titles, titlecounts))

    logger.log('Generating clusters.\n')
    clusters, _ = find_clusters(
        args.clusters, skillvectors, labels=titles,
        distance=lambda v1, v2: distance(v1, v2, power=args.power),
        merge=merge)
    clusters = [(c,
                 max(c, key=lambda x: titlecounts[x]),
                 sum(titlecounts[t] for t in c)) for c in clusters]
    clusters.sort(key=lambda c: -c[-1])
    for cluster, title, count in clusters:
        logger.log('{0:s} ({1:d})\n'.format(title[1], count))
        clusterlist = [(t, titlecounts[t]) for t in cluster]
        clusterlist.sort(key=lambda x: -x[-1])
        for title, count in clusterlist:
            logger.log('    {0:s} ({1:d})\n'.format(title[1], count))
    logger.log('\n')

    with open(args.output_file, 'w') as outputfile:
        csvwriter = csv.writer(outputfile)
        for cluster, title, count in clusters:
            for sector, subtitle, sector_filter in cluster:
                if args.reprocess:
                    csvwriter.writerow(
                        [sector, subtitle, int(sector_filter), title[1],
                         titlecounts[sector, subtitle, sector_filter]])
                else:
                    sector = subtitle[0] if subtitle[2] else ''
                    csvwriter.writerow(
                        ['title', 'en', sector, subtitle[1], title[1]])
            

    # logger.log('Generating MDS plot.\n')
    # mds = manifold.MDS(n_components=2, dissimilarity='precomputed',
    #                    random_state=args.random_seed)
    # projections = mds.fit(dm).embedding_
    # xvals = projections[:, 0]
    # yvals = projections[:, 1]

    # maxerr = 0.0
    # avgerr = 0.0
    # npoints = len(xvals)
    # for i in range(npoints):
    #     for j in range(i):
    #         d = sqrt((xvals[i]-xvals[j])**2 + (yvals[i]-yvals[j])**2)
    #         err = abs(d-dm[i,j])/dm[i,j]
    #         maxerr = max(err, maxerr)
    #         avgerr += err
    # avgerr /= npoints*(npoints-1)
    # logger.log('Maximum error: {0:3.0f}%\n'.format(maxerr*100))
    # logger.log('Average error: {0:5.1f}%\n'.format(avgerr*100))

    # output_file(args.output_file)
    # maxlogcount = max(log(count) for count in titlecounts)
    # alphas = [log(count)/maxlogcount for count in titlecounts]
    # source = ColumnDataSource(data=dict(x=xvals, y=yvals, count=titlecounts,
    #                                     desc=titles, alpha=alphas))
    # # hover = HoverTool(tooltips="""
    # #         <span style="font-family: Helvetica; font-size: 12pt;">@desc</span>
    # #         """)
    # hover = HoverTool(tooltips=[('Job','@desc'), ('Count', '@count')])
    # p = figure(plot_width=800, plot_height=600, tools='pan,wheel_zoom',
    #            title="Job Clusters")
    # set_aspect(p, xvals, yvals)
    # p.circle('x', 'y', size=15, source=source, alpha='alpha')
    # p.add_tools(hover)

    # save(p)
    
