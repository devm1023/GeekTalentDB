import conf
from analyticsdb import *
from logger import Logger
from textnormalization import normalized_entity
from analytics_get_entitycloud import relevance_scores
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

def skillvectors(titles, mincount=1):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger()

    totalc = andb.query(LIProfile.id) \
                 .join(Location) \
                 .filter(LIProfile.language == 'en',
                         Location.nuts0 == 'UK') \
                 .count()
    skillvectors = []
    newtitles = []
    titlecounts = []
    for title in titles:
        logger.log('Processing: {0:s}\n'.format(title))
        nrm_title = normalized_entity('title', 'linkedin', 'en', title)
        titlec = andb.query(LIProfile.id) \
                     .join(Location) \
                     .filter(LIProfile.language == 'en',
                             Location.nuts0 == 'UK',
                             LIProfile.nrm_curr_title == nrm_title) \
                     .count()
        entitiesq = lambda entities: \
                    andb.query(Entity.nrm_name, Entity.profile_count) \
                        .filter(Entity.language == 'en',
                                Entity.type == 'skill',
                                Entity.source == 'linkedin',
                                Entity.nrm_name.in_(entities))
        coincidenceq = andb.query(LIProfileSkill.nrm_name, func.count()) \
                           .join(LIProfile) \
                           .join(Location) \
                           .filter(LIProfile.language == 'en',
                                   Location.nuts0 == 'UK',
                                   LIProfile.nrm_curr_title == nrm_title)
        skillvector = {}
        for nrm_skill, skillc, titleskillc, _, _ in \
            relevance_scores(totalc, titlec, entitiesq, coincidenceq):
            if skillc < mincount:
                continue
            # skillvector[nrm_skill] = titleskillc
            skillvector[nrm_skill] = titleskillc/totalc*log(totalc/skillc)
        if skillvector:
            norm = sqrt(sum(v**2 for v in skillvector.values()))
            skillvector = dict((s, v/norm) for s,v in skillvector.items())
            newtitles.append(title)
            titlecounts.append(titlec)
            skillvectors.append(skillvector)

    return titles, titlecounts, skillvectors

def distance(vec1, vec2):
    v1sq = sum(v**2 for v in vec1.values())
    v2sq = sum(v**2 for v in vec2.values())
    v1v2 = sum(v1*vec2.get(k, 0.0) for k, v1 in vec1.items())
    return acos(max(0.0, min(1.0, v1v2/sqrt(v1sq*v2sq))))

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
                        help='Name of the HTML file to generate.')
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
            if len(row) < 2:
                continue
            titles.append(row[1])

    titles, titlecounts, skillvectors = skillvectors(titles, args.min_count)
    titlecounts = dict(zip(titles, titlecounts))

    logger.log('Generating clusters.\n')
    clusters, _ = find_clusters(20, skillvectors, labels=titles,
                                distance=distance, merge=merge)
    clusters = [(c,
                 max(c, key=lambda x: titlecounts[x]),
                 sum(titlecounts[t] for t in c)) for c in clusters]
    clusters.sort(key=lambda c: -c[-1])
    for cluster, title, count in clusters:
        logger.log('{0:s} ({1:d})\n'.format(title, count))
        clusterlist = [(t, titlecounts[t]) for t in cluster]
        clusterlist.sort(key=lambda x: -x[-1])
        for title, count in clusterlist:
            logger.log('    {0:s} ({1:d})\n'.format(title, count))
    logger.log('\n')

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
    
