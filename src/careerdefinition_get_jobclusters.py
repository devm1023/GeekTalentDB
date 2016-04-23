import conf
from analyticsdb import *
from logger import Logger
from textnormalization import normalized_entity
from analytics_get_entitycloud import relevance_scores
from sqlalchemy import func
import csv
from math import log, sqrt, acos
import argparse

import numpy as np
from sklearn import manifold

from bokeh.plotting import figure, output_file, show, save, ColumnDataSource
from bokeh.models import HoverTool
from bokeh_aspect import *

def distance_matrix(titles, mincount=1):
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    logger = Logger()

    totalc = andb.query(LIProfile.id) \
                 .join(Location) \
                 .filter(LIProfile.language == 'en',
                         Location.nuts0 == 'UK') \
                 .count()
    skillclouds = []
    newtitles = []
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
                    andb.query(LIProfileSkill.nrm_name, func.count()) \
                        .join(LIProfile) \
                        .join(Location) \
                        .filter(LIProfile.language == 'en',
                                Location.nuts0 == 'UK',
                                LIProfileSkill.nrm_name.in_(entities)) \
                        .group_by(LIProfileSkill.nrm_name)
        coincidenceq = andb.query(LIProfileSkill.nrm_name, func.count()) \
                           .join(LIProfile) \
                           .join(Location) \
                           .filter(LIProfile.language == 'en',
                                   Location.nuts0 == 'UK',
                                   LIProfile.nrm_curr_title == nrm_title)
        skillcloud = {}
        for nrm_skill, skillc, titleskillc, _, _ in \
            relevance_scores(totalc, titlec, entitiesq, coincidenceq):
            if skillc < mincount:
                continue
            skillcloud[nrm_skill] = titleskillc/totalc*log(totalc/skillc)
        if skillcloud:
            norm = sqrt(sum(v**2 for v in skillcloud.values()))
            skillcloud = dict((s, v/norm) for s,v in skillcloud.items())
            newtitles.append(title)
            skillclouds.append(skillcloud)

    titles = newtitles
    dm = np.zeros((len(titles), len(titles)), dtype=float)
    for i, cloud1 in enumerate(skillclouds):
        for j, cloud2 in enumerate(skillclouds):
            for nrm_skill, v1 in cloud1.items():
                dm[i, j] += v1*cloud2.get(nrm_skill, 0.0)
            dm[i, j] = acos(max(-1.0, min(1.0, dm[i, j])))
            
    return titles, dm

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
    
    titles = []
    with open(args.input_file, 'r') as input_file:
        csvreader = csv.reader(input_file)
        for row in csvreader:
            if len(row) < 2:
                continue
            titles.append(row[1])

    titles, dm = distance_matrix(titles, args.min_count)

    mds = manifold.MDS(n_components=2, dissimilarity='precomputed',
                       random_state=args.random_seed)
    projections = mds.fit(dm).embedding_
    xvals = projections[:, 0]
    yvals = projections[:, 1]

    output_file(args.output_file)
    source = ColumnDataSource(data=dict(x=xvals, y=yvals, desc=titles))
    # hover = HoverTool(tooltips="""
    #         <span style="font-family: Helvetica; font-size: 12pt;">@desc</span>
    #         """)
    hover = HoverTool(tooltips=[('Job','@desc')])
    p = figure(plot_width=800, plot_height=600, tools='pan,wheel_zoom',
               title="Job Clusters")
    set_aspect(p, xvals, yvals)
    p.circle('x', 'y', size=10, source=source)
    p.add_tools(hover)

    save(p)
    
