import csv
import argparse
import sys
from textnormalization import normalized_title
from canonicaldb import *
from logger import Logger
from windowquery import split_process, process_db

from careerdefinition_get_skillvectors import skillvectors
from careerdefinition_cluster import get_skillvectors, distance

from math import acos, sqrt


def find_closest_cluster_adzuna(jobid, fromid, toid, skill_vectors, mappings):
    logger = Logger()
    cndb = CanonicalDB()

    q = cndb.query(ADZJob).filter(ADZJob.id >= fromid)
    all_titles = cndb.query(ADZJob.category, ADZJob.parsed_title) \
                     .filter(~ADZJob.nrm_title.in_(mappings.keys())) \
                     .filter(ADZJob.id >= fromid)
    if toid is not None:
        q = q.filter(ADZJob.id < toid)
        all_titles = all_titles.filter(ADZJob.id < toid)

    if args.sector is not None:
        q = q.filter(ADZJob.category == args.sector)
        all_titles = all_titles.filter(ADZJob.category == args.sector)

    logger.log('Querying titles...\n')
    titles = [(row[0], row[1], False) for row in all_titles]
    titles = list(set(titles))

    sv_titles, _ ,tmp_svs = skillvectors(ADZJob, ADZJobSkill, 'adzuna', titles, None)
    title_skill_vectors = {}

    for title, vector in zip(sv_titles, tmp_svs):
        title_skill_vectors[title[1]] = vector

    def find_closest_cluster(adzjob):

        # matches a clustered title
        if adzjob.nrm_title in mappings:
            adzjob.merged_title = mappings[adzjob.nrm_title][1]
            return

        if adzjob.parsed_title not in title_skill_vectors:
            # no skills?
            return

        closest = None
        closest_dist = 0
        skill_intersection = []
        for cluster, vector in skill_vectors.items():
            dist = distance(title_skill_vector, vector, 1)

            if not closest or dist < closest_dist:
                closest = cluster
                closest_dist = dist

        adzjob.merged_title = closest[1]

    process_db(q, find_closest_cluster, cndb, logger=logger)


def main(args):
    njobs = max(args.jobs, 1)
    batchsize = args.batch_size

    cndb = CanonicalDB()
    logger = Logger()

    titles, titlecounts, skillvectors = get_skillvectors(args.skill_file, None)

    # workaround for applying linkedin clusters
    renamed_skillvectors = []
    for vector in skillvectors:
        renamed_skillvectors.append({k.replace(':linkedin:', ':adzuna:'): vector[k] for k in vector})

    skillvectors = renamed_skillvectors
    #

    skillvectors = dict(zip(titles, skillvectors))
    mappings = {}

    with open(args.clusters_file, 'r') as inputfile:
        csvreader = csv.reader(inputfile)
        for row in csvreader:
            sector, title, _, mapped_title, _ = row
            mappings[normalized_title('adzuna', 'en', title)] = (normalized_title('adzuna', 'en', mapped_title), mapped_title)


    query = cndb.query(ADZJob.id).filter(~ADZJob.nrm_title.in_(mappings.keys()))
    if args.from_id is not None:
        query = query.filter(ADZJob.id >= args.from_id)
    
    if args.sector is not None:
        query = query.filter(ADZJob.category == args.sector)

    split_process(query, find_closest_cluster_adzuna, args.batch_size,
                njobs=njobs, args=[skillvectors, mappings],
                logger=logger, workdir='jobs',
                prefix='canonical_find_closest_clusters')


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-id', help=
                        'Start processing from this ID. Useful for '
                        'crash recovery.')
    parser.add_argument('--sector')
    parser.add_argument('skill_file', help=
                        'File containing skill vectors from clustering')
    parser.add_argument('clusters_file', help=
                        'File containing clusters')
    args = parser.parse_args()


    main(args)
