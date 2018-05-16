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


def find_closest_cluster(jobid, fromid, toid, people_skill_vectors, output_csv):
    logger = Logger()
    cndb = CanonicalDB()

    if args.source == 'adzuna':
        table = ADZJob
        skill_table = ADZJobSkill
    elif args.source == 'indeedjob':
        table = INJob
        skill_table = INJobSkill

    q = cndb.query(table).filter(table.id >= fromid)
    all_titles = cndb.query(table.category, table.parsed_title) \
                     .filter(table.id >= fromid)

    if toid is not None:
        q = q.filter(table.id < toid)
        all_titles = all_titles.filter(table.id < toid)

    if args.sector is not None:
        q = q.filter(table.category == args.sector)
        all_titles = all_titles.filter(table.category == args.sector)

    all_titles = all_titles.filter(table.parsed_title.isnot(None))

    logger.log('Querying titles...\n')
    titles = [(row[0], row[1], False) for row in all_titles]
    titles = list(set(titles))

    sv_titles, _, tmp_svs = skillvectors(table, skill_table, args.source, titles, args.mappings)
    jobs_skill_vectors = {}

    for title, vector in zip(sv_titles, tmp_svs):
        jobs_skill_vectors[title[1]] = vector

    def find_closest_cluster(adzjob):

        if adzjob.parsed_title not in jobs_skill_vectors:
            # no skills?
            return

        job_skill_vector = jobs_skill_vectors[adzjob.parsed_title]

        closest = None
        closest_dist = 0
        skill_intersection = []
        for cluster, vector in people_skill_vectors.items():
            dist = distance(job_skill_vector, vector, 1)

            if not closest or dist < closest_dist:
                closest = cluster
                closest_dist = dist
                skill_intersection = list(set(vector.keys()) & set(job_skill_vector.keys()))


        has_full_desc = adzjob.full_description is not None

        if output_csv is not None:
            output_csv.writerow([adzjob.parsed_title, closest[1], closest_dist, has_full_desc, len(job_skill_vector), len(skill_intersection), ", ".join(skill_intersection)])

        adzjob.merged_title = closest[1]

    process_db(q, find_closest_cluster, cndb, logger=logger)


def main(args):
    njobs = max(args.jobs, 1)
    batchsize = args.batch_size

    cndb = CanonicalDB()
    logger = Logger()

    if args.source == 'adzuna':
        table = ADZJob
    elif args.source == 'indeedjob':
        table = INJob

    titles, titlecounts, skillvectors = get_skillvectors(args.skill_file, None)

    # workaround for applying linkedin clusters
    renamed_skillvectors = []
    for vector in skillvectors:
        renamed_skillvectors.append({k.replace(':linkedin:', ':{}:'.format(args.source)): vector[k] for k in vector})

    skillvectors = renamed_skillvectors
    skillvectors = dict(zip(titles, skillvectors))

    query = cndb.query(table.id)

    if args.from_id is not None:
        query = query.filter(table.id >= args.from_id)
    
    if args.sector is not None:
        query = query.filter(table.category == args.sector)

    output_csv = None
    if args.output is not None:
        output_csv = csv.writer(open(args.output, 'w'))

    split_process(query, find_closest_cluster, args.batch_size,
                njobs=njobs, args=[skillvectors, output_csv],
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
    parser.add_argument('--source',
                        choices=['adzuna', 'indeedjob'],
                        help='Source type to process.')
    parser.add_argument('skill_file', help=
                        'File containing skill vectors from clustering')
    parser.add_argument('--output', help=
                        'File to save matches and distances to')
    parser.add_argument('--mappings',
                        help='CSV file holding entity mappings.')
    args = parser.parse_args()

    main(args)
