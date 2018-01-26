from canonicaldb import *
from logger import Logger
from sqlalchemy import func
import argparse
import csv


LOG_INTERVAL = 100


def get_idf(job_table, skill_table, category):
    cndb   = CanonicalDB()
    logger = Logger()

    logger.log('Counting total document count:\n')

    q = cndb.query(job_table.merged_title) \
            .filter(job_table.nut == 'en') \
            .group_by(job_table.merged_title)

    if category is not None:
        q.filter(job_table.category == category) \

    totalc = q.count()

    logger.log("Total: {} merged titles.\n".format(totalc))

    logger.log('Counting skills.\n')

    q = cndb.query(skill_table.nrm_name)\
        .filter(skill_table.language =='en')\
        .group_by(skill_table.nrm_name)

    skills = list(q)

    logger.log('Skills found: {}.\n'.format(len(skills)))

    idfs = dict()

    for nrm_skill in skills:
        nrm_skill = nrm_skill[0]
        skillc = cndb.query(job_table.merged_title) \
            .join(skill_table) \
            .filter(job_table.id == skill_table.adzjob_id) \
            .filter(skill_table.language == 'en') \
            .filter(skill_table.nrm_name == nrm_skill) \
            .group_by(job_table.merged_title) \
            .count()
        idfs[nrm_skill] = totalc / skillc

        if len(idfs) % LOG_INTERVAL == 0:
            logger.log('Skill IDFs calculated: {}.\n'.format(len(idfs)))

    return idfs


def get_tfs(job_table, skill_table, category):
    cndb = CanonicalDB()
    logger = Logger()

    q = cndb.query(job_table.merged_title) \
        .filter(job_table.language == 'en') \
        .group_by(job_table.merged_title) \
        .order_by(job_table.merged_title.asc())

    if category is not None:
        q.filter(job_table.category == category)

    merged_titles = [row[0] for row in q]

    tfs = dict()

    for merged_title in merged_titles:
        logger.log("TFing skills for: {}\n".format(merged_title))

        q = cndb.query(skill_table.nrm_name, func.count()) \
            .join(job_table) \
            .filter(skill_table.adzjob_id == job_table.id) \
            .filter(skill_table.language == 'en') \
            .filter(job_table.merged_title == merged_title) \
            .group_by(skill_table.nrm_name)

        tf = dict(q)
        tfs[merged_title] = tf

    return tfs


def compute_tfidfs(tfs, idfs):

    for doc, tf in tfs.items():
        for term, tf_score in tf.items():
           tf[term] = float(tf_score) * float(idfs[term])

    return tfs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_file',
                        help='Name of the CSV file to generate.')
    parser.add_argument('--source', choices=['indeed', 'adzuna'], required=True,
                        help='The data source to process.')
    parser.add_argument('--category', choices=['it-jobs', 'engineering-jobs', 'healthcare-nursing-jobs'],
                        help='Sector category.')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--output-idf',
                        help='CSV with skill idf values to be written to.')
    group.add_argument('--input-idf',
                        help='CSV with skill idf values to be written to.')

    args = parser.parse_args()

    logger = Logger()

    if args.source == 'indeed':
        job_table = INJob
        skill_table = INJobSkill
    elif args.source == 'adzuna':
        job_table = ADZJob
        skill_table = ADZJobSkill

    if args.input_idf:
        idfs = dict()
        with open(args.input_idf, 'r') as input_file:
            csvreader = csv.reader(input_file)
            for row in csvreader:
                if len(row) < 2:
                    continue
                idfs[row[0]] = row[1]
    else:
        idfs = get_idf(job_table, skill_table, args.category)

    if args.output_idf:
        with open(args.output_idf, 'w') as outputfile:
            csvwriter = csv.writer(outputfile)
            for skill_name, idf_score in idfs.items():
                csvwriter.writerow([skill_name, idf_score])

    logger.log("IDFs done. Doing TFs...\n")

    tfs = get_tfs(job_table, skill_table, args.category)

    logger.log("TFs done. Doing TF-IDFs...\n")

    tfidfs = compute_tfidfs(tfs, idfs)

    if args.output_file:
        with open(args.output_file, 'w') as outputfile:
            csvwriter = csv.writer(outputfile)
            for merged_title, tfidf in tfidfs.items():
                for skill_name, score in tfidf.items():
                    csvwriter.writerow([merged_title if not None else "None", skill_name, score])

    logger.log("All done.\n")

