import argparse
import hashlib
import re
from datetime import datetime
from pprint import pprint

from dbtools import dict_from_row
from logger import Logger
from parse_datetime import parse_datetime
from sqlalchemy import func
from whichunidb import *
from windowquery import process_db, split_process

SOLR_URL = "http://localhost:8983/solr/whichuni"

def solr_import(jobid, fromid, toid):
    logger = Logger()
    wudb = WhichUniDB()
    q = wudb.query(WUUniversity) \
            .filter(WUUniversity.id >= fromid)
    if toid is not None:
        q = q.filter(WUUniversity.id < toid)


    def add_to_solr(university):
        courses = wudb.query(WUCourse) \
                       .filter(WUCourse.university_id == university.id) \
                       .all()

        def get_course_documents(course, university_name):

            def get_subject_documents(subject, university_name, course_name):
                return dict({
                    'content_type': 'subject',
                    'subject_name': subject.name,
                    'university_name': university_name,
                    'course_name': course_name,
                    'subject_average_salary'
                })

            return dict({
                'content_type': 'course',
                'university_name': university_name,
                'course_name': course.title,
                'course_entryreq_grades': [e.grades for e in course.entry_requirements],
                'course_entryreq_names': [e.entry_requirement.name for e in course.entry_requirements],
                'course_entryreq_texts': [e.text for e in course.entry_requirements],
                'course_modules': course.modules,
                'course_offers': course.offers,
                'course_description': course.description,
                'course_fee': course.tuition_fee,
                'course_ucas_high': course.ucas_points_h,
                'course_ucas_low': course.ucas_points_l,
                'course_ucas_code': course.ucas_code,
                'course_study_type_durations': [s.duration for s in course.study_types],
                'course_study_type_modes': [s.modes for s in course.study_types],
                'course_study_type_names': [s.qualification_name for s in course.study_types],
                'course_study_type_years': [s.years for s in course.study_types],
                '_childDocuments_': [get_subject_documents(subject, university_name, course_name) for subject in course.university_subjects]
            })

        document = dict({
            'content_type': 'university',
            'university_name': university.name,
            'description': university.description,
            'city': university.city.name if university.city else None,
            'ucas_code': university.ucas_code,
            'website': university.website,
            'further_study': university.further_study,
            'further_study_rating': university.further_study_r,
            'average_salary': university.average_salary,
            'average_salary_rating': university.average_salary_r,
            'student_score': university.student_score,
            'student_score_rating': university.student_score_r,
            'satisfaction': university.satisfaction,
            'no_of_students': university.no_of_students,
            'undergraduate': university.undergraduate,
            'postgraduate': university.postgraduate,
            'full_time': university.full_time,
            'part_time': university.part_time,
            'male': university.male,
            'female': university.female,
            'young': university.young,
            'mature': university.mature,
            'uk': university.uk,
            'non_uk': university.non_uk,
            'characteristic_names': [c.characteristic.name for c in university.university_characteristics],
            'characteristic_scores': [c.score for c in university.university_characteristics],
            'characteristic_score_ratings': [c.score_r for c in university.university_characteristics],
            'league_table_names': [l.league_table.name for l in university.university_league_tables],
            'league_table_ratings': [l.rating for l in university.university_league_tables],
            'league_table_totals': [l.league_table.total for l in university.university_league_tables],
            'url': university.url,
            '_childDocuments_': [get_course_documents(course, university.name) for course in courses]
        })
        pprint(document)

    for university in q.all():
        add_to_solr(university)

def main(args):
    njobs = max(args.jobs, 1)
    batchsize = args.batch_size

    wudb = WhichUniDB()
    logger = Logger()

    query = wudb.query(WUUniversity.id)
    if args.from_id is not None:
        query = query.filter(table.id >= from_id)
    split_process(query, solr_import, args.batch_size,
                    njobs=njobs, logger=logger, workdir='jobs',
                    prefix='whichuni_solr_importer')


if __name__ == "__main__":
     # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-id', help=
                        'Start processing from this course ID. Useful for '
                        'crash recovery.')
    args = parser.parse_args()
    main(args)