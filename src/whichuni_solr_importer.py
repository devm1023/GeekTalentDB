import argparse
import hashlib
import json
import re
import requests
from datetime import datetime
from pprint import pprint
import hashlib
from dbtools import dict_from_row
from logger import Logger
from parse_datetime import parse_datetime
from sqlalchemy import func
from whichunidb import *
from windowquery import process_db, split_process

SOLR_URL = "http://localhost:8983/solr/whichuni"
GEO_KEY = "AIzaSyBipNiucZXVvVUmOrrV5voUhcsuoyiTPvI"

def make_hash(url):
    hasher = hashlib.md5()
    hasher.update(url.encode('utf-8'))
    digest = hasher.hexdigest()
    return digest

def make_rating(rating):
    if not rating: return None
    if rating.lower() == 'low': return 0
    if rating.lower() == 'med': return 1
    if rating.lower() == 'high': return 2



def get_geo(university_name):
    url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query={0}&key={1}'.format('{0}, UK'.format(university_name), GEO_KEY)
    r = requests.get(url)
    country = None
    city = None
    latlng = None
    if r.status_code != 200:
        raise RuntimeError('status code: {0} for url: {1}'.format(r.status_code, url))
    else:
        response1 = r.json()
        place_id = None
        if 'results' in response1 and len(response1['results']) > 0:
            place_id = response1['results'][0]['place_id']
            url = 'https://maps.googleapis.com/maps/api/geocode/json?place_id={0}&key={1}'.format(place_id, GEO_KEY)
            r = requests.get(url)
            response = r.json()
            if 'results' in response and len(response['results']) > 0:
                result = response['results'][0]
                for component in result['address_components']:
                    if 'administrative_area_level_1' in component['types']:
                        country = component['long_name']
                    if 'postal_town' in component['types']:
                        city = component['long_name']
                lat = result['geometry']['location']['lat']
                lng = result['geometry']['location']['lng']
                latlng = '{0}, {1}'.format(lat, lng)
    return city, country, latlng
        


def get_subject_documents(university_subject, university, course_name, parent_course_id, parent_university_id):
    return dict({
        'content_type': 'subject',
        'subject_id': make_hash(university_subject.subject_name),
        'parent_course_id': parent_course_id,
        'parent_university_id': parent_university_id,
        'subject_name': university_subject.subject_name,
        'university_name': university.name,
        'course_name': course_name,
        'subject_average_salary': university_subject.average_salary,
        'subject_average_salary_rating': university_subject.average_salary_rating,
        'subject_average_salary_rating_i': make_rating(university_subject.average_salary_rating),
        'subject_dropout_rate': university_subject.dropout_rate,
        'subject_further_study': university_subject.employed_furtherstudy,
        'subject_further_study_rating': university_subject.employed_furtherstudy_rating,
        'subject_further_study_rating_i': make_rating(university_subject.employed_furtherstudy_rating),
        'subject_full_time': university_subject.full_time,
        'subject_part_time': university_subject.part_time,
        'subject_male': university_subject.male,
        'subject_female': university_subject.female,
        'subject_uk': university_subject.uk,
        'subject_nonuk': university_subject.non_uk,
        'subject_rating_names': [r.name for r in university_subject.ratings],
        'subject_rating_scores': [r.rating for r in university_subject.ratings],
        'subject_satisfaction': university_subject.satisfaction,
        'subject_sectors_after': [s.sector_after.name for s in university_subject.sectors_after],
        'subject_sectors_after_percent': [s.percent for s in university_subject.sectors_after],
        'subject_student_score': university_subject.student_score,
        'subject_student_score_rating': university_subject.student_score_rating,
        'subject_student_score_rating_i': make_rating(university_subject.student_score_rating),
        'subject_twotoone_above': university_subject.twotoone_or_above,
        'subject_typical_ucas_points': university_subject.typical_ucas_points
    })

def get_course_documents(course, university, parent_university_id, location):
    course_id = make_hash(course.url)
    return dict({
        'content_type': 'course',
        'city': location[0],
        'country': location[1],
        'geo': location[2],
        'university_name': university.name,
        'parent_university_id': parent_university_id,
        'course_id': make_hash(course.url),
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
        'course_study_type_modes': [s.mode for s in course.study_types],
        'course_study_type_names': [s.qualification_name for s in course.study_types],
        'course_study_type_years': [s.years for s in course.study_types],
        'course_subjects': [s.subject_name for s in course.university_subjects],
        'url': course.url,
        '_childDocuments_': [get_subject_documents(subject, university, course.title, course_id, parent_university_id) for subject in course.university_subjects]
    })

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
        university_id = make_hash(university.url)
        location = get_geo(university.name)
        document = dict({
            'content_type': 'university',
            'university_id': university_id,
            'university_name': university.name,
            'description': university.description,
            'city': location[0],
            'country': location[1],
            'geo': location[2],
            'ucas_code': university.ucas_code,
            'website': university.website,
            'further_study': university.further_study,
            'further_study_rating': university.further_study_r,
            'further_study_rating_i': make_rating(university.further_study_r),
            'average_salary': university.average_salary,
            'average_salary_rating': university.average_salary_r,
            'average_salary_rating_i': make_rating(university.average_salary_r),
            'student_score': university.student_score,
            'student_score_rating': university.student_score_r,
            'student_score_rating_i': make_rating(university.student_score_r),
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
            'nonuk': university.non_uk,
            'characteristic_names': [c.characteristic.name for c in university.university_characteristics],
            'characteristic_scores': [c.score for c in university.university_characteristics],
            'characteristic_score_ratings': [c.score_r for c in university.university_characteristics],
            'characteristic_score_ratings_i': [make_rating(c.score_r) for c in university.university_characteristics],
            'league_table_names': [l.league_table.name for l in university.university_league_tables],
            'league_table_rankings': [l.rating for l in university.university_league_tables],
            'league_table_totals': [l.league_table.total for l in university.university_league_tables],
            'url': university.url,
            '_childDocuments_': [get_course_documents(course, university, university_id, location) for course in courses]
        })
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.post('{0}{1}'.format(SOLR_URL, '/update?commitWithin=3000'), data=json.dumps([document]), headers=headers)
        if r.status_code < 200 or r.status_code > 399:
                raise RuntimeError('Received status code {0:d} for university {1}' \
                                   .format(r.status_code, university.name))
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