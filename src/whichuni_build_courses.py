import argparse
import hashlib
import re
from datetime import datetime
from pprint import pprint

import parsedb as ps
from dbtools import dict_from_row
from logger import Logger
from parse_datetime import parse_datetime
from sqlalchemy import func
from whichunidb import *
from windowquery import process_db, split_process


def import_wucourses(jobid, fromid, toid):
    logger = Logger()
    psdb = ps.ParseDB()
    wudb = WhichUniDB()

    q = psdb.query(ps.WUCourse).filter(ps.WUCourse.id >= fromid)
    if toid is not None:
        q = q.filter(ps.WUCourse.id < toid)

    def add_wucourse(wucourse):
        wudb = WhichUniDB()
        # find university
        university = wudb.query(WUUniversity) \
                         .filter(WUUniversity.name == wucourse.university_name) \
                         .first()
        if university is not None:
            # proceed
            if wucourse.description:
                wucourse.description = wucourse.description.replace('â??', "'")
            if wucourse.modules:
                wucourse.modules = wucourse.modules.replace('â??', "'")
            new_course = WUCourse(ucas_code=wucourse.ucas_code,
                                url=wucourse.url,
                                university_id=university.id,
                                title=wucourse.title,
                                ucas_points_l=wucourse.ucas_points_l,
                                ucas_points_h=wucourse.ucas_points_h,
                                offers=wucourse.offers,
                                tuition_fee=wucourse.tuition_fee,
                                description=wucourse.description,
                                modules=wucourse.modules)
            wudb.add(new_course)
            wudb.flush()
            for entry_requirement in wucourse.entry_requirements:
                existing_requirement = wudb.query(WUEntryRequirement) \
                                           .filter(WUEntryRequirement.name \
                                                    == entry_requirement.name) \
                                           .first()
                if existing_requirement is None:
                    existing_requirement = WUEntryRequirement(name=entry_requirement.name)
                    wudb.add(existing_requirement)
                    wudb.flush()
                course_entry_requirement = WUCourseEntryRequirement(
                    course_id=new_course.id,
                    entryrequirement_id=existing_requirement.id,
                    grades=entry_requirement.grades,
                    text=entry_requirement.text
                )
                wudb.add(course_entry_requirement)
                wudb.flush()
            for study_type in wucourse.study_types:
                new_duration = int(study_type.duration) if study_type.duration else 0
                new_study_type = WUStudyType(course_id=new_course.id,
                                            qualification_name=study_type.qualification_name,
                                            duration=new_duration,
                                            mode=study_type.mode,
                                            years=study_type.years)
                wudb.add(new_study_type)
                wudb.flush()
            for university_subject in wucourse.subjects:
                existing_subject = wudb.query(WUSubject) \
                                       .filter(func.lower(WUSubject.title) == func.lower(university_subject.subject_name)) \
                                       .first()
                if existing_subject:
                    new_university_subject = WUUniversitySubject(
                        university_id=university.id,
                        course_id=new_course.id,
                        student_score=university_subject.student_score,
                        student_score_rating=university_subject.student_score_rating,
                        employed_furtherstudy=university_subject.employed_furtherstudy,
                        employed_furtherstudy_rating=university_subject.employed_furtherstudy_rating,
                        average_salary=university_subject.average_salary,
                        average_salary_rating=university_subject.average_salary_rating,
                        uk=university_subject.uk,
                        non_uk=university_subject.non_uk,
                        male=university_subject.male,
                        female=university_subject.female,
                        full_time=university_subject.full_time,
                        part_time=university_subject.part_time,
                        typical_ucas_points=university_subject.typical_ucas_points,
                        twotoone_or_above=university_subject.twotoone_or_above,
                        satisfaction=university_subject.satisfaction,
                        dropout_rate=university_subject.dropout_rate,
                        subject_name=university_subject.subject_name,
                        subject_id=existing_subject.id
                    )
                    wudb.add(new_university_subject)
                    wudb.flush()
                else:
                    new_university_subject = WUUniversitySubject(
                        university_id=university.id,
                        course_id=new_course.id,
                        student_score=university_subject.student_score,
                        student_score_rating=university_subject.student_score_rating,
                        employed_furtherstudy=university_subject.employed_furtherstudy,
                        employed_furtherstudy_rating=university_subject.employed_furtherstudy_rating,
                        average_salary=university_subject.average_salary,
                        average_salary_rating=university_subject.average_salary_rating,
                        uk=university_subject.uk,
                        non_uk=university_subject.non_uk,
                        male=university_subject.male,
                        female=university_subject.female,
                        full_time=university_subject.full_time,
                        part_time=university_subject.part_time,
                        typical_ucas_points=university_subject.typical_ucas_points,
                        twotoone_or_above=university_subject.twotoone_or_above,
                        satisfaction=university_subject.satisfaction,
                        dropout_rate=university_subject.dropout_rate,
                        subject_name=university_subject.subject_name
                    )
                    wudb.add(new_university_subject)
                    wudb.flush()
                for studied_before in university_subject.studied_before:
                    new_studied_before = wudb.query(WUStudiedBefore) \
                                             .filter(WUStudiedBefore.name == studied_before.name) \
                                             .first()
                    if new_studied_before is None:
                        new_studied_before = WUStudiedBefore(name=studied_before.name)
                        wudb.add(new_studied_before)
                        wudb.flush()
                    university_subject_studied_before = WUUniversitySubjectStudiedBefore(
                        university_subject_id=new_university_subject.id,
                        studied_before_id=new_studied_before.id,
                        percent=studied_before.percent,
                        common_grade=studied_before.common_grade,
                        common_grade_percent=studied_before.common_grade_percent
                    )
                    wudb.add(university_subject_studied_before)
                    wudb.flush()
                for sector_after in university_subject.sectors_after:
                    existing_sector = wudb.query(WUSectorAfter) \
                                          .filter(WUSectorAfter.name == sector_after.name) \
                                          .first()
                    if existing_sector is None:
                        existing_sector = WUSectorAfter(name=sector_after.name)
                        wudb.add(existing_sector)
                        wudb.flush()
                    university_subject_sector_after = WUUniversitySubjectSectorAfter(
                        university_subject_id=new_university_subject.id,
                        sector_after_id=existing_sector.id,
                        percent=sector_after.percent
                    )
                    wudb.add(university_subject_sector_after)
                for rating in university_subject.ratings:
                    new_rating = WURating(
                        name=rating.name,
                        rating=rating.rating,
                        university_subject_id=new_university_subject.id
                    )
                    wudb.add(new_rating)
                    wudb.flush()
        wudb.commit()    

    process_db(q, add_wucourse, wudb, logger=logger)

def main(args):
    njobs = max(args.jobs, 1)
    batchsize = args.batch_size

    psdb = ps.ParseDB()
    logger = Logger()

    query = psdb.query(ps.WUCourse.id)
    if args.from_id is not None:
        query = query.filter(table.id >= from_id)
    print(query)
    split_process(query, import_wucourses, args.batch_size,
                    njobs=njobs, logger=logger, workdir='jobs',
                    prefix='whichuni_build_courses')

if __name__ == '__main__':
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
